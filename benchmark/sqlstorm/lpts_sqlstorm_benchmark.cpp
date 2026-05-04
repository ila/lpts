//
// SQLStorm TPC-H benchmark runner for LPTS.
//
// For each query, run DuckDB first to establish that the query is accepted, then
// run PRAGMA lpts_check('<query>') to verify LPTS round-trip correctness.
//

#include "duckdb.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/connection.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits.h>
#include <map>
#include <poll.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace duckdb {

enum class QueryState : uint8_t {
	SUCCESS = 0,
	INCORRECT = 1,
	DUCKDB_ERROR = 2,
	LPTS_ERROR = 3,
	NOT_IMPLEMENTED = 4,
	TIMEOUT = 5,
	CRASH = 6
};

struct QueryResultSummary {
	double duckdb_time_ms = 0;
	double lpts_check_time_ms = 0;
	QueryState state = QueryState::SUCCESS;
	string phase;
	string error;

	bool IsSuccess() const {
		return state == QueryState::SUCCESS;
	}
};

struct PassStats {
	int success = 0;
	int incorrect = 0;
	int duckdb_error = 0;
	int lpts_error = 0;
	int not_implemented = 0;
	int timed_out = 0;
	int crashed = 0;
	double total_duckdb_success_ms = 0;
	double total_lpts_check_success_ms = 0;
	std::map<string, int> error_counts;
	std::map<string, vector<string>> error_queries;
	vector<string> incorrect_queries;
	vector<string> timeout_queries;
	vector<string> crash_queries;
};

static string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return string(buf);
}

static void Log(const string &msg) {
	Printer::Print("[" + Timestamp() + "] " + msg);
}

static string FormatNumber(double v) {
	std::ostringstream oss;
	oss << std::setprecision(5) << std::defaultfloat << v;
	string s = oss.str();
	auto pos = s.find('.');
	if (pos != string::npos) {
		while (!s.empty() && s.back() == '0') {
			s.pop_back();
		}
		if (!s.empty() && s.back() == '.') {
			s.pop_back();
		}
	}
	return s;
}

static string FormatSF(double sf) {
	std::ostringstream oss;
	oss << std::fixed << std::setprecision(4) << sf;
	string s = oss.str();
	auto dot = s.find('.');
	if (dot != string::npos) {
		auto last_nonzero = s.find_last_not_of('0');
		if (last_nonzero != string::npos && last_nonzero > dot) {
			s = s.substr(0, last_nonzero + 1);
		} else {
			s = s.substr(0, dot + 2);
		}
	}
	return s;
}

static bool FileExists(const string &path) {
	struct stat buffer;
	return stat(path.c_str(), &buffer) == 0;
}

static string ReadFileToString(const string &path) {
	std::ifstream in(path);
	if (!in.is_open()) {
		return string();
	}
	std::ostringstream ss;
	ss << in.rdbuf();
	return ss.str();
}

static vector<string> CollectQueryFiles(const string &dir) {
	vector<string> files;
	DIR *d = opendir(dir.c_str());
	if (!d) {
		return files;
	}
	struct dirent *entry;
	while ((entry = readdir(d)) != nullptr) {
		string name = entry->d_name;
		if (name.size() > 4 && name.substr(name.size() - 4) == ".sql") {
			files.push_back(dir + "/" + name);
		}
	}
	closedir(d);
	std::sort(files.begin(), files.end());
	return files;
}

static string QueryName(const string &path) {
	auto pos = path.find_last_of('/');
	string fname = (pos != string::npos) ? path.substr(pos + 1) : path;
	if (fname.size() > 4) {
		fname = fname.substr(0, fname.size() - 4);
	}
	return fname;
}

static string FindQueriesDir() {
	vector<string> candidates = {
	    "benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	    "../../../benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries",
	};
	for (auto &candidate : candidates) {
		if (FileExists(candidate)) {
			return candidate;
		}
	}

	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}
		for (int i = 0; i < 8; ++i) {
			string candidate = dir + "/benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries";
			if (FileExists(candidate)) {
				return candidate;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) {
				break;
			}
			dir = dir.substr(0, p2);
		}
	}
	return "";
}

static string StateToString(QueryState state) {
	switch (state) {
	case QueryState::SUCCESS:
		return "success";
	case QueryState::INCORRECT:
		return "incorrect";
	case QueryState::DUCKDB_ERROR:
		return "duckdb_error";
	case QueryState::LPTS_ERROR:
		return "lpts_error";
	case QueryState::NOT_IMPLEMENTED:
		return "not_implemented";
	case QueryState::TIMEOUT:
		return "timeout";
	default:
		return "crash";
	}
}

static string TrimError(string error) {
	auto sp = error.find("\n\nStack Trace");
	if (sp != string::npos) {
		error = error.substr(0, sp);
	}
	while (!error.empty() && (error.back() == '\n' || error.back() == '\r' || error.back() == ' ')) {
		error.pop_back();
	}
	if (error.size() > 240) {
		error = error.substr(0, 240);
	}
	return error;
}

static bool IsCrashError(const string &error) {
	return error.find("FATAL") != string::npos || error.find("INTERNAL") != string::npos ||
	       error.find("database has been invalidated") != string::npos;
}

static bool IsNotImplementedError(const string &error) {
	return error.find("Not implemented Error:") != string::npos || error.find("not implemented") != string::npos ||
	       error.find("not yet implemented") != string::npos || error.find("not (yet) supported") != string::npos ||
	       error.find("Unsupported expression") != string::npos;
}

static string EscapeSQLLiteral(const string &input) {
	string result;
	result.reserve(input.size() + 16);
	for (auto c : input) {
		if (c == '\'') {
			result += "''";
		} else {
			result += c;
		}
	}
	return result;
}

static string EscapeCSV(const string &input) {
	bool needs_quotes = input.find_first_of(",\"\n\r") != string::npos;
	if (!needs_quotes) {
		return input;
	}
	string result = "\"";
	for (auto c : input) {
		if (c == '"') {
			result += "\"\"";
		} else {
			result += c;
		}
	}
	result += "\"";
	return result;
}

static bool WriteAllBytes(int fd, const void *buf, size_t n) {
	const char *p = static_cast<const char *>(buf);
	while (n > 0) {
		ssize_t w = write(fd, p, n);
		if (w <= 0) {
			return false;
		}
		p += w;
		n -= static_cast<size_t>(w);
	}
	return true;
}

static bool ReadAllBytes(int fd, void *buf, size_t n) {
	char *p = static_cast<char *>(buf);
	while (n > 0) {
		ssize_t r = read(fd, p, n);
		if (r <= 0) {
			return false;
		}
		p += r;
		n -= static_cast<size_t>(r);
	}
	return true;
}

static void LoadExtension(Connection &con, const string &name) {
	auto load_result = con.Query("LOAD " + name);
	if (!load_result->HasError()) {
		return;
	}
	auto install_result = con.Query("INSTALL " + name);
	if (install_result->HasError()) {
		throw std::runtime_error("Failed to load or install " + name + ": " + load_result->GetError());
	}
	load_result = con.Query("LOAD " + name);
	if (load_result->HasError()) {
		throw std::runtime_error("Failed to load " + name + ": " + load_result->GetError());
	}
}

static void ConfigureConnection(Connection &con) {
	con.Query("PRAGMA threads=16");
	con.Query("PRAGMA memory_limit='16GB'");
	con.Query("SET temp_directory='/tmp/duckdb_temp'");
	LoadExtension(con, "tpch");
	LoadExtension(con, "lpts");
}

[[noreturn]] static void ChildWorkerMain(int read_fd, int write_fd, const string &db_path) {
	try {
		DuckDB db(db_path);
		Connection con(db);
		ConfigureConnection(con);

		while (true) {
			uint32_t sql_len = 0;
			if (!ReadAllBytes(read_fd, &sql_len, sizeof(sql_len))) {
				break;
			}
			if (sql_len == 0) {
				break;
			}

			string sql(sql_len, '\0');
			if (!ReadAllBytes(read_fd, &sql[0], sql_len)) {
				break;
			}

			QueryResultSummary summary;
			string error;

			auto duckdb_start = std::chrono::steady_clock::now();
			unique_ptr<MaterializedQueryResult> duckdb_result;
			try {
				duckdb_result = con.Query(sql);
			} catch (std::exception &ex) {
				error = ex.what();
			} catch (...) {
				error = "unknown DuckDB exception";
			}
			auto duckdb_end = std::chrono::steady_clock::now();
			summary.duckdb_time_ms = std::chrono::duration<double, std::milli>(duckdb_end - duckdb_start).count();

			if (error.empty() && duckdb_result && duckdb_result->HasError()) {
				error = duckdb_result->GetError();
			}
			duckdb_result.reset();

			if (!error.empty()) {
				summary.error = TrimError(error);
				summary.phase = "duckdb";
				summary.state = IsCrashError(summary.error) ? QueryState::CRASH : QueryState::DUCKDB_ERROR;
			} else {
				string check_sql = "PRAGMA lpts_check('" + EscapeSQLLiteral(sql) + "')";
				auto lpts_start = std::chrono::steady_clock::now();
				unique_ptr<MaterializedQueryResult> check_result;
				try {
					check_result = con.Query(check_sql);
				} catch (std::exception &ex) {
					error = ex.what();
				} catch (...) {
					error = "unknown LPTS exception";
				}
				auto lpts_end = std::chrono::steady_clock::now();
				summary.lpts_check_time_ms =
				    std::chrono::duration<double, std::milli>(lpts_end - lpts_start).count();

				if (error.empty() && check_result && check_result->HasError()) {
					error = check_result->GetError();
				}
				if (!error.empty()) {
					summary.error = TrimError(error);
					summary.phase = "lpts_check";
					if (IsCrashError(summary.error)) {
						summary.state = QueryState::CRASH;
					} else if (IsNotImplementedError(summary.error)) {
						summary.state = QueryState::NOT_IMPLEMENTED;
					} else {
						summary.state = QueryState::LPTS_ERROR;
					}
				} else if (!check_result || check_result->RowCount() == 0) {
					summary.error = "lpts_check returned no rows";
					summary.phase = "lpts_check";
					summary.state = QueryState::LPTS_ERROR;
				} else {
					auto match_value = check_result->GetValue(0, 0);
					summary.state = match_value.ToString() == "true" ? QueryState::SUCCESS : QueryState::INCORRECT;
					if (summary.state == QueryState::INCORRECT) {
						summary.phase = "lpts_check";
						summary.error = "lpts_check returned false";
					}
				}
			}

			uint8_t state = static_cast<uint8_t>(summary.state);
			WriteAllBytes(write_fd, &summary.duckdb_time_ms, sizeof(summary.duckdb_time_ms));
			WriteAllBytes(write_fd, &summary.lpts_check_time_ms, sizeof(summary.lpts_check_time_ms));
			WriteAllBytes(write_fd, &state, sizeof(state));
			uint32_t phase_len = static_cast<uint32_t>(summary.phase.size());
			WriteAllBytes(write_fd, &phase_len, sizeof(phase_len));
			if (phase_len > 0) {
				WriteAllBytes(write_fd, summary.phase.data(), phase_len);
			}
			uint32_t error_len = static_cast<uint32_t>(summary.error.size());
			WriteAllBytes(write_fd, &error_len, sizeof(error_len));
			if (error_len > 0) {
				WriteAllBytes(write_fd, summary.error.data(), error_len);
			}

			if (summary.state == QueryState::CRASH) {
				_exit(1);
			}
		}
	} catch (...) {
		_exit(2);
	}
	_exit(0);
}

struct ForkWorker {
	pid_t child_pid = -1;
	int to_child_fd = -1;
	int from_child_fd = -1;
	string db_path;
	QueryResultSummary result;

	enum SubmitResult { SR_OK, SR_TIMEOUT, SR_CHILD_DIED };

	void Start() {
		Stop();
		int to_child[2], from_child[2];
		if (pipe(to_child) != 0 || pipe(from_child) != 0) {
			throw std::runtime_error("pipe() failed");
		}

		child_pid = fork();
		if (child_pid < 0) {
			close(to_child[0]);
			close(to_child[1]);
			close(from_child[0]);
			close(from_child[1]);
			throw std::runtime_error("fork() failed");
		}

		if (child_pid == 0) {
			close(to_child[1]);
			close(from_child[0]);
			ChildWorkerMain(to_child[0], from_child[1], db_path);
		}

		close(to_child[0]);
		close(from_child[1]);
		to_child_fd = to_child[1];
		from_child_fd = from_child[0];
	}

	void Stop() {
		if (child_pid > 0) {
			uint32_t zero = 0;
			WriteAllBytes(to_child_fd, &zero, sizeof(zero));

			int status;
			for (int i = 0; i < 4; i++) {
				int w = waitpid(child_pid, &status, WNOHANG);
				if (w != 0) {
					break;
				}
				usleep(50000);
			}
			int w = waitpid(child_pid, &status, WNOHANG);
			if (w == 0) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, &status, 0);
			}
			child_pid = -1;
		}
		if (to_child_fd >= 0) {
			close(to_child_fd);
			to_child_fd = -1;
		}
		if (from_child_fd >= 0) {
			close(from_child_fd);
			from_child_fd = -1;
		}
	}

	size_t GetChildRSS() {
		if (child_pid <= 0) {
			return 0;
		}
		char path[64];
		snprintf(path, sizeof(path), "/proc/%d/statm", child_pid);
		FILE *f = fopen(path, "r");
		if (!f) {
			return 0;
		}
		long pages = 0, rss = 0;
		if (fscanf(f, "%ld %ld", &pages, &rss) != 2) {
			rss = 0;
		}
		fclose(f);
		return static_cast<size_t>(rss) * static_cast<size_t>(sysconf(_SC_PAGESIZE));
	}

	SubmitResult Submit(const string &sql, double timeout_s) {
		result = QueryResultSummary();
		if (child_pid <= 0) {
			return SR_CHILD_DIED;
		}

		uint32_t sql_len = static_cast<uint32_t>(sql.size());
		if (!WriteAllBytes(to_child_fd, &sql_len, sizeof(sql_len)) ||
		    !WriteAllBytes(to_child_fd, sql.data(), sql_len)) {
			int status;
			waitpid(child_pid, &status, 0);
			child_pid = -1;
			result.state = QueryState::CRASH;
			result.error = "child process died (write failed)";
			return SR_CHILD_DIED;
		}

		auto deadline = std::chrono::steady_clock::now() +
		                std::chrono::duration_cast<std::chrono::steady_clock::duration>(
		                    std::chrono::duration<double>(timeout_s));

		while (true) {
			auto now = std::chrono::steady_clock::now();
			if (now >= deadline) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, nullptr, 0);
				child_pid = -1;
				result.state = QueryState::TIMEOUT;
				result.error = "timeout";
				return SR_TIMEOUT;
			}

			int remaining_ms =
			    static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
			int poll_ms = std::min(remaining_ms, 500);

			struct pollfd pfd;
			pfd.fd = from_child_fd;
			pfd.events = POLLIN;
			pfd.revents = 0;
			int ret = poll(&pfd, 1, poll_ms);

			if (ret > 0 && (pfd.revents & POLLIN)) {
				uint8_t state = 0;
				if (!ReadAllBytes(from_child_fd, &result.duckdb_time_ms, sizeof(result.duckdb_time_ms)) ||
				    !ReadAllBytes(from_child_fd, &result.lpts_check_time_ms, sizeof(result.lpts_check_time_ms)) ||
				    !ReadAllBytes(from_child_fd, &state, sizeof(state))) {
					int status;
					waitpid(child_pid, &status, 0);
					child_pid = -1;
					result.state = QueryState::CRASH;
					result.error = "child process died (read failed)";
					return SR_CHILD_DIED;
				}
				result.state = static_cast<QueryState>(state);

				uint32_t phase_len = 0;
				if (!ReadAllBytes(from_child_fd, &phase_len, sizeof(phase_len))) {
					return SR_CHILD_DIED;
				}
				if (phase_len > 0) {
					result.phase.resize(phase_len);
					if (!ReadAllBytes(from_child_fd, &result.phase[0], phase_len)) {
						return SR_CHILD_DIED;
					}
				}
				uint32_t error_len = 0;
				if (!ReadAllBytes(from_child_fd, &error_len, sizeof(error_len))) {
					return SR_CHILD_DIED;
				}
				if (error_len > 0) {
					result.error.resize(error_len);
					if (!ReadAllBytes(from_child_fd, &result.error[0], error_len)) {
						return SR_CHILD_DIED;
					}
				}

				if (result.state == QueryState::CRASH) {
					int status;
					waitpid(child_pid, &status, 0);
					child_pid = -1;
					return SR_CHILD_DIED;
				}
				return SR_OK;
			}

			if (ret > 0 && (pfd.revents & (POLLHUP | POLLERR))) {
				int status;
				waitpid(child_pid, &status, 0);
				child_pid = -1;
				result.state = QueryState::CRASH;
				result.error = "child process died unexpectedly";
				return SR_CHILD_DIED;
			}

			int status;
			int w = waitpid(child_pid, &status, WNOHANG);
			if (w > 0) {
				child_pid = -1;
				result.state = QueryState::CRASH;
				result.error = "child process died unexpectedly";
				return SR_CHILD_DIED;
			}

			size_t rss = GetChildRSS();
			if (rss > 25ULL * 1024 * 1024 * 1024) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, nullptr, 0);
				child_pid = -1;
				result.state = QueryState::TIMEOUT;
				result.error = "memory limit exceeded (RSS > 25GB)";
				return SR_TIMEOUT;
			}
		}
	}

	~ForkWorker() {
		Stop();
	}
};

static QueryResultSummary RunQuery(ForkWorker &worker, const string &sql, double timeout_s) {
	auto start = std::chrono::steady_clock::now();
	auto submit_result = worker.Submit(sql, timeout_s);
	auto end = std::chrono::steady_clock::now();

	if (submit_result == ForkWorker::SR_OK) {
		return worker.result;
	}

	QueryResultSummary summary = worker.result;
	if (submit_result == ForkWorker::SR_TIMEOUT) {
		summary.state = QueryState::TIMEOUT;
	} else {
		summary.state = QueryState::CRASH;
		if (summary.error.empty()) {
			summary.error = "child process died";
		}
	}
	if (summary.duckdb_time_ms == 0 && summary.lpts_check_time_ms == 0) {
		summary.duckdb_time_ms = std::chrono::duration<double, std::milli>(end - start).count();
	}
	return summary;
}

static void TrackStats(PassStats &stats, const string &name, const QueryResultSummary &summary) {
	switch (summary.state) {
	case QueryState::SUCCESS:
		stats.success++;
		stats.total_duckdb_success_ms += summary.duckdb_time_ms;
		stats.total_lpts_check_success_ms += summary.lpts_check_time_ms;
		break;
	case QueryState::INCORRECT:
		stats.incorrect++;
		stats.incorrect_queries.push_back(name);
		break;
	case QueryState::DUCKDB_ERROR:
		stats.duckdb_error++;
		break;
	case QueryState::LPTS_ERROR:
		stats.lpts_error++;
		break;
	case QueryState::NOT_IMPLEMENTED:
		stats.not_implemented++;
		break;
	case QueryState::TIMEOUT:
		stats.timed_out++;
		stats.timeout_queries.push_back(name);
		break;
	case QueryState::CRASH:
		stats.crashed++;
		stats.crash_queries.push_back(name);
		break;
	}

	if (!summary.error.empty()) {
		stats.error_counts[summary.error]++;
		stats.error_queries[summary.error].push_back(name);
	}
}

static string FormatQueryList(const vector<string> &names, size_t max_show = 20) {
	string out;
	for (size_t i = 0; i < names.size(); ++i) {
		if (i > 0) {
			out += ", ";
		}
		if (i >= max_show) {
			out += "... +" + std::to_string(names.size() - i) + " more";
			break;
		}
		out += names[i];
	}
	return out;
}

static void PrintStats(const PassStats &stats, int total) {
	Log("========================================");
	Log("=== LPTS SQLStorm TPC-H RESULTS ===");
	Log("========================================");
	Log("  Total:        " + std::to_string(total));
	Log("  Success:      " + std::to_string(stats.success) + " (" + FormatNumber(100.0 * stats.success / total) + "%)");
	Log("  Incorrect:    " + std::to_string(stats.incorrect) + " (" +
	    FormatNumber(100.0 * stats.incorrect / total) + "%)");
	Log("  DuckDB error: " + std::to_string(stats.duckdb_error) + " (" +
	    FormatNumber(100.0 * stats.duckdb_error / total) + "%)");
	Log("  LPTS error:   " + std::to_string(stats.lpts_error) + " (" +
	    FormatNumber(100.0 * stats.lpts_error / total) + "%)");
	Log("  Not implemented: " + std::to_string(stats.not_implemented) + " (" +
	    FormatNumber(100.0 * stats.not_implemented / total) + "%)");
	Log("  Timeout:      " + std::to_string(stats.timed_out) + " (" +
	    FormatNumber(100.0 * stats.timed_out / total) + "%)");
	Log("  Crash:        " + std::to_string(stats.crashed) + " (" +
	    FormatNumber(100.0 * stats.crashed / total) + "%)");
	if (stats.success > 0) {
		Log("  DuckDB time (successful):     " + FormatNumber(stats.total_duckdb_success_ms) + " ms");
		Log("  lpts_check time (successful): " + FormatNumber(stats.total_lpts_check_success_ms) + " ms");
	}
	if (!stats.incorrect_queries.empty()) {
		Log("=== Incorrect queries (" + std::to_string(stats.incorrect_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(stats.incorrect_queries));
	}
	if (!stats.timeout_queries.empty()) {
		Log("=== Timeouts (" + std::to_string(stats.timeout_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(stats.timeout_queries));
	}
	if (!stats.crash_queries.empty()) {
		Log("=== Crashes (" + std::to_string(stats.crash_queries.size()) + ") ===");
		Log("  queries: " + FormatQueryList(stats.crash_queries));
	}
	if (!stats.error_counts.empty()) {
		vector<std::pair<string, int>> sorted_errors(stats.error_counts.begin(), stats.error_counts.end());
		std::sort(sorted_errors.begin(), sorted_errors.end(),
		          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
			          return a.second > b.second;
		          });
		Log("=== Error Breakdown ===");
		for (auto &entry : sorted_errors) {
			string msg = entry.first;
			if (msg.size() > 90) {
				msg = msg.substr(0, 90) + "...";
			}
			Log("  " + std::to_string(entry.second) + "x " + msg);
			auto it = stats.error_queries.find(entry.first);
			if (it != stats.error_queries.end()) {
				Log("    queries: " + FormatQueryList(it->second, 10));
			}
		}
	}
}

static bool WriteCSV(const string &out_csv, const vector<string> &query_files,
                     const vector<QueryResultSummary> &summaries) {
	std::ofstream out(out_csv);
	if (!out.is_open()) {
		Log("ERROR: Cannot open CSV for writing: " + out_csv);
		return false;
	}
	out << "query_index,query,duckdb_time_ms,lpts_check_time_ms,state,phase,error\n";
	for (size_t i = 0; i < query_files.size() && i < summaries.size(); ++i) {
		const auto &summary = summaries[i];
		out << (i + 1) << "," << EscapeCSV(QueryName(query_files[i])) << "," << std::fixed << std::setprecision(3)
		    << summary.duckdb_time_ms << "," << summary.lpts_check_time_ms << ","
		    << StateToString(summary.state) << "," << EscapeCSV(summary.phase) << ","
		    << EscapeCSV(summary.error) << "\n";
	}
	Log("CSV written to: " + out_csv);
	return true;
}

int RunSQLStormBenchmark(const string &queries_dir, const string &out_csv, double timeout_s, double tpch_sf) {
	try {
		string sf_str = FormatSF(tpch_sf);
		string qdir = queries_dir.empty() ? FindQueriesDir() : queries_dir;
		if (qdir.empty() || !FileExists(qdir)) {
			Log("ERROR: Cannot find SQLStorm TPC-H queries directory.");
			Log("Provide it with --queries <path>");
			return 1;
		}

		auto query_files = CollectQueryFiles(qdir);
		if (query_files.empty()) {
			Log("ERROR: No .sql files found in " + qdir);
			return 1;
		}

		string csv_path = out_csv.empty() ? ("benchmark/sqlstorm/lpts_sqlstorm_tpch_sf" + sf_str + ".csv") : out_csv;
		string db_path = "tpch_sf" + sf_str + "_lpts_sqlstorm.db";
		bool db_exists = FileExists(db_path);

		Log("LPTS SQLStorm TPC-H benchmark");
		Log("Queries directory: " + qdir);
		Log("Found " + std::to_string(query_files.size()) + " queries");
		Log("TPC-H scale factor: " + sf_str);
		Log("Timeout: " + FormatNumber(timeout_s) + "s");

		{
			DuckDB setup_db(db_path);
			Connection setup_con(setup_db);
			ConfigureConnection(setup_con);
			if (!db_exists) {
				Log("Generating TPC-H SF" + sf_str + " data...");
				auto t0 = std::chrono::steady_clock::now();
				auto dbgen_result = setup_con.Query("CALL dbgen(sf=" + sf_str + ");");
				auto t1 = std::chrono::steady_clock::now();
				if (dbgen_result->HasError()) {
					throw std::runtime_error("CALL dbgen failed: " + dbgen_result->GetError());
				}
				double gen_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
				Log("TPC-H data generated in " + FormatNumber(gen_ms) + " ms");
			} else {
				Log("Using existing database: " + db_path);
			}
			setup_con.Query("CHECKPOINT");
		}

		PassStats stats;
		vector<QueryResultSummary> summaries;
		summaries.reserve(query_files.size());
		int total = static_cast<int>(query_files.size());
		int log_interval = std::max(1, total / 10);

		ForkWorker worker;
		worker.db_path = db_path;
		worker.Start();

		for (int i = 0; i < total; ++i) {
			if (worker.child_pid <= 0) {
				worker.Start();
			}

			auto &query_file = query_files[i];
			string name = QueryName(query_file);
			string sql = ReadFileToString(query_file);
			QueryResultSummary summary;
			if (sql.empty()) {
				summary.state = QueryState::DUCKDB_ERROR;
				summary.phase = "duckdb";
				summary.error = "empty query file";
			} else {
				summary = RunQuery(worker, sql, timeout_s);
			}

			if (summary.state == QueryState::TIMEOUT || summary.state == QueryState::CRASH) {
				worker.Stop();
			}

			TrackStats(stats, name, summary);
			summaries.push_back(summary);

			if ((i + 1) % log_interval == 0 || i + 1 == total) {
				Log("[" + std::to_string(i + 1) + "/" + std::to_string(total) + "] success=" +
				    std::to_string(stats.success) + " incorrect=" + std::to_string(stats.incorrect) +
				    " duckdb_error=" + std::to_string(stats.duckdb_error) +
				    " lpts_error=" + std::to_string(stats.lpts_error) +
				    " not_implemented=" + std::to_string(stats.not_implemented) +
				    " timeout=" + std::to_string(stats.timed_out) + " crash=" +
				    std::to_string(stats.crashed));
			}
		}

		worker.Stop();
		PrintStats(stats, total);
		if (!WriteCSV(csv_path, query_files, summaries)) {
			return 1;
		}
		return 0;
	} catch (std::exception &ex) {
		Log(string("Fatal error: ") + ex.what());
		return 2;
	}
}

} // namespace duckdb

static void PrintUsage() {
	std::cout << "Usage: lpts_sqlstorm_benchmark [options]\n"
	          << "Options:\n"
	          << "  --queries <dir>    SQLStorm TPC-H queries directory\n"
	          << "                     (default: benchmark/sqlstorm/SQLStorm/v1.0/tpch/queries)\n"
	          << "  --out <csv>        Output CSV path (default: benchmark/sqlstorm/lpts_sqlstorm_tpch_sf*.csv)\n"
	          << "  --timeout <sec>    Per-query timeout in seconds (default: 10)\n"
	          << "  --tpch_sf <float>  TPC-H scale factor (default: 1.0)\n"
	          << "  -h, --help         Show this help message\n";
}

int main(int argc, char **argv) {
	signal(SIGPIPE, SIG_IGN);

	std::string queries_dir;
	std::string out_csv;
	double timeout_s = 10.0;
	double tpch_sf = 1.0;

	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "-h" || arg == "--help") {
			PrintUsage();
			return 0;
		} else if (arg == "--queries" && i + 1 < argc) {
			queries_dir = argv[++i];
		} else if (arg == "--out" && i + 1 < argc) {
			out_csv = argv[++i];
		} else if (arg == "--timeout" && i + 1 < argc) {
			timeout_s = std::stod(argv[++i]);
		} else if (arg == "--tpch_sf" && i + 1 < argc) {
			tpch_sf = std::stod(argv[++i]);
		} else {
			std::cerr << "Unknown option: " << arg << "\n";
			PrintUsage();
			return 1;
		}
	}

	return duckdb::RunSQLStormBenchmark(queries_dir, out_csv, timeout_s, tpch_sf);
}
