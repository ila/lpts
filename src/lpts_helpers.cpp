#include "lpts_helpers.hpp"

#include <regex>

namespace duckdb {

string VecToSeparatedList(vector<string> input_list, const string &separator) {
	std::ostringstream ret_str;
	for (size_t i = 0; i < input_list.size(); ++i) {
		ret_str << input_list[i];
		if (i != input_list.size() - 1) {
			ret_str << separator;
		}
	}
	return ret_str.str();
}

string EscapeSingleQuotes(const string &input) {
	std::stringstream escaped_stream;
	for (char c : input) {
		if (c == '\'') {
			escaped_stream << "''";
		} else {
			escaped_stream << c;
		}
	}
	return escaped_stream.str();
}

string SQLToLowercase(const string &sql) {
	std::stringstream lowercase_stream;
	bool in_string = false;
	for (char c : sql) {
		if (c == '\'') {
			in_string = !in_string;
		}
		if (!in_string) {
			lowercase_stream << (char)tolower(c);
		} else {
			lowercase_stream << c;
		}
	}
	return lowercase_stream.str();
}

void RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

} // namespace duckdb
