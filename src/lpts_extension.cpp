#define DUCKDB_EXTENSION_MAIN

#include "lpts_extension.hpp"
#include "logical_plan_to_sql.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// PRAGMA lpts('query') — converts a SQL query's logical plan to a SQL string.
//
// Uses DuckDB's pragma_query_t mechanism: the function returns a substitute SQL
// query that DuckDB then executes. So we return "SELECT '<result>' AS sql;"
// which displays the converted SQL string to the user.
//------------------------------------------------------------------------------

static string LptsPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	auto query = StringValue::Get(parameters.values[0]);

	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty()) {
		throw ParserException("Failed to parse query: %s", query);
	}

	Planner planner(context);
	planner.CreatePlan(parser.statements[0]->Copy());

#if LPTS_DEBUG
	LPTS_DEBUG_PRINT("[LPTS] Logical plan for: " + query);
	planner.plan->Print();
#endif

	LogicalPlanToSql lpts(context, planner.plan);
	auto cte_list = lpts.LogicalPlanToCteList();
	string result_sql = cte_list->ToQuery(true);

	// Return a substitute query that displays the result
	string escaped = EscapeSingleQuotes(result_sql);
	return "SELECT '" + escaped + "' AS sql;";
}

//------------------------------------------------------------------------------
// Table function lpts_query('query') — for programmatic use.
//
// Unlike the PRAGMA, this returns the result as a proper table row, making it
// usable in SELECT queries and sqllogictest files:
//   SELECT * FROM lpts_query('SELECT ...');
//------------------------------------------------------------------------------

struct LptsBindData : public TableFunctionData {
	string result_sql;
};

struct LptsGlobalState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> LptsTableBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto query = StringValue::Get(input.inputs[0]);

	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty()) {
		throw ParserException("Failed to parse query: %s", query);
	}

	Planner planner(context);
	planner.CreatePlan(parser.statements[0]->Copy());

#if LPTS_DEBUG
	LPTS_DEBUG_PRINT("[LPTS] Logical plan for: " + query);
	planner.plan->Print();
#endif

	LogicalPlanToSql lpts(context, planner.plan);
	auto cte_list = lpts.LogicalPlanToCteList();

	auto result = make_uniq<LptsBindData>();
	result->result_sql = cte_list->ToQuery(true);

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sql");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> LptsTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<LptsGlobalState>();
}

static void LptsTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = dynamic_cast<LptsGlobalState &>(*data_p.global_state);
	if (state.done) {
		return;
	}
	auto &bind_data = dynamic_cast<const LptsBindData &>(*data_p.bind_data);
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(bind_data.result_sql));
	state.done = true;
}

//------------------------------------------------------------------------------
// Extension loading
//------------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	// Register PRAGMA lpts('query')
	auto pragma = PragmaFunction::PragmaCall("lpts", LptsPragmaFunction, {LogicalType::VARCHAR});
	loader.RegisterFunction(pragma);

	// Register table function lpts_query('query') for SELECT * FROM lpts_query(...)
	TableFunction table_func("lpts_query", {LogicalType::VARCHAR}, LptsTableFunc, LptsTableBind, LptsTableInit);
	loader.RegisterFunction(table_func);
}

void LptsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LptsExtension::Name() {
	return "lpts";
}

std::string LptsExtension::Version() const {
#ifdef EXT_VERSION_LPTS
	return EXT_VERSION_LPTS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(lpts, loader) {
	duckdb::LoadInternal(loader);
}
}
