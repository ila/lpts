#define DUCKDB_EXTENSION_MAIN

#include "lpts_extension.hpp"
#include "cte_nodes.hpp"
#include "lpts_ast.hpp"
#include "lpts_ast_renderer.hpp"
#include "lpts_pipeline.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Helper: read lpts_dialect from the session settings.
//   Defaults to DuckDB if not set.
//------------------------------------------------------------------------------
static SqlDialect ReadDialect(ClientContext &context) {
	Value dialect_val;
	if (context.TryGetCurrentSetting("lpts_dialect", dialect_val)) {
		return ParseSqlDialect(dialect_val.GetValue<string>());
	}
	return SqlDialect::DUCKDB;
}

/// Plan a query and run it through the optimizer, returning the optimized
/// logical plan. This ensures LPTS sees the same plan DuckDB would execute.
///
/// Some optimizers are disabled because they produce plan nodes or structures
/// that LPTS cannot yet convert back to SQL:
///   - COLUMN_LIFETIME: changes column bindings in ways that break CTE references
///   - STATISTICS_PROPAGATION: triggers DUMMY_SCAN for constant-foldable queries
///   - REORDER_FILTER: introduces optional filter prefixes in scan filters
///   - TOP_N: fuses ORDER+LIMIT into a single node LPTS can't serialize
///   - JOIN_FILTER_PUSHDOWN: wraps pushed-down filters in OptionalFilter ("optional:" prefix)
///   - CTE_INLINING: introduces CTE_SCAN nodes
///   - MATERIALIZED_CTE: introduces CTE_SCAN nodes
///   - COMMON_SUBPLAN: introduces CTE_SCAN nodes
///
/// TODO: research whether these can be re-enabled by adding support for the
/// plan structures they produce (EMPTY_RESULT node, optional filters, CTE_SCAN, etc.)
static unique_ptr<LogicalOperator> PlanQuery(ClientContext &context, const string &query) {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty()) {
		throw ParserException("Failed to parse query: %s", query);
	}
	Planner planner(context);
	planner.CreatePlan(parser.statements[0]->Copy());

	// Temporarily disable optimizers that produce plan structures LPTS has not implemented yet.
	auto &config = DBConfig::GetConfig(context);
	auto saved = config.options.disabled_optimizers;
	config.options.disabled_optimizers.insert(OptimizerType::COLUMN_LIFETIME);
	config.options.disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
	config.options.disabled_optimizers.insert(OptimizerType::REORDER_FILTER);
	config.options.disabled_optimizers.insert(OptimizerType::TOP_N);
	config.options.disabled_optimizers.insert(OptimizerType::JOIN_FILTER_PUSHDOWN);
	config.options.disabled_optimizers.insert(OptimizerType::CTE_INLINING);
	config.options.disabled_optimizers.insert(OptimizerType::MATERIALIZED_CTE);
	config.options.disabled_optimizers.insert(OptimizerType::COMMON_SUBPLAN);

#if LPTS_DEBUG
	{
		string dump = "[LPTS] disabled_optimizers BEFORE LPTS optimize: {";
		for (auto &t : config.options.disabled_optimizers) {
			dump += OptimizerTypeToString(t) + ", ";
		}
		dump += "}";
		LPTS_DEBUG_PRINT(dump);
	}
#endif

	Optimizer optimizer(*planner.binder, context);
	auto result = optimizer.Optimize(std::move(planner.plan));

#if LPTS_DEBUG
	// Re-enable ALL optimizers (clear disabled set) and re-plan for side-by-side comparison.
	config.options.disabled_optimizers.clear();
	{
		string dump = "[LPTS] disabled_optimizers BEFORE full optimize: {";
		for (auto &t : config.options.disabled_optimizers) {
			dump += OptimizerTypeToString(t) + ", ";
		}
		dump += "}";
		LPTS_DEBUG_PRINT(dump);
	}

	Parser full_parser;
	full_parser.ParseQuery(query);
	Planner full_planner(context);
	full_planner.CreatePlan(full_parser.statements[0]->Copy());
	Optimizer full_optimizer(*full_planner.binder, context);
	auto full_result = full_optimizer.Optimize(std::move(full_planner.plan));

	LPTS_DEBUG_PRINT("[LPTS] ===== Plan WITH LPTS-disabled optimizers (used by LPTS) =====");
	result->Print();
	LPTS_DEBUG_PRINT("[LPTS] ===== Plan WITH ALL optimizers enabled =====");
	full_result->Print();
	LPTS_DEBUG_PRINT("[LPTS] ===== end plan comparison =====");
#endif

	// Restore original disabled_optimizers set
	config.options.disabled_optimizers = saved;
	return result;
}

static string StripTrailingSemicolon(string sql) {
	while (!sql.empty() &&
	       (sql.back() == ';' || sql.back() == ' ' || sql.back() == '\n' || sql.back() == '\r' || sql.back() == '\t')) {
		sql.pop_back();
	}
	return sql;
}

static string FirstStatementSqlForSubquery(const string &query) {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty()) {
		throw ParserException("Failed to parse query: %s", query);
	}
	return StripTrailingSemicolon(parser.statements[0]->ToString());
}

//------------------------------------------------------------------------------
// PRAGMA lpts('query') — converts a SQL query's logical plan to a SQL string.
//
// Uses DuckDB's pragma_query_t mechanism: the function returns a substitute SQL
// query that DuckDB then executes. So we return "SELECT '<result>' AS sql;"
// which displays the converted SQL string to the user.
//------------------------------------------------------------------------------

static string LptsPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	auto query = StringValue::Get(parameters.values[0]);
	auto plan = PlanQuery(context, query);

#if LPTS_DEBUG
	LPTS_DEBUG_PRINT("[LPTS] Logical plan for: " + query);
	plan->Print();
#endif

	SqlDialect dialect = ReadDialect(context);
	auto ast = LogicalPlanToAst(context, plan, dialect);
	auto cte_list = AstToCteList(*ast, dialect);
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
	auto plan = PlanQuery(context, query);

#if LPTS_DEBUG
	LPTS_DEBUG_PRINT("[LPTS] Logical plan for: " + query);
	plan->Print();
#endif

	SqlDialect dialect = ReadDialect(context);
	auto ast = LogicalPlanToAst(context, plan, dialect);
	auto cte_list = AstToCteList(*ast, dialect);

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
// PRAGMA lpts_exec('query') — converts a SQL query via LPTS then executes it.
//
// Runs the query through the full LPTS pipeline (plan → AST → SQL) and then
// executes the generated SQL, returning its results directly.
//------------------------------------------------------------------------------

static string LptsExecPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	auto query = StringValue::Get(parameters.values[0]);
	auto plan = PlanQuery(context, query);

	SqlDialect dialect = ReadDialect(context);
	auto ast = LogicalPlanToAst(context, plan, dialect);
	auto cte_list = AstToCteList(*ast, dialect);
	return cte_list->ToQuery(true);
}

//------------------------------------------------------------------------------
// PRAGMA lpts_check('query') — round-trip correctness check.
//
// Runs the original query and the LPTS-generated query, then compares results
// using EXCEPT ALL in both directions. Returns a single boolean column "match".
//
// A false result means strict bag equality failed. This can be a real LPTS bug,
// but it can also happen for SQL with nondeterministic result values, e.g.
// unordered string_agg/list aggregates, row_number() over tied ORDER BY keys, or
// ORDER BY ... LIMIT with tied boundary rows.
//------------------------------------------------------------------------------

static string LptsCheckPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	auto query = StringValue::Get(parameters.values[0]);
	auto plan = PlanQuery(context, query);

	SqlDialect dialect = ReadDialect(context);
	auto ast = LogicalPlanToAst(context, plan, dialect);
	auto cte_list = AstToCteList(*ast, dialect);
	string lpts_sql = cte_list->ToQuery(true);

	// Normalize the original query to DuckDB's first parsed statement before embedding
	// it as a subquery. Raw SQLStorm inputs often end with "LIMIT ...; -- comment",
	// where simply trimming the last character leaves a semicolon inside the subquery.
	string orig = FirstStatementSqlForSubquery(query);
	lpts_sql = StripTrailingSemicolon(std::move(lpts_sql));

	string escaped_lpts = EscapeSingleQuotes(lpts_sql);

	// Compare: no rows in (A EXCEPT ALL B) AND no rows in (B EXCEPT ALL A)
	return "SELECT "
	       "(SELECT count(*) FROM ((" +
	       orig + ") EXCEPT ALL (" + lpts_sql +
	       "))) = 0 AND "
	       "(SELECT count(*) FROM ((" +
	       lpts_sql + ") EXCEPT ALL (" + orig + "))) = 0 AS match;";
}

//------------------------------------------------------------------------------
// PRAGMA print_ast('query') — shows the AST tree for a SQL query.
//------------------------------------------------------------------------------

static void PrintAstPragmaFunction(ClientContext &context, const FunctionParameters &parameters) {
	auto query = StringValue::Get(parameters.values[0]);
	auto plan = PlanQuery(context, query);

	auto ast = LogicalPlanToAst(context, plan);
	string rendered = RenderAstTree(*ast);
	Printer::RawPrint(OutputStream::STREAM_STDOUT, rendered);
}

//------------------------------------------------------------------------------
// Table function print_ast_query('query') — for programmatic use.
//------------------------------------------------------------------------------

struct PrintAstBindData : public TableFunctionData {
	string rendered;
};

static unique_ptr<FunctionData> PrintAstTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto query = StringValue::Get(input.inputs[0]);
	auto plan = PlanQuery(context, query);

	auto ast = LogicalPlanToAst(context, plan);

	auto result = make_uniq<PrintAstBindData>();
	result->rendered = PrintAst(*ast);

	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("ast");

	return std::move(result);
}

static void PrintAstTableFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = dynamic_cast<LptsGlobalState &>(*data_p.global_state);
	if (state.done) {
		return;
	}
	auto &bind_data = dynamic_cast<const PrintAstBindData &>(*data_p.bind_data);
	output.SetCardinality(1);
	output.SetValue(0, 0, Value(bind_data.rendered));
	state.done = true;
}

//------------------------------------------------------------------------------
// Extension loading
//------------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	// Register the lpts_dialect session setting.
	// Users can change it with: SET lpts_dialect = 'postgres';
	DBConfig &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("lpts_dialect",
	                          "SQL dialect for lpts output. Valid values: 'duckdb' (default), 'postgres'",
	                          LogicalType::VARCHAR, Value("duckdb"));

	// Register PRAGMA lpts('query')
	auto pragma = PragmaFunction::PragmaCall("lpts", LptsPragmaFunction, {LogicalType::VARCHAR});
	loader.RegisterFunction(pragma);

	// Register table function lpts_query('query') for SELECT * FROM lpts_query(...)
	TableFunction table_func("lpts_query", {LogicalType::VARCHAR}, LptsTableFunc, LptsTableBind, LptsTableInit);
	loader.RegisterFunction(table_func);

	// Register PRAGMA lpts_exec('query') — round-trip: plan → SQL → execute
	auto lpts_exec_pragma = PragmaFunction::PragmaCall("lpts_exec", LptsExecPragmaFunction, {LogicalType::VARCHAR});
	loader.RegisterFunction(lpts_exec_pragma);

	// Register PRAGMA lpts_check('query') — round-trip correctness check
	auto lpts_check_pragma = PragmaFunction::PragmaCall("lpts_check", LptsCheckPragmaFunction, {LogicalType::VARCHAR});
	loader.RegisterFunction(lpts_check_pragma);

	// Register PRAGMA print_ast('query')
	auto print_ast_pragma = PragmaFunction::PragmaCall("print_ast", PrintAstPragmaFunction, {LogicalType::VARCHAR});
	loader.RegisterFunction(print_ast_pragma);

	// Register table function print_ast_query('query')
	TableFunction print_ast_table("print_ast_query", {LogicalType::VARCHAR}, PrintAstTableFunc, PrintAstTableBind,
	                              LptsTableInit);
	loader.RegisterFunction(print_ast_table);
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
