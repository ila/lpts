#pragma once

#include "lpts_ast.hpp"
#include "logical_plan_to_sql.hpp"

namespace duckdb {

//==============================================================================
// SqlDialect
//
// Selects the SQL dialect used for CTE/SQL generation.
// Set via: SET lpts_dialect = 'postgres';
//==============================================================================
enum class SqlDialect {
	DUCKDB,  ///< Default. Uses DuckDB-specific syntax (fully-qualified table refs, etc.)
	POSTGRES ///< PostgreSQL-compatible syntax (unqualified table refs, etc.)
};

/// Parse a dialect string ("duckdb" or "postgres") into the enum.
/// Throws InvalidInputException on unrecognised values.
SqlDialect ParseSqlDialect(const string &value);

/// Phase 1: Convert a DuckDB LogicalOperator tree into a dialect-agnostic AST.
/// `dialect` is forwarded to expression serialization for dialect-specific function renaming.
unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                     SqlDialect dialect = SqlDialect::DUCKDB);

/// Phase 2: Convert an AST into a flat CTE list.
/// `dialect` controls dialect-specific SQL rendering (default: DuckDB).
unique_ptr<CteList> AstToCteList(const AstNode &root, SqlDialect dialect = SqlDialect::DUCKDB);

} // namespace duckdb
