#pragma once

#include "lpts_ast.hpp"
#include "logical_plan_to_sql.hpp"

namespace duckdb {

/// Phase 1: Convert a DuckDB LogicalOperator tree into a dialect-agnostic AST.
unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan);

/// Phase 2: Convert an AST into a flat CTE list (future: dialect-aware).
unique_ptr<CteList> AstToCteList(const AstNode &root);

} // namespace duckdb
