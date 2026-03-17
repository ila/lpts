#pragma once

#include "duckdb.hpp"

namespace duckdb {

class AstNode;

/// Render an AST tree using DuckDB's box-drawing TextTreeRenderer.
string RenderAstTree(const AstNode &root);

} // namespace duckdb
