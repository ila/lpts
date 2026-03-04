#include "lpts_pipeline.hpp"

namespace duckdb {

unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	// TODO: walk the LogicalOperator tree and build the corresponding AST.
	throw NotImplementedException("LogicalPlanToAst is not yet implemented");
}

unique_ptr<CteList> AstToCteList(const AstNode &root) {
	// TODO: traverse the AST and produce a flat CTE list.
	throw NotImplementedException("AstToCteList is not yet implemented");
}

} // namespace duckdb
