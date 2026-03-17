#include "lpts_ast_renderer.hpp"
#include "lpts_ast.hpp"

#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"

#include <sstream>

namespace duckdb {

//------------------------------------------------------------------------------
// Layout helpers — mirrors DuckDB's anonymous-namespace functions from
// render_tree.cpp, but operating on AstNode instead of LogicalOperator.
//------------------------------------------------------------------------------

static void GetAstTreeWidthHeight(const AstNode &node, idx_t &width, idx_t &height) {
	if (node.children.empty()) {
		width = 1;
		height = 1;
		return;
	}
	width = 0;
	height = 0;
	for (auto &child : node.children) {
		idx_t child_width, child_height;
		GetAstTreeWidthHeight(*child, child_width, child_height);
		width += child_width;
		height = MaxValue<idx_t>(height, child_height);
	}
	height++;
}

static idx_t CreateAstTreeRecursive(RenderTree &result, const AstNode &node, idx_t x, idx_t y) {
	auto render_node = make_uniq<RenderTreeNode>(node.NodeType(), node.GetExtraInfo());

	if (node.children.empty()) {
		result.SetNode(x, y, std::move(render_node));
		return 1;
	}

	idx_t width = 0;
	for (auto &child : node.children) {
		auto child_x = x + width;
		auto child_y = y + 1;
		render_node->AddChildPosition(child_x, child_y);
		width += CreateAstTreeRecursive(result, *child, child_x, child_y);
	}
	result.SetNode(x, y, std::move(render_node));
	return width;
}

//------------------------------------------------------------------------------
// RenderAstTree
//------------------------------------------------------------------------------

string RenderAstTree(const AstNode &root) {
	idx_t width, height;
	GetAstTreeWidthHeight(root, width, height);

	RenderTree tree(width, height);
	CreateAstTreeRecursive(tree, root, 0, 0);

	TextTreeRenderer renderer;
	std::stringstream ss;
	renderer.ToStream(tree, ss);
	return ss.str();
}

} // namespace duckdb
