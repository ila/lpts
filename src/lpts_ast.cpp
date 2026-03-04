#include "lpts_ast.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// AstNode helpers
//------------------------------------------------------------------------------

string AstNode::Indent(int indent) {
	return string(static_cast<size_t>(indent), ' ');
}

//------------------------------------------------------------------------------
// AstGetNode — fully implemented as a worked example
//------------------------------------------------------------------------------

string AstGetNode::ToString(int indent) const {
	string result = Indent(indent) + "Get";
	result += " " + catalog + "." + schema + "." + table_name;
	result += " (table_index=" + std::to_string(table_index) + ")";
	result += "\n" + Indent(indent + 2) + "columns: [";
	for (size_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += column_names[i];
	}
	result += "]";
	// Children (a Get node typically has none, but print them for completeness).
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstFilterNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstFilterNode::ToString(int indent) const {
	// TODO: implement pretty-printing for filter conditions
	string result = Indent(indent) + "Filter";
	result += " (conditions=" + std::to_string(conditions.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstProjectNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstProjectNode::ToString(int indent) const {
	// TODO: implement pretty-printing for projected expressions
	string result = Indent(indent) + "Project";
	result += " (expressions=" + std::to_string(expressions.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstAggregateNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstAggregateNode::ToString(int indent) const {
	// TODO: implement pretty-printing for group-by and aggregate expressions
	string result = Indent(indent) + "Aggregate";
	result += " (groups=" + std::to_string(group_by_columns.size());
	result += ", aggregates=" + std::to_string(aggregate_expressions.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstJoinNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstJoinNode::ToString(int indent) const {
	// TODO: implement pretty-printing for join type and conditions
	string result = Indent(indent) + "Join";
	result += " (conditions=" + std::to_string(conditions.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstUnionNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstUnionNode::ToString(int indent) const {
	// TODO: implement pretty-printing for union details
	string result = Indent(indent) + (is_union_all ? "UnionAll" : "Union");
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstInsertNode — skeleton + TODO
//------------------------------------------------------------------------------

string AstInsertNode::ToString(int indent) const {
	// TODO: implement pretty-printing for insert target and conflict action
	string result = Indent(indent) + "Insert";
	result += " into " + target_table;
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// PrintAst — free function
//------------------------------------------------------------------------------

string PrintAst(const AstNode &root) {
	return root.ToString(0);
}

} // namespace duckdb
