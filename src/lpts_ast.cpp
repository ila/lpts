#include "lpts_ast.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// AstNode helpers
//------------------------------------------------------------------------------

string AstNode::Indent(int indent) {
	return string(static_cast<size_t>(indent), ' ');
}

//------------------------------------------------------------------------------
// AstGetNode
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
	if (!table_filters.empty()) {
		result += "\n" + Indent(indent + 2) + "filters: [";
		for (size_t i = 0; i < table_filters.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += table_filters[i];
		}
		result += "]";
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstFilterNode
//------------------------------------------------------------------------------

string AstFilterNode::ToString(int indent) const {
	string result = Indent(indent) + "Filter";
	result += " (conditions=" + std::to_string(conditions.size()) + ")";
	for (size_t i = 0; i < conditions.size(); i++) {
		result += "\n" + Indent(indent + 2) + "cond[" + std::to_string(i) + "]: " + conditions[i];
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstProjectNode
//------------------------------------------------------------------------------

string AstProjectNode::ToString(int indent) const {
	string result = Indent(indent) + "Project";
	result +=
	    " (table_index=" + std::to_string(table_index) + ", expressions=" + std::to_string(expressions.size()) + ")";
	for (size_t i = 0; i < expressions.size(); i++) {
		result += "\n" + Indent(indent + 2) + cte_column_names[i] + " <- " + expressions[i];
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstAggregateNode
//------------------------------------------------------------------------------

string AstAggregateNode::ToString(int indent) const {
	string result = Indent(indent) + "Aggregate";
	result += " (groups=" + std::to_string(group_by_columns.size());
	result += ", aggregates=" + std::to_string(aggregate_expressions.size()) + ")";
	if (!group_by_columns.empty()) {
		result += "\n" + Indent(indent + 2) + "group_by: [";
		for (size_t i = 0; i < group_by_columns.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += group_by_columns[i];
		}
		result += "]";
	}
	for (size_t i = 0; i < aggregate_expressions.size(); i++) {
		result += "\n" + Indent(indent + 2) + cte_column_names[group_by_columns.size() + i] + " <- " +
		          aggregate_expressions[i];
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstJoinNode
//------------------------------------------------------------------------------

string AstJoinNode::ToString(int indent) const {
	string result = Indent(indent) + "Join";
	result += " (" + JoinTypeToString(join_type) + ", conditions=" + std::to_string(conditions.size()) + ")";
	for (size_t i = 0; i < conditions.size(); i++) {
		result += "\n" + Indent(indent + 2) + "on: " + conditions[i];
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstUnionNode
//------------------------------------------------------------------------------

string AstUnionNode::ToString(int indent) const {
	string result = Indent(indent) + (is_union_all ? "UnionAll" : "Union");
	result += " (columns=" + std::to_string(cte_column_names.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstInsertNode
//------------------------------------------------------------------------------

string AstInsertNode::ToString(int indent) const {
	string result = Indent(indent) + "Insert";
	result += " into " + target_table;
	result += " (on_conflict=" + EnumUtil::ToString(action_type) + ")";
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
