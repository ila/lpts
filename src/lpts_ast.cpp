#include "lpts_ast.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// AstNode helpers
//------------------------------------------------------------------------------

string AstNode::Indent(int indent) {
	return string(static_cast<size_t>(indent), ' ');
}

InsertionOrderPreservingMap<string> AstNode::GetExtraInfo() const {
	return InsertionOrderPreservingMap<string>();
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

InsertionOrderPreservingMap<string> AstGetNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	info.insert("Table", catalog + "." + schema + "." + table_name);
	string cols = "[";
	for (size_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			cols += ", ";
		}
		cols += column_names[i];
	}
	cols += "]";
	info.insert("Columns", std::move(cols));
	if (!table_filters.empty()) {
		string filters = "[";
		for (size_t i = 0; i < table_filters.size(); i++) {
			if (i > 0) {
				filters += ", ";
			}
			filters += table_filters[i];
		}
		filters += "]";
		info.insert("Filters", std::move(filters));
	}
	return info;
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

InsertionOrderPreservingMap<string> AstFilterNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	for (size_t i = 0; i < conditions.size(); i++) {
		info.insert("Condition [" + std::to_string(i) + "]", conditions[i]);
	}
	return info;
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

InsertionOrderPreservingMap<string> AstProjectNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	for (size_t i = 0; i < expressions.size(); i++) {
		info.insert(cte_column_names[i], expressions[i]);
	}
	return info;
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

InsertionOrderPreservingMap<string> AstAggregateNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	if (!group_by_columns.empty()) {
		string groups = "[";
		for (size_t i = 0; i < group_by_columns.size(); i++) {
			if (i > 0) {
				groups += ", ";
			}
			groups += group_by_columns[i];
		}
		groups += "]";
		info.insert("Group By", std::move(groups));
	}
	for (size_t i = 0; i < aggregate_expressions.size(); i++) {
		info.insert(cte_column_names[group_by_columns.size() + i], aggregate_expressions[i]);
	}
	return info;
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

InsertionOrderPreservingMap<string> AstJoinNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	info.insert("Type", JoinTypeToString(join_type));
	for (size_t i = 0; i < conditions.size(); i++) {
		info.insert("On [" + std::to_string(i) + "]", conditions[i]);
	}
	return info;
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

InsertionOrderPreservingMap<string> AstUnionNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	info.insert("Type", is_union_all ? "UNION ALL" : "UNION");
	string cols = "[";
	for (size_t i = 0; i < cte_column_names.size(); i++) {
		if (i > 0) {
			cols += ", ";
		}
		cols += cte_column_names[i];
	}
	cols += "]";
	info.insert("Columns", std::move(cols));
	return info;
}

//------------------------------------------------------------------------------
// AstSetOperationNode
//------------------------------------------------------------------------------

string AstSetOperationNode::ToString(int indent) const {
	string result = Indent(indent) + op_name + (is_all ? "All" : "");
	result += " (columns=" + std::to_string(cte_column_names.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

InsertionOrderPreservingMap<string> AstSetOperationNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	info.insert("Type", op_name + (is_all ? " ALL" : ""));
	string cols = "[";
	for (size_t i = 0; i < cte_column_names.size(); i++) {
		if (i > 0) {
			cols += ", ";
		}
		cols += cte_column_names[i];
	}
	cols += "]";
	info.insert("Columns", std::move(cols));
	return info;
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

InsertionOrderPreservingMap<string> AstInsertNode::GetExtraInfo() const {
	InsertionOrderPreservingMap<string> info;
	info.insert("Target", target_table);
	info.insert("On Conflict", EnumUtil::ToString(action_type));
	return info;
}

//------------------------------------------------------------------------------
// AstOrderNode
//------------------------------------------------------------------------------

string AstOrderNode::ToString(int indent) const {
	string result = Indent(indent) + "Order";
	result += " (" + std::to_string(order_items.size()) + " items)";
	for (auto &item : order_items) {
		result += "\n" + Indent(indent + 2) + "by: " + item;
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstLimitNode
//------------------------------------------------------------------------------

string AstLimitNode::ToString(int indent) const {
	string result = Indent(indent) + "Limit";
	if (!limit_str.empty()) {
		result += " LIMIT=" + limit_str;
	}
	if (!offset_str.empty()) {
		result += " OFFSET=" + offset_str;
	}
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstDistinctNode
//------------------------------------------------------------------------------

string AstDistinctNode::ToString(int indent) const {
	string result = Indent(indent) + "Distinct";
	result += " (columns=" + std::to_string(cte_column_names.size()) + ")";
	for (auto &child : children) {
		result += "\n" + child->ToString(indent + 2);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstMaterializedCteNode
//------------------------------------------------------------------------------

string AstMaterializedCteNode::ToString(int indent) const {
	string result = Indent(indent) + "MaterializedCte (cte_table_index=" + std::to_string(cte_table_index) + ")";
	if (children.size() >= 1) {
		result += "\n" + Indent(indent + 2) + "body:";
		result += "\n" + children[0]->ToString(indent + 4);
	}
	if (children.size() >= 2) {
		result += "\n" + Indent(indent + 2) + "outer:";
		result += "\n" + children[1]->ToString(indent + 4);
	}
	return result;
}

//------------------------------------------------------------------------------
// AstCteRefNode
//------------------------------------------------------------------------------

string AstCteRefNode::ToString(int indent) const {
	string result = Indent(indent) + "CteRef (cte_table_index=" + std::to_string(cte_table_index) + ")";
	if (!cte_column_names.empty()) {
		result += "\n" + Indent(indent + 2) + "columns: [";
		for (size_t i = 0; i < cte_column_names.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += cte_column_names[i];
		}
		result += "]";
	}
	return result;
}

//------------------------------------------------------------------------------
// AstDelimGetNode
//------------------------------------------------------------------------------

string AstDelimGetNode::ToString(int indent) const {
	string result = Indent(indent) + "DelimGet (table_index=" + std::to_string(table_index) + ")";
	if (!cte_column_names.empty()) {
		result += "\n" + Indent(indent + 2) + "exposed: [";
		for (size_t i = 0; i < cte_column_names.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += cte_column_names[i];
		}
		result += "]";
	}
	if (!source_col_names.empty()) {
		result += "\n" + Indent(indent + 2) + "source: [";
		for (size_t i = 0; i < source_col_names.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += source_col_names[i];
		}
		result += "]";
	}
	return result;
}

//------------------------------------------------------------------------------
// AstDelimJoinNode
//------------------------------------------------------------------------------

string AstDelimJoinNode::ToString(int indent) const {
	string result = Indent(indent) + "DelimJoin (" + EnumUtil::ToString(join_type) + ")";
	result += " delim_table_indices=[";
	for (size_t i = 0; i < delim_table_indices.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += std::to_string(delim_table_indices[i]);
	}
	result += "]";
	for (const auto &cond : conditions) {
		result += "\n" + Indent(indent + 2) + "on: " + cond;
	}
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
