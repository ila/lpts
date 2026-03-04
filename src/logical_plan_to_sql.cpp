#include "logical_plan_to_sql.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"

#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

string FinalReadNode::ToQuery() {
	const size_t col_count = final_column_list.size();
	if (child_cte_column_list.size() != col_count) {
		throw std::runtime_error("Size mismatch between column lists!");
	}
	vector<string> merged_list;
	// Assign the final column names to the CTE column names. Format: "cte_col AS final_col".
	merged_list.reserve(col_count);
	for (size_t i = 0; i < final_column_list.size(); ++i) {
		merged_list.emplace_back(child_cte_column_list[i] + " AS " + final_column_list[i]);
	}
	std::ostringstream sql_str;
	sql_str << "SELECT ";
	sql_str << VecToSeparatedList(std::move(merged_list));
	sql_str << " FROM ";
	sql_str << child_cte_name;
	return sql_str.str();
}

string InsertNode::ToQuery() {
	stringstream insert_str;
	insert_str << "INSERT ";
	switch (action_type) {
	case OnConflictAction::THROW:
		break;
	case OnConflictAction::REPLACE:
	case OnConflictAction::UPDATE:
		insert_str << "OR REPLACE ";
		break;
	case OnConflictAction::NOTHING:
		insert_str << "OR IGNORE ";
		break;
	default:
		throw NotImplementedException("OnConflictAction::%s is not (yet) supported", EnumUtil::ToString(action_type));
	}
	insert_str << "INTO ";
	insert_str << target_table;
	insert_str << " SELECT * FROM ";
	insert_str << child_cte_name;
	return insert_str.str();
}

string CteNode::ToCteQuery() {
	std::ostringstream cte_str;
	cte_str << cte_name;
	if (!cte_column_list.empty()) {
		cte_str << " (";
		cte_str << VecToSeparatedList(cte_column_list);
		cte_str << ")";
	}
	cte_str << " AS (";
	cte_str << this->ToQuery();
	cte_str << ")";
	return cte_str.str();
}

string GetNode::ToQuery() {
	std::ostringstream get_str;
	get_str << "SELECT ";
	if (column_names.empty()) {
		get_str << "*";
	} else {
		get_str << VecToSeparatedList(column_names);
	}
	get_str << " FROM ";
	get_str << catalog;
	get_str << ".";
	get_str << schema;
	get_str << ".";
	get_str << table_name;
	if (!table_filters.empty()) {
		get_str << " WHERE ";
		get_str << VecToSeparatedList(table_filters);
	}
	return get_str.str();
}

string FilterNode::ToQuery() {
	std::ostringstream get_str;
	get_str << "SELECT * FROM ";
	get_str << child_cte_name;
	if (!conditions.empty()) {
		get_str << " WHERE ";
		get_str << VecToSeparatedList(conditions);
	}
	return get_str.str();
}

string ProjectNode::ToQuery() {
	std::ostringstream project_str;
	project_str << "SELECT ";
	if (column_names.empty()) {
		project_str << "*";
	} else {
		project_str << VecToSeparatedList(column_names);
	}
	project_str << " FROM ";
	project_str << child_cte_name;
	return project_str.str();
}

string AggregateNode::ToQuery() {
	std::ostringstream aggregate_str;
	aggregate_str << "SELECT ";
	if (!group_by_columns.empty()) {
		aggregate_str << VecToSeparatedList(group_by_columns);
		aggregate_str << ", ";
	}
	aggregate_str << VecToSeparatedList(aggregate_expressions);
	aggregate_str << " FROM ";
	aggregate_str << child_cte_name;
	if (!group_by_columns.empty()) {
		aggregate_str << " GROUP BY ";
		aggregate_str << VecToSeparatedList(group_by_columns);
	}
	return aggregate_str.str();
}

string JoinNode::ToQuery() {
	std::ostringstream join_str;
	join_str << "SELECT * FROM ";
	join_str << left_cte_name;
	join_str << " ";
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		join_str << EnumUtil::ToString(join_type);
		break;
	default:
		throw NotImplementedException("JoinType::%s is not (yet) supported", EnumUtil::ToString(join_type));
	}
	join_str << " JOIN ";
	join_str << right_cte_name;
	join_str << " ON ";
	join_str << VecToSeparatedList(join_conditions, " AND ");
	return join_str.str();
}

string UnionNode::ToQuery() {
	std::ostringstream union_str;
	union_str << "SELECT * FROM ";
	union_str << left_cte_name;
	if (is_union_all) {
		union_str << " UNION ALL ";
	} else {
		union_str << " UNION ";
	}
	union_str << "SELECT * FROM ";
	union_str << right_cte_name;
	return union_str.str();
}

string IRStruct::ToQuery(const bool use_newlines) {
	std::ostringstream sql_str;
	if (!nodes.empty()) {
		sql_str << "WITH ";
		for (size_t i = 0; i < nodes.size(); ++i) {
			sql_str << nodes[i]->ToCteQuery();
			if (i != nodes.size() - 1) {
				sql_str << ", ";
			} else if (!use_newlines) {
				sql_str << " ";
			}
			if (use_newlines) {
				sql_str << "\n";
			}
		}
	}
	sql_str << final_node->ToQuery();
	sql_str << ";";
	return sql_str.str();
}

string LogicalPlanToSql::ColStruct::ToUniqueColumnName() const {
	return "t" + std::to_string(table_index) + "_" + (alias.empty() ? column_name : alias);
}

string LogicalPlanToSql::ExpressionToAliasedString(const unique_ptr<Expression> &expression) const {
	const ExpressionClass e_class = expression->GetExpressionClass();
	std::ostringstream expr_str;
	switch (e_class) {
	case (ExpressionClass::BOUND_COLUMN_REF): {
		const BoundColumnRefExpression &bcr = expression->Cast<BoundColumnRefExpression>();
		const unique_ptr<ColStruct> &col_struct = column_map.at(bcr.binding);
		expr_str << col_struct->ToUniqueColumnName();
		break;
	}
	case (ExpressionClass::BOUND_CONSTANT): {
		expr_str << expression->ToString();
		break;
	}
	case (ExpressionClass::BOUND_COMPARISON): {
		const BoundComparisonExpression &expr_cast = expression->Cast<BoundComparisonExpression>();
		expr_str << "(";
		expr_str << ExpressionToAliasedString(expr_cast.left);
		expr_str << ") ";
		expr_str << ExpressionTypeToOperator(expr_cast.GetExpressionType());
		expr_str << " (";
		expr_str << ExpressionToAliasedString(expr_cast.right);
		expr_str << ")";
		break;
	}
	case (ExpressionClass::BOUND_CAST): {
		const BoundCastExpression &expr_cast = expression->Cast<BoundCastExpression>();
		expr_str << (expr_cast.try_cast ? "TRY_CAST(" : "CAST(");
		expr_str << ExpressionToAliasedString(expr_cast.child);
		expr_str << " AS " + expr_cast.return_type.ToString() + ")";
		break;
	}
	case (ExpressionClass::BOUND_CONJUNCTION): {
		const BoundConjunctionExpression &conjunction_expr = expression->Cast<BoundConjunctionExpression>();
		expr_str << "(";
		expr_str << ExpressionToAliasedString(conjunction_expr.children[0]);
		expr_str << ") ";
		expr_str << ExpressionTypeToOperator(conjunction_expr.GetExpressionType());
		expr_str << " (";
		expr_str << ExpressionToAliasedString(conjunction_expr.children[1]);
		expr_str << ")";
		break;
	}
	default: {
		throw NotImplementedException("Unsupported expression for ExpressionToAliasedString: %s",
		                              ExpressionTypeToString(expression->type));
	}
	}
	return expr_str.str();
}

unique_ptr<CteNode> LogicalPlanToSql::CreateCteNode(unique_ptr<LogicalOperator> &subplan,
                                                    const vector<size_t> &children_indices) {
	const size_t my_index = node_count++;

	LPTS_DEBUG_PRINT("[LPTS] CreateCteNode: type=" + LogicalOperatorToString(subplan->type) + " my_index=" +
	                 std::to_string(my_index) + " children_indices.size()=" + std::to_string(children_indices.size()) +
	                 " subplan->children.size()=" + std::to_string(subplan->children.size()));
	for (size_t i = 0; i < children_indices.size(); ++i) {
		LPTS_DEBUG_PRINT("[LPTS]   children_indices[" + std::to_string(i) + "]=" + std::to_string(children_indices[i]));
	}
	LPTS_DEBUG_PRINT("[LPTS]   cte_nodes.size()=" + std::to_string(cte_nodes.size()));

	switch (subplan->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		const LogicalGet &plan_as_get = subplan->Cast<LogicalGet>();
		LPTS_DEBUG_PRINT("[LPTS] GET: table_index=" + std::to_string(plan_as_get.table_index) +
		                 " names.size()=" + std::to_string(plan_as_get.names.size()));
		auto catalog_entry = plan_as_get.GetTable();
		LPTS_DEBUG_PRINT("[LPTS] GET: catalog_entry=" + string(catalog_entry ? "valid" : "null"));
		size_t table_index = plan_as_get.table_index;
		string table_name = catalog_entry.get()->name;
		string catalog_name = plan_as_get.GetTable()->schema.ParentCatalog().GetName();
		string schema_name = catalog_entry->schema.name;
		LPTS_DEBUG_PRINT("[LPTS] GET: " + catalog_name + "." + schema_name + "." + table_name);
		vector<string> column_names;
		vector<string> filters;

		vector<string> cte_column_names;
		const vector<ColumnBinding> col_binds = subplan->GetColumnBindings();
		const auto col_ids = plan_as_get.GetColumnIds();
		LPTS_DEBUG_PRINT("[LPTS] GET: col_binds.size()=" + std::to_string(col_binds.size()) +
		                 " col_ids.size()=" + std::to_string(col_ids.size()));
		for (size_t di = 0; di < col_ids.size(); ++di) {
			LPTS_DEBUG_PRINT("[LPTS]   col_ids[" + std::to_string(di) +
			                 "].GetPrimaryIndex()=" + std::to_string(col_ids[di].GetPrimaryIndex()));
		}
		for (size_t i = 0; i < col_binds.size(); ++i) {
			if (i >= col_ids.size()) {
				// No corresponding column ID (e.g. ROWID-only scan for count(*)).
				// Register a dummy entry in column_map so ancestors can still resolve.
				LPTS_DEBUG_PRINT("[LPTS]   col_bind i=" + std::to_string(i) +
				                 " has no col_id (ROWID-only scan), skipping");
				const ColumnBinding &cb = col_binds[i];
				auto col_struct = make_uniq<ColStruct>(table_index, "rowid", "");
				column_map[cb] = std::move(col_struct);
				continue;
			}
			const idx_t idx = col_ids[i].GetPrimaryIndex();
			LPTS_DEBUG_PRINT("[LPTS]   processing col_bind i=" + std::to_string(i) + " idx=" + std::to_string(idx) +
			                 " names.size()=" + std::to_string(plan_as_get.names.size()));
			string column_name = plan_as_get.names[idx];
			const ColumnBinding &cb = col_binds[i];
			column_names.push_back(column_name);
			auto col_struct = make_uniq<ColStruct>(table_index, column_name, "");
			cte_column_names.push_back(col_struct->ToUniqueColumnName());
			column_map[cb] = std::move(col_struct);
		}
		if (!plan_as_get.table_filters.filters.empty()) {
			for (auto &filter : plan_as_get.table_filters.filters) {
				filters.push_back(filter.second->ToString(plan_as_get.names[filter.first]));
			}
		}
		return make_uniq<GetNode>(my_index, std::move(cte_column_names), std::move(catalog_name),
		                          std::move(schema_name), std::move(table_name), table_index, std::move(filters),
		                          std::move(column_names));
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		const LogicalFilter &plan_as_filter = subplan->Cast<LogicalFilter>();
		vector<string> conditions;
		for (const unique_ptr<Expression> &expression : plan_as_filter.expressions) {
			conditions.emplace_back(ExpressionToAliasedString(expression));
		}
		return make_uniq<FilterNode>(my_index, vector<string>(), cte_nodes[children_indices[0]]->cte_name,
		                             std::move(conditions));
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		const LogicalProjection &plan_as_projection = subplan->Cast<LogicalProjection>();
		const size_t table_index = plan_as_projection.table_index;
		vector<string> column_names;
		vector<string> cte_column_names;
		for (size_t i = 0; i < plan_as_projection.expressions.size(); ++i) {
			const unique_ptr<Expression> &expression = plan_as_projection.expressions[i];
			const ColumnBinding new_cb = ColumnBinding(table_index, i);
			if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
				BoundColumnRefExpression &bcr = expression->Cast<BoundColumnRefExpression>();
				unique_ptr<ColStruct> &descendant_col_struct = column_map.at(bcr.binding);
				column_names.push_back(descendant_col_struct->ToUniqueColumnName());
				auto new_col_struct =
				    make_uniq<ColStruct>(table_index, descendant_col_struct->column_name, descendant_col_struct->alias);
				cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
				column_map[new_cb] = std::move(new_col_struct);
			} else {
				string expr_str = ExpressionToAliasedString(expression);
				column_names.emplace_back(expr_str);
				string scalar_alias;
				if (expression->HasAlias()) {
					scalar_alias = expression->GetAlias();
				} else {
					scalar_alias = "scalar_" + std::to_string(i);
				}
				column_map[new_cb] = make_uniq<ColStruct>(table_index, expr_str, scalar_alias);
			}
		}
		return make_uniq<ProjectNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                              std::move(column_names), table_index);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		const LogicalAggregate &plan_as_aggregate = subplan->Cast<LogicalAggregate>();
		LPTS_DEBUG_PRINT("[LPTS] AGGREGATE: group_index=" + std::to_string(plan_as_aggregate.group_index) +
		                 " aggregate_index=" + std::to_string(plan_as_aggregate.aggregate_index) +
		                 " groups.size()=" + std::to_string(plan_as_aggregate.groups.size()) +
		                 " expressions.size()=" + std::to_string(plan_as_aggregate.expressions.size()) +
		                 " children_indices.size()=" + std::to_string(children_indices.size()));
		for (size_t di = 0; di < children_indices.size(); ++di) {
			LPTS_DEBUG_PRINT("[LPTS]   children_indices[" + std::to_string(di) +
			                 "]=" + std::to_string(children_indices[di]) +
			                 " cte_name=" + cte_nodes[children_indices[di]]->cte_name);
		}
		auto col_bindings = subplan->GetColumnBindings();
		LPTS_DEBUG_PRINT("[LPTS]   column_bindings.size()=" + std::to_string(col_bindings.size()));
		for (size_t di = 0; di < col_bindings.size(); ++di) {
			LPTS_DEBUG_PRINT("[LPTS]   binding[" + std::to_string(di) + "]=(" +
			                 std::to_string(col_bindings[di].table_index) + "," +
			                 std::to_string(col_bindings[di].column_index) + ")");
		}
		vector<string> cte_column_names;
		vector<string> group_names;
		{
			const idx_t group_table_index = plan_as_aggregate.group_index;
			const auto &groups = plan_as_aggregate.groups;
			for (size_t i = 0; i < groups.size(); ++i) {
				const unique_ptr<Expression> &col_as_expression = groups[i];
				if (col_as_expression->type == ExpressionType::BOUND_COLUMN_REF) {
					BoundColumnRefExpression &bcr = col_as_expression->Cast<BoundColumnRefExpression>();
					unique_ptr<ColStruct> &descendant_col_struct = column_map.at(bcr.binding);
					group_names.emplace_back(descendant_col_struct->ToUniqueColumnName());
					auto new_col_struct = make_uniq<ColStruct>(group_table_index, descendant_col_struct->column_name,
					                                           descendant_col_struct->alias);
					cte_column_names.emplace_back(new_col_struct->ToUniqueColumnName());
					column_map[ColumnBinding(group_table_index, i)] = std::move(new_col_struct);
				} else {
					throw NotImplementedException("Only supporting BoundColumnRef for now.");
				}
			}
		}
		vector<string> aggregate_names;
		{
			const idx_t aggregate_table_idx = plan_as_aggregate.aggregate_index;
			const auto &agg_expressions = plan_as_aggregate.expressions;
			unique_ptr<ColStruct> agg_col_struct;
			for (size_t i = 0; i < agg_expressions.size(); ++i) {
				const unique_ptr<Expression> &expr = agg_expressions[i];
				if (expr->type == ExpressionType::BOUND_AGGREGATE) {
					BoundAggregateExpression &bound_agg = expr->Cast<BoundAggregateExpression>();
					std::ostringstream agg_str;
					agg_str << bound_agg.function.name;
					agg_str << "(";
					if (bound_agg.IsDistinct()) {
						agg_str << "DISTINCT ";
					}
					vector<string> child_expressions;
					for (const unique_ptr<Expression> &agg_child : bound_agg.children) {
						string expr_str = ExpressionToAliasedString(agg_child);
						child_expressions.emplace_back(std::move(expr_str));
					}
					agg_str << VecToSeparatedList(child_expressions);
					agg_str << ")";
					string agg_alias = "aggregate_" + std::to_string(i);
					agg_col_struct = make_uniq<ColStruct>(aggregate_table_idx, agg_str.str(), std::move(agg_alias));
				} else {
					throw NotImplementedException("Only supporting BoundAggregateExpression for now.");
				}
				aggregate_names.emplace_back(agg_col_struct->column_name);
				cte_column_names.emplace_back(agg_col_struct->ToUniqueColumnName());
				column_map[ColumnBinding(aggregate_table_idx, i)] = std::move(agg_col_struct);
			}
		}
		return make_uniq<AggregateNode>(my_index, cte_column_names, cte_nodes[children_indices[0]]->cte_name,
		                                std::move(group_names), std::move(aggregate_names));
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		const LogicalSetOperation &plan_as_union = subplan->Cast<LogicalSetOperation>();
		const size_t table_index = plan_as_union.table_index;
		vector<string> cte_column_names;
		const auto &lhs_bindings = subplan->children[0]->GetColumnBindings();
		const auto &union_bindings = subplan->GetColumnBindings();
		if (lhs_bindings.size() != union_bindings.size()) {
			throw std::runtime_error("Size mismatch between column bindings!");
		}
		for (size_t i = 0; i < lhs_bindings.size(); ++i) {
			const unique_ptr<ColStruct> &lhs_col_struct = column_map.at(lhs_bindings[i]);
			unique_ptr<ColStruct> new_col_struct =
			    make_uniq<ColStruct>(table_index, lhs_col_struct->column_name, lhs_col_struct->alias);
			cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
			column_map[union_bindings[i]] = std::move(new_col_struct);
		}
		return make_uniq<UnionNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                            cte_nodes[children_indices[1]]->cte_name, plan_as_union.setop_all);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		const LogicalComparisonJoin &plan_as_join = subplan->Cast<LogicalComparisonJoin>();
		vector<string> join_conditions;
		string left_cte_name = cte_nodes[children_indices[0]]->cte_name;
		string right_cte_name = cte_nodes[children_indices[1]]->cte_name;
		for (auto &cond : plan_as_join.conditions) {
			string condition_lhs = ExpressionToAliasedString(cond.left);
			string condition_rhs = ExpressionToAliasedString(cond.right);
			string comparison = ExpressionTypeToOperator(cond.comparison);
			join_conditions.emplace_back("(" + condition_lhs + " " + comparison + " " + condition_rhs + ")");
		}
		vector<string> cte_column_names;
		for (const ColumnBinding &binding : subplan->GetColumnBindings()) {
			unique_ptr<ColStruct> &col_struct = column_map.at(binding);
			cte_column_names.push_back(col_struct->ToUniqueColumnName());
		}
		return make_uniq<JoinNode>(my_index, std::move(cte_column_names), std::move(left_cte_name),
		                           std::move(right_cte_name), plan_as_join.join_type, std::move(join_conditions));
	}
	default: {
		throw NotImplementedException("This logical operator is not implemented: %s.",
		                              LogicalOperatorToString(subplan->type));
	}
	}
}

unique_ptr<CteNode> LogicalPlanToSql::RecursiveTraversal(unique_ptr<LogicalOperator> &sub_plan) {
	LPTS_DEBUG_PRINT("[LPTS] RecursiveTraversal: type=" + LogicalOperatorToString(sub_plan->type) +
	                 " num_children=" + std::to_string(sub_plan->children.size()));
	vector<size_t> children_indices;
	for (auto &child : sub_plan->children) {
		unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
		children_indices.push_back(child_as_node->idx);
		cte_nodes.emplace_back(std::move(child_as_node));
	}
	unique_ptr<CteNode> to_return = CreateCteNode(sub_plan, children_indices);
	return to_return;
}

unique_ptr<IRStruct> LogicalPlanToSql::LogicalPlanToIR() {
	if (node_count != 0) {
		throw std::runtime_error("This function can only be called once.");
	}
	LPTS_DEBUG_PRINT("[LPTS] LogicalPlanToIR: root type=" + LogicalOperatorToString(plan->type) +
	                 " num_children=" + std::to_string(plan->children.size()));
	vector<size_t> children_indices;
	for (auto &child : plan->children) {
		unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
		children_indices.push_back(child_as_node->idx);
		cte_nodes.emplace_back(std::move(child_as_node));
	}
	LPTS_DEBUG_PRINT("[LPTS] LogicalPlanToIR: after traversal, cte_nodes.size()=" + std::to_string(cte_nodes.size()) +
	                 " children_indices.size()=" + std::to_string(children_indices.size()));
	unique_ptr<IRStruct> to_return;
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_INSERT: {
		const LogicalInsert &insert_ref = plan->Cast<LogicalInsert>();
		unique_ptr<InsertNode> insert_node =
		    make_uniq<InsertNode>(node_count++, insert_ref.table.name, cte_nodes[children_indices[0]]->cte_name,
		                          insert_ref.on_conflict_info.action_type);
		to_return = make_uniq<IRStruct>(std::move(cte_nodes), std::move(insert_node));
		break;
	}
	case LogicalOperatorType::LOGICAL_DELETE: {
		throw std::runtime_error("Not yet implemented.");
	}
	case LogicalOperatorType::LOGICAL_UPDATE: {
		throw std::runtime_error("Not yet implemented.");
	}
	default: {
		unique_ptr<CteNode> last_cte = CreateCteNode(plan, children_indices);
		vector<string> final_column_list;
		for (const auto &cb : plan->GetColumnBindings()) {
			const unique_ptr<ColStruct> &col_struct = column_map.at(cb);
			final_column_list.emplace_back(col_struct->alias.empty() ? col_struct->column_name : col_struct->alias);
		}
		unique_ptr<FinalReadNode> final_node = make_uniq<FinalReadNode>(
		    node_count++, last_cte->cte_name, last_cte->cte_column_list, std::move(final_column_list));
		cte_nodes.emplace_back(std::move(last_cte));
		to_return = make_uniq<IRStruct>(std::move(cte_nodes), std::move(final_node));
	}
	}
	return to_return;
}

string LogicalPlanToSql::IRToSql(unique_ptr<IRStruct> &ir_struct) {
	return ir_struct->ToQuery(false);
}

} // namespace duckdb
