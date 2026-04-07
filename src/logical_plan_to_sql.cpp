//==============================================================================
// logical_plan_to_sql.cpp
//
// Converts DuckDB's LogicalOperator tree into a flat CTE-based SQL string.
//
// The main entry point is LogicalPlanToSql::LogicalPlanToCteList(), which:
//   1. Walks the logical plan tree bottom-up (leaves first).
//   2. For each operator (scan, filter, projection, ...), creates a CteNode
//      and registers its output columns in `column_map`.
//   3. Parent operators look up child columns through `column_map` to produce
//      correct CTE column references.
//   4. The result is a CteList (ordered list of CTEs + a final SELECT/INSERT)
//      that can be serialized to a SQL string via ToQuery().
//==============================================================================

#include "logical_plan_to_sql.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"

#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_set.hpp"
#include "duckdb/planner/planner.hpp"

#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/lambda_functions.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// ToQuery() implementations for each IR node type.
// Each returns the SQL fragment for its CTE body (the part inside AS (...)).
//------------------------------------------------------------------------------

/// FinalReadNode: the closing SELECT that renames CTE columns back to their
/// original names (e.g. "SELECT t2_name AS name FROM projection_2").
string FinalReadNode::ToQuery() {
	const size_t col_count = final_column_list.size();
	if (child_cte_column_list.size() != col_count) {
		throw InternalException("LPTS: Size mismatch between column lists");
	}
	vector<string> merged_list;
	// Assign the final column names to the CTE column names. Format: "cte_col AS final_col".
	merged_list.reserve(col_count);
	for (size_t i = 0; i < final_column_list.size(); ++i) {
		// Quote final column names that contain special characters (e.g. "sum(salary)")
		string final_name = final_column_list[i];
		if (final_name.find_first_of("()*, ") != string::npos) {
			final_name = "\"" + final_name + "\"";
		}
		merged_list.emplace_back(child_cte_column_list[i] + " AS " + final_name);
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
	if (!catalog.empty()) {
		// Fully-qualified: catalog.schema.table (DuckDB dialect)
		get_str << catalog << "." << schema << "." << table_name;
	} else {
		// Unqualified: table function or simple table name
		get_str << table_name;
		// For table functions (name contains parentheses), add column aliases
		// so that renamed columns (e.g. range(1,10) t(i)) resolve correctly.
		if (table_name.find('(') != string::npos && !column_names.empty()) {
			get_str << " _tf(" << VecToSeparatedList(column_names) << ")";
		}
	}
	if (!table_filters.empty()) {
		get_str << " WHERE ";
		get_str << VecToSeparatedList(table_filters, " AND ");
	}
	return get_str.str();
}

string FilterNode::ToQuery() {
	std::ostringstream get_str;
	get_str << "SELECT * FROM ";
	get_str << child_cte_name;
	if (!conditions.empty()) {
		get_str << " WHERE ";
		get_str << VecToSeparatedList(conditions, " AND ");
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
	// Use explicit column list instead of SELECT * to avoid including
	// duplicate join key columns from both sides of the join.
	join_str << "SELECT " << VecToSeparatedList(cte_column_list) << " FROM ";
	join_str << left_cte_name;
	join_str << " ";
	switch (join_type) {
	case JoinType::INNER:
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
	case JoinType::SEMI:
	case JoinType::ANTI:
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

string ExceptNode::ToQuery() {
	std::ostringstream except_str;
	except_str << "SELECT * FROM ";
	except_str << left_cte_name;
	if (is_except_all) {
		except_str << " EXCEPT ALL ";
	} else {
		except_str << " EXCEPT ";
	}
	except_str << "SELECT * FROM ";
	except_str << right_cte_name;
	return except_str.str();
}

string OrderNode::ToQuery() {
	std::ostringstream order_str;
	order_str << "SELECT * FROM " << child_cte_name;
	if (!order_items.empty()) {
		order_str << " ORDER BY " << VecToSeparatedList(order_items);
	}
	return order_str.str();
}

string LimitNode::ToQuery() {
	std::ostringstream limit_str_stream;
	limit_str_stream << "SELECT * FROM " << child_cte_name;
	if (!limit_str.empty()) {
		limit_str_stream << " LIMIT " << limit_str;
	}
	if (!offset_str.empty()) {
		limit_str_stream << " OFFSET " << offset_str;
	}
	return limit_str_stream.str();
}

string DistinctNode::ToQuery() {
	std::ostringstream distinct_str;
	distinct_str << "SELECT DISTINCT * FROM " << child_cte_name;
	return distinct_str.str();
}

/// Serialize the entire CTE list into a SQL string.
/// Output format: WITH cte_0(...) AS (...), cte_1(...) AS (...), ... SELECT ...;
string CteList::ToQuery(const bool use_newlines) {
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
	string base = alias.empty() ? column_name : alias;
	// Sanitize: replace characters that would break SQL identifiers
	string result = "t" + std::to_string(table_index) + "_";
	for (char c : base) {
		if (c == '(' || c == ')' || c == ' ' || c == ',' || c == '*') {
			result += '_';
		} else {
			result += c;
		}
	}
	return result;
}

//------------------------------------------------------------------------------
// CollectLambdaParamNames: walk a lambda body to find BoundReferenceExpression
// nodes (which are the post-binding form of lambda parameters).
// After binding, lambda params become BoundReferenceExpression with alias = param name
// and index reversed (index 0 = last param, parameter_count-1 = first param).
// Stops at nested lambda functions (they have their own params in bind_info).
//------------------------------------------------------------------------------
static void CollectLambdaParamNames(const Expression &expr, std::map<idx_t, string> &names) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		if (names.find(ref.index) == names.end()) {
			names[ref.index] = ref.alias.empty() ? ("p" + to_string(ref.index)) : ref.alias;
		}
		return;
	}
	// For nested lambda functions, only recurse into children (not bind_info)
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if (func.function.bind_lambda != nullptr) {
			for (auto &child : func.children) {
				CollectLambdaParamNames(*child, names);
			}
			return;
		}
	}
	ExpressionIterator::EnumerateChildren(const_cast<Expression &>(expr),
	                                      [&](Expression &child) { CollectLambdaParamNames(child, names); });
}

//------------------------------------------------------------------------------
// ExpressionToAliasedString: convert a bound expression tree into a SQL string.
// Column references are replaced with their CTE column names via column_map.
//------------------------------------------------------------------------------
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
	case (ExpressionClass::BOUND_FUNCTION): {
		const BoundFunctionExpression &func_expr = expression->Cast<BoundFunctionExpression>();
		// DuckDB's optimizer may rewrite functions to internal variants.
		// Map them back to their public SQL equivalents.
		if (func_expr.function.name.rfind("__internal_", 0) == 0 && !func_expr.children.empty()) {
			// Internal compress/decompress wrappers — just emit the child
			expr_str << ExpressionToAliasedString(func_expr.children[0]);
			break;
		}
		string func_name = func_expr.function.name;
		if (func_name == "sum_no_overflow") {
			func_name = "sum";
		}
		// Arithmetic operators: render as infix (arg1 op arg2) instead of func(arg1, arg2)
		if ((func_name == "/" || func_name == "+" || func_name == "-" || func_name == "*" || func_name == "%" ||
		     func_name == "**") &&
		    func_expr.children.size() == 2) {
			expr_str << "(" << ExpressionToAliasedString(func_expr.children[0]) << " " << func_name << " "
			         << ExpressionToAliasedString(func_expr.children[1]) << ")";
			break;
		}
		// For lambda functions, only serialize non-lambda, non-capture children
		idx_t child_count = func_expr.children.size();
		if (func_expr.function.bind_lambda != nullptr) {
			child_count = 0;
			for (auto &arg_type : func_expr.function.arguments) {
				if (arg_type.id() != LogicalTypeId::LAMBDA) {
					child_count++;
				}
			}
			if (child_count > func_expr.children.size()) {
				child_count = func_expr.children.size();
			}
		}
		// Operators: use infix notation (e.g. "a + b") instead of function call syntax
		if (func_expr.is_operator && child_count == 2) {
			expr_str << "(";
			expr_str << ExpressionToAliasedString(func_expr.children[0]);
			expr_str << " " << func_name << " ";
			expr_str << ExpressionToAliasedString(func_expr.children[1]);
			expr_str << ")";
		} else {
			expr_str << func_name << "(";
			for (idx_t i = 0; i < child_count; i++) {
				if (i > 0) {
					expr_str << ", ";
				}
				expr_str << ExpressionToAliasedString(func_expr.children[i]);
			}
			// Lambda function: serialize the lambda expression from bind_info
			if (func_expr.function.bind_lambda != nullptr && func_expr.bind_info) {
				auto &bind_data = func_expr.bind_info->Cast<ListLambdaBindData>();
				if (bind_data.lambda_expr) {
					if (child_count > 0) {
						expr_str << ", ";
					}
					// Collect parameter names from BoundReferenceExpression nodes in the body
					std::map<idx_t, string> param_map;
					CollectLambdaParamNames(*bind_data.lambda_expr, param_map);
					idx_t param_count = param_map.empty() ? 0 : param_map.rbegin()->first + 1;
					// Build param list (indices are reversed: 0 = last param)
					vector<string> param_names(param_count);
					for (auto &entry : param_map) {
						if (entry.first < param_count) {
							param_names[param_count - 1 - entry.first] = entry.second;
						}
					}
					for (idx_t i = 0; i < param_count; i++) {
						if (param_names[i].empty()) {
							param_names[i] = "p" + to_string(i);
						}
					}
					expr_str << "lambda ";
					for (idx_t i = 0; i < param_count; i++) {
						if (i > 0) {
							expr_str << ", ";
						}
						expr_str << param_names[i];
					}
					expr_str << ": ";
					expr_str << ExpressionToAliasedString(bind_data.lambda_expr);
				}
			}
			expr_str << ")";
		}
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
	case (ExpressionClass::BOUND_REF): {
		// BoundReferenceExpression: used for lambda parameters after binding.
		// ToString() returns the alias (original param name) or "#index".
		expr_str << expression->ToString();
		break;
	}
	case (ExpressionClass::BOUND_LAMBDA_REF): {
		expr_str << expression->ToString();
		break;
	}
	case (ExpressionClass::BOUND_CASE): {
		const BoundCaseExpression &case_expr = expression->Cast<BoundCaseExpression>();
		expr_str << "CASE";
		for (auto &check : case_expr.case_checks) {
			expr_str << " WHEN " << ExpressionToAliasedString(check.when_expr);
			expr_str << " THEN " << ExpressionToAliasedString(check.then_expr);
		}
		if (case_expr.else_expr) {
			expr_str << " ELSE " << ExpressionToAliasedString(case_expr.else_expr);
		}
		expr_str << " END";
		break;
	}
	case (ExpressionClass::BOUND_OPERATOR): {
		const BoundOperatorExpression &op_expr = expression->Cast<BoundOperatorExpression>();
		switch (op_expr.GetExpressionType()) {
		case ExpressionType::OPERATOR_IS_NULL:
			expr_str << "(" << ExpressionToAliasedString(op_expr.children[0]) << ") IS NULL";
			break;
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			expr_str << "(" << ExpressionToAliasedString(op_expr.children[0]) << ") IS NOT NULL";
			break;
		case ExpressionType::OPERATOR_NOT:
			expr_str << "NOT (" << ExpressionToAliasedString(op_expr.children[0]) << ")";
			break;
		case ExpressionType::COMPARE_IN:
		case ExpressionType::COMPARE_NOT_IN: {
			const bool is_not_in = op_expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN;
			expr_str << "(" << ExpressionToAliasedString(op_expr.children[0]) << ")";
			expr_str << (is_not_in ? " NOT IN (" : " IN (");
			for (idx_t i = 1; i < op_expr.children.size(); i++) {
				if (i > 1) {
					expr_str << ", ";
				}
				expr_str << ExpressionToAliasedString(op_expr.children[i]);
			}
			expr_str << ")";
			break;
		}
		case ExpressionType::OPERATOR_COALESCE: {
			expr_str << "COALESCE(";
			for (idx_t i = 0; i < op_expr.children.size(); i++) {
				if (i > 0) {
					expr_str << ", ";
				}
				expr_str << ExpressionToAliasedString(op_expr.children[i]);
			}
			expr_str << ")";
			break;
		}
		case ExpressionType::OPERATOR_NULLIF:
			expr_str << "NULLIF(" << ExpressionToAliasedString(op_expr.children[0]) << ", "
			         << ExpressionToAliasedString(op_expr.children[1]) << ")";
			break;
		case ExpressionType::OPERATOR_TRY:
			expr_str << "TRY(" << ExpressionToAliasedString(op_expr.children[0]) << ")";
			break;
		default:
			throw NotImplementedException("Unsupported BOUND_OPERATOR subtype for ExpressionToAliasedString: %s",
			                              ExpressionTypeToString(op_expr.GetExpressionType()));
		}
		break;
	}
	default: {
		throw NotImplementedException("Unsupported expression for ExpressionToAliasedString: %s",
		                              ExpressionTypeToString(expression->type));
	}
	}
	return expr_str.str();
}

//------------------------------------------------------------------------------
// CreateCteNode: convert one LogicalOperator into the corresponding CteNode.
//
// For each operator type, we:
//   1. Extract relevant info (table name, expressions, join conditions, etc.)
//   2. Register output columns in column_map so parent operators can find them
//   3. Return the CteNode with its SQL fragment
//
// The children_indices parameter tells us which entries in cte_nodes[] correspond
// to the children of this operator (already processed, since we go bottom-up).
//------------------------------------------------------------------------------
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
		// GET = table scan. Reads columns from a physical table or table function.
		// We register each output column in column_map so that parent operators
		// (filter, projection, etc.) can resolve column references.
		const LogicalGet &plan_as_get = subplan->Cast<LogicalGet>();
		LPTS_DEBUG_PRINT("[LPTS] GET: table_index=" + std::to_string(plan_as_get.table_index) +
		                 " names.size()=" + std::to_string(plan_as_get.names.size()));
		auto catalog_entry = plan_as_get.GetTable();
		LPTS_DEBUG_PRINT("[LPTS] GET: catalog_entry=" + string(catalog_entry ? "valid" : "null"));
		size_t table_index = plan_as_get.table_index;
		string table_name;
		string catalog_name;
		string schema_name;
		if (catalog_entry) {
			table_name = catalog_entry.get()->name;
			catalog_name = plan_as_get.GetTable()->schema.ParentCatalog().GetName();
			schema_name = catalog_entry->schema.name;
		} else {
			// Table function (e.g. range(), read_csv(), generate_series())
			std::ostringstream func_str;
			func_str << plan_as_get.function.name << "(";
			for (size_t i = 0; i < plan_as_get.parameters.size(); i++) {
				if (i > 0) {
					func_str << ", ";
				}
				func_str << plan_as_get.parameters[i].ToSQLString();
			}
			func_str << ")";
			table_name = func_str.str();
		}
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
		// PROJECTION = column selection and computed expressions.
		const LogicalProjection &plan_as_projection = subplan->Cast<LogicalProjection>();
		const size_t table_index = plan_as_projection.table_index;

		vector<string> column_names;
		vector<string> cte_column_names;
		unordered_set<string> seen_names;
		for (size_t i = 0; i < plan_as_projection.expressions.size(); ++i) {
			const unique_ptr<Expression> &expression = plan_as_projection.expressions[i];
			const ColumnBinding new_cb = ColumnBinding(table_index, i);
			if (expression->type == ExpressionType::BOUND_COLUMN_REF) {
				BoundColumnRefExpression &bcr = expression->Cast<BoundColumnRefExpression>();
				unique_ptr<ColStruct> &descendant_col_struct = column_map.at(bcr.binding);
				column_names.push_back(descendant_col_struct->ToUniqueColumnName());
			string col_name = descendant_col_struct->column_name;
				string alias = descendant_col_struct->alias;
				// Deduplicate: projections joining multiple tables can have same-named columns.
				// Build unique CTE column name; append _N suffix on collision.
				string base = alias.empty() ? col_name : alias;
				string deduped = base;
				string unique_name = "t" + to_string(table_index) + "_" + deduped;
				for (size_t suffix = i; seen_names.count(unique_name); suffix++) {
					deduped = base + "_" + to_string(suffix);
					unique_name = "t" + to_string(table_index) + "_" + deduped;
				}
				seen_names.insert(unique_name);
				// Store deduped name back into col_name or alias for the ColStruct
				if (alias.empty()) {
					col_name = deduped;
				} else {
					alias = deduped;
				}
				auto new_col_struct = make_uniq<ColStruct>(table_index, col_name, alias);
				cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
				column_map[new_cb] = std::move(new_col_struct);
			} else {
				string expr_str = ExpressionToAliasedString(expression);
				column_names.emplace_back(expr_str);
				string scalar_alias = "scalar_" + std::to_string(i);
				string unique_name = "t" + to_string(table_index) + "_" + scalar_alias;
				for (size_t suffix = i; seen_names.count(unique_name); suffix++) {
					scalar_alias = "scalar_" + to_string(i) + "_" + to_string(suffix);
					unique_name = "t" + to_string(table_index) + "_" + scalar_alias;
				}
				seen_names.insert(unique_name);
				auto new_col_struct = make_uniq<ColStruct>(table_index, expr_str, scalar_alias);
				cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
				column_map[new_cb] = std::move(new_col_struct);
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
					string agg_name = bound_agg.function.name;
					if (agg_name == "sum_no_overflow") {
						agg_name = "sum";
					}
					agg_str << agg_name;
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
					string agg_alias = expr->alias.empty() ? ("aggregate_" + std::to_string(i)) : expr->alias;
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
		const auto &union_bindings = subplan->GetColumnBindings();
		// Derive names from the left child's finalized CTE column list, NOT from
		// column_map lookups. The right child's bottom-up processing may overwrite
		// left-child column_map entries when subtrees share table indices
		// (e.g., CTE inlining, plan copies, optimizer rewrites).
		const auto &lhs_cte_cols = cte_nodes[children_indices[0]]->cte_column_list;
		if (lhs_cte_cols.size() != union_bindings.size()) {
			throw InternalException("LPTS: Size mismatch between union column bindings (%zu) and lhs CTE columns (%zu)",
			                        union_bindings.size(), lhs_cte_cols.size());
		}
		unordered_set<string> seen_names;
		for (size_t i = 0; i < lhs_cte_cols.size(); ++i) {
			// Extract base name from CTE column name by stripping the "tN_" prefix.
			// CTE names follow the format "t<digits>_<base>". We find the underscore
			// that separates the table index from the column name.
			string full_name = lhs_cte_cols[i];
			string col_name = full_name;
			if (full_name.size() > 1 && full_name[0] == 't' && isdigit(full_name[1])) {
				size_t sep = 1;
				while (sep < full_name.size() && isdigit(full_name[sep])) {
					sep++;
				}
				if (sep < full_name.size() && full_name[sep] == '_') {
					col_name = full_name.substr(sep + 1);
				}
			}
			// Deduplicate column names to avoid ambiguous references in generated SQL
			string base = col_name;
			string unique_name = "t" + to_string(table_index) + "_" + col_name;
			for (size_t suffix = i; seen_names.count(unique_name); suffix++) {
				col_name = base + "_" + to_string(suffix);
				unique_name = "t" + to_string(table_index) + "_" + col_name;
			}
			seen_names.insert(unique_name);
			unique_ptr<ColStruct> new_col_struct = make_uniq<ColStruct>(table_index, col_name, "");
			cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
			column_map[union_bindings[i]] = std::move(new_col_struct);
		}
		return make_uniq<UnionNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                            cte_nodes[children_indices[1]]->cte_name, plan_as_union.setop_all);
	}
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		const LogicalSetOperation &plan_as_except = subplan->Cast<LogicalSetOperation>();
		const size_t table_index = plan_as_except.table_index;
		vector<string> cte_column_names;
		const auto &lhs_bindings = subplan->children[0]->GetColumnBindings();
		const auto &except_bindings = subplan->GetColumnBindings();
		if (lhs_bindings.size() != except_bindings.size()) {
			throw InternalException("LPTS: Size mismatch between column bindings");
		}
		for (size_t i = 0; i < lhs_bindings.size(); ++i) {
			const unique_ptr<ColStruct> &lhs_col_struct = column_map.at(lhs_bindings[i]);
			unique_ptr<ColStruct> new_col_struct =
			    make_uniq<ColStruct>(table_index, lhs_col_struct->column_name, lhs_col_struct->alias);
			cte_column_names.push_back(new_col_struct->ToUniqueColumnName());
			column_map[except_bindings[i]] = std::move(new_col_struct);
		}
		return make_uniq<ExceptNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                             cte_nodes[children_indices[1]]->cte_name, plan_as_except.setop_all);
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
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// Cross product = unconditional join. Rendered as INNER JOIN ON (TRUE).
		string left_cte_name = cte_nodes[children_indices[0]]->cte_name;
		string right_cte_name = cte_nodes[children_indices[1]]->cte_name;
		vector<string> cte_column_names;
		for (const ColumnBinding &binding : subplan->GetColumnBindings()) {
			unique_ptr<ColStruct> &col_struct = column_map.at(binding);
			cte_column_names.push_back(col_struct->ToUniqueColumnName());
		}
		vector<string> cross_condition = {"(TRUE)"};
		return make_uniq<JoinNode>(my_index, std::move(cte_column_names), std::move(left_cte_name),
		                           std::move(right_cte_name), JoinType::INNER, std::move(cross_condition));
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		const LogicalOrder &order_op = subplan->Cast<LogicalOrder>();
		vector<string> order_items;
		for (const BoundOrderByNode &order : order_op.orders) {
			string col_str = ExpressionToAliasedString(order.expression);
			switch (order.type) {
			case OrderType::DESCENDING:
				col_str += " DESC";
				break;
			case OrderType::ASCENDING:
				col_str += " ASC";
				break;
			default:
				break; // ORDER_DEFAULT: no explicit keyword
			}
			order_items.push_back(std::move(col_str));
		}
		vector<string> cte_column_names;
		for (const ColumnBinding &cb : subplan->GetColumnBindings()) {
			cte_column_names.push_back(column_map.at(cb)->ToUniqueColumnName());
		}
		return make_uniq<OrderNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                            std::move(order_items));
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		const LogicalLimit &limit_op = subplan->Cast<LogicalLimit>();
		string limit_str;
		if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			limit_str = std::to_string(limit_op.limit_val.GetConstantValue());
		} else if (limit_op.limit_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			limit_str = ExpressionToAliasedString(const_cast<BoundLimitNode &>(limit_op.limit_val).GetExpression());
		} else if (limit_op.limit_val.Type() != LimitNodeType::UNSET) {
			throw NotImplementedException("LPTS: unsupported LIMIT node type");
		}
		string offset_str;
		if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			offset_str = std::to_string(limit_op.offset_val.GetConstantValue());
		} else if (limit_op.offset_val.Type() == LimitNodeType::EXPRESSION_VALUE) {
			offset_str = ExpressionToAliasedString(const_cast<BoundLimitNode &>(limit_op.offset_val).GetExpression());
		} else if (limit_op.offset_val.Type() != LimitNodeType::UNSET) {
			throw NotImplementedException("LPTS: unsupported OFFSET node type");
		}
		vector<string> cte_column_names;
		for (const ColumnBinding &cb : subplan->GetColumnBindings()) {
			cte_column_names.push_back(column_map.at(cb)->ToUniqueColumnName());
		}
		return make_uniq<LimitNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name,
		                            std::move(limit_str), std::move(offset_str));
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		vector<string> cte_column_names;
		for (const ColumnBinding &cb : subplan->GetColumnBindings()) {
			cte_column_names.push_back(column_map.at(cb)->ToUniqueColumnName());
		}
		return make_uniq<DistinctNode>(my_index, std::move(cte_column_names), cte_nodes[children_indices[0]]->cte_name);
	}
	default: {
		throw NotImplementedException("This logical operator is not implemented: %s.",
		                              LogicalOperatorToString(subplan->type));
	}
	}
}

/// Bottom-up traversal: process all children first, then create a CteNode for
/// this operator. This guarantees CTEs are ordered leaf-to-root.
unique_ptr<CteNode> LogicalPlanToSql::RecursiveTraversal(unique_ptr<LogicalOperator> &sub_plan) {
	LPTS_DEBUG_PRINT("[LPTS] RecursiveTraversal: type=" + LogicalOperatorToString(sub_plan->type) +
	                 " num_children=" + std::to_string(sub_plan->children.size()));
	vector<size_t> children_indices;
	if (sub_plan->type == LogicalOperatorType::LOGICAL_UNION && sub_plan->children.size() == 2) {
		// UNION ALL: scope column_map to prevent the right child's bottom-up processing
		// from overwriting left-child entries (subtrees may share table indices via
		// CTE inlining, plan copies, or optimizer rewrites).
		auto left_node = RecursiveTraversal(sub_plan->children[0]);
		children_indices.push_back(left_node->idx);
		cte_nodes.emplace_back(std::move(left_node));
		// Deep-copy column_map before right child, restore after
		std::map<MappableColumnBinding, unique_ptr<ColStruct>> saved_map;
		for (auto &entry : column_map) {
			saved_map[entry.first] =
			    make_uniq<ColStruct>(entry.second->table_index, entry.second->column_name, entry.second->alias);
		}
		auto right_node = RecursiveTraversal(sub_plan->children[1]);
		children_indices.push_back(right_node->idx);
		cte_nodes.emplace_back(std::move(right_node));
		column_map = std::move(saved_map);
	} else {
		for (auto &child : sub_plan->children) {
			unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
			children_indices.push_back(child_as_node->idx);
			cte_nodes.emplace_back(std::move(child_as_node));
		}
	}
	unique_ptr<CteNode> to_return = CreateCteNode(sub_plan, children_indices);
	return to_return;
}

/// Main entry point. Traverses the entire logical plan and builds the CTE list.
/// The root operator is handled specially: INSERT becomes an InsertNode,
/// while SELECT-like queries get a FinalReadNode that maps CTE column names
/// back to the original column names the user expects.
unique_ptr<CteList> LogicalPlanToSql::LogicalPlanToCteList() {
	if (node_count != 0) {
		throw InternalException("LPTS: LogicalPlanToCteList can only be called once");
	}
	LPTS_DEBUG_PRINT("[LPTS] LogicalPlanToCteList: root type=" + LogicalOperatorToString(plan->type) +
	                 " num_children=" + std::to_string(plan->children.size()));
	vector<size_t> children_indices;
	for (auto &child : plan->children) {
		unique_ptr<CteNode> child_as_node = RecursiveTraversal(child);
		children_indices.push_back(child_as_node->idx);
		cte_nodes.emplace_back(std::move(child_as_node));
	}
	LPTS_DEBUG_PRINT(
	    "[LPTS] LogicalPlanToCteList: after traversal, cte_nodes.size()=" + std::to_string(cte_nodes.size()) +
	    " children_indices.size()=" + std::to_string(children_indices.size()));
	unique_ptr<CteList> to_return;
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_INSERT: {
		const LogicalInsert &insert_ref = plan->Cast<LogicalInsert>();
		unique_ptr<InsertNode> insert_node =
		    make_uniq<InsertNode>(node_count++, insert_ref.table.name, cte_nodes[children_indices[0]]->cte_name,
		                          insert_ref.on_conflict_info.action_type);
		to_return = make_uniq<CteList>(std::move(cte_nodes), std::move(insert_node));
		break;
	}
	case LogicalOperatorType::LOGICAL_DELETE: {
		throw NotImplementedException("LPTS: DELETE is not yet supported");
	}
	case LogicalOperatorType::LOGICAL_UPDATE: {
		throw NotImplementedException("LPTS: UPDATE is not yet supported");
	}
	default: {
		unique_ptr<CteNode> last_cte = CreateCteNode(plan, children_indices);
		vector<string> final_column_list;
		auto plan_bindings = plan->GetColumnBindings();
		for (size_t i = 0; i < plan_bindings.size(); i++) {
			if (i < output_names.size() && !output_names[i].empty()) {
				final_column_list.emplace_back(output_names[i]);
			} else {
				const unique_ptr<ColStruct> &col_struct = column_map.at(plan_bindings[i]);
				final_column_list.emplace_back(col_struct->alias.empty() ? col_struct->column_name : col_struct->alias);
			}
		}
		unique_ptr<FinalReadNode> final_node = make_uniq<FinalReadNode>(
		    node_count++, last_cte->cte_name, last_cte->cte_column_list, std::move(final_column_list));
		cte_nodes.emplace_back(std::move(last_cte));
		to_return = make_uniq<CteList>(std::move(cte_nodes), std::move(final_node));
	}
	}
	return to_return;
}

string LogicalPlanToSql::CteListToSql(unique_ptr<CteList> &ir_struct) {
	return ir_struct->ToQuery(false);
}

} // namespace duckdb
