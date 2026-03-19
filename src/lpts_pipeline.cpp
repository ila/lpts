//==============================================================================
// lpts_pipeline.cpp
//
// Implements the two-phase pipeline:
//   Phase 1: Logical Plan → AST   (LogicalPlanToAst)
//   Phase 2: AST → CTE List       (AstToCteList)
//
// The AST is a dialect-agnostic intermediate representation. AstBuilder
// mirrors the bookkeeping in LogicalPlanToSql::CreateCteNode — it maintains
// the same column_map so that expression strings can be resolved to their
// CTE column names (e.g. ColumnBinding{0,1} → "t0_name").
//
// Operator support (Task 1 scope):
//   LOGICAL_GET         → AstGetNode        ✓
//   LOGICAL_FILTER      → AstFilterNode     ✓
//   LOGICAL_PROJECTION  → AstProjectNode    ✓
//   All others          → NotImplementedException
//==============================================================================

#include "lpts_pipeline.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//==============================================================================
// AstBuilder — Phase 1 helper
//
// Walks the LogicalOperator tree bottom-up and constructs AstNode objects.
// Maintains a column_map identical to LogicalPlanToSql so expressions can be
// converted to CTE-qualified strings (e.g. "t0_age") during the build phase.
//==============================================================================
class AstBuilder {
private:
	/// Wrapper around ColumnBinding for use as std::map key.
	struct MappableColumnBinding {
		ColumnBinding cb;
		explicit MappableColumnBinding(const ColumnBinding &_cb) : cb(_cb) {
		}
		bool operator<(const MappableColumnBinding &other) const {
			return std::tie(cb.table_index, cb.column_index) < std::tie(other.cb.table_index, other.cb.column_index);
		}
	};

	/// Metadata for one column as it flows through the plan.
	struct ColStruct {
		const idx_t table_index;
		string column_name; ///< Original physical column name.
		string alias;       ///< Optional alias for expressions. Empty if not set.

		ColStruct(const idx_t _table_index, string _column_name, string _alias)
		    : table_index(_table_index), column_name(std::move(_column_name)), alias(std::move(_alias)) {
		}

		/// Produce "t{table_index}_{alias || column_name}".
		string ToUniqueColumnName() const {
			return "t" + std::to_string(table_index) + "_" + (alias.empty() ? column_name : alias);
		}
	};

	/// Global map: ColumnBinding → ColStruct.
	/// Populated bottom-up; each operator registers its output columns here.
	std::map<MappableColumnBinding, unique_ptr<ColStruct>> column_map;

	//--------------------------------------------------------------------------
	// CollectLambdaParamNames
	//
	// Walk a lambda body to find BoundReferenceExpression nodes (the post-binding
	// form of lambda parameters). Stops at nested lambda functions.
	//--------------------------------------------------------------------------
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
			if (func.function.HasBindLambdaCallback()) {
				for (auto &child : func.children) {
					CollectLambdaParamNames(*child, names);
				}
				return;
			}
		}
		ExpressionIterator::EnumerateChildren(const_cast<Expression &>(expr),
		                                      [&](Expression &child) { CollectLambdaParamNames(child, names); });
	}

	//--------------------------------------------------------------------------
	// ExpressionToAliasedString
	//
	// Converts a bound DuckDB expression into a SQL string, replacing internal
	// ColumnBinding references with their CTE column names via column_map.
	// Mirrors LogicalPlanToSql::ExpressionToAliasedString exactly.
	//--------------------------------------------------------------------------
	string ExpressionToAliasedString(const unique_ptr<Expression> &expression) const {
		const ExpressionClass e_class = expression->GetExpressionClass();
		std::ostringstream expr_str;
		switch (e_class) {
		case ExpressionClass::BOUND_COLUMN_REF: {
			const BoundColumnRefExpression &bcr = expression->Cast<BoundColumnRefExpression>();
			const unique_ptr<ColStruct> &col_struct = column_map.at(MappableColumnBinding(bcr.binding));
			expr_str << col_struct->ToUniqueColumnName();
			break;
		}
		case ExpressionClass::BOUND_CONSTANT: {
			expr_str << expression->ToString();
			break;
		}
		case ExpressionClass::BOUND_COMPARISON: {
			const BoundComparisonExpression &cmp = expression->Cast<BoundComparisonExpression>();
			expr_str << "(";
			expr_str << ExpressionToAliasedString(cmp.left);
			expr_str << ") ";
			expr_str << ExpressionTypeToOperator(cmp.GetExpressionType());
			expr_str << " (";
			expr_str << ExpressionToAliasedString(cmp.right);
			expr_str << ")";
			break;
		}
		case ExpressionClass::BOUND_CAST: {
			const BoundCastExpression &cast_expr = expression->Cast<BoundCastExpression>();
			expr_str << (cast_expr.try_cast ? "TRY_CAST(" : "CAST(");
			expr_str << ExpressionToAliasedString(cast_expr.child);
			expr_str << " AS " + cast_expr.return_type.ToString() + ")";
			break;
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			const BoundConjunctionExpression &conj = expression->Cast<BoundConjunctionExpression>();
			expr_str << "(";
			expr_str << ExpressionToAliasedString(conj.children[0]);
			expr_str << ") ";
			expr_str << ExpressionTypeToOperator(conj.GetExpressionType());
			expr_str << " (";
			expr_str << ExpressionToAliasedString(conj.children[1]);
			expr_str << ")";
			break;
		}
		case ExpressionClass::BOUND_FUNCTION: {
			const BoundFunctionExpression &func_expr = expression->Cast<BoundFunctionExpression>();
			// For lambda functions, only serialize non-lambda, non-capture children
			idx_t child_count = func_expr.children.size();
			if (func_expr.function.HasBindLambdaCallback()) {
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
			// Operators: use infix notation (e.g. "a + b")
			if (func_expr.is_operator && child_count == 2) {
				expr_str << "(";
				expr_str << ExpressionToAliasedString(func_expr.children[0]);
				expr_str << " " << func_expr.function.name << " ";
				expr_str << ExpressionToAliasedString(func_expr.children[1]);
				expr_str << ")";
			} else {
				expr_str << func_expr.function.name << "(";
				for (idx_t i = 0; i < child_count; i++) {
					if (i > 0) {
						expr_str << ", ";
					}
					expr_str << ExpressionToAliasedString(func_expr.children[i]);
				}
				// Lambda function: serialize the lambda expression from bind_info
				if (func_expr.function.HasBindLambdaCallback() && func_expr.bind_info) {
					auto &bind_data = func_expr.bind_info->Cast<ListLambdaBindData>();
					if (bind_data.lambda_expr) {
						if (child_count > 0) {
							expr_str << ", ";
						}
						std::map<idx_t, string> param_map;
						CollectLambdaParamNames(*bind_data.lambda_expr, param_map);
						idx_t param_count = param_map.empty() ? 0 : param_map.rbegin()->first + 1;
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
		case ExpressionClass::BOUND_REF: {
			expr_str << expression->ToString();
			break;
		}
		case ExpressionClass::BOUND_LAMBDA_REF: {
			expr_str << expression->ToString();
			break;
		}
		default:
			throw NotImplementedException("Unsupported expression for ExpressionToAliasedString: %s",
			                              ExpressionTypeToString(expression->type));
		}
		return expr_str.str();
	}

	//--------------------------------------------------------------------------
	// BuildNode
	//
	// Creates an AstNode for the given operator. Children must already be
	// processed and attached. Returns the AstNode with column_map updated for
	// this operator's output columns so parent operators can resolve them.
	//--------------------------------------------------------------------------
	unique_ptr<AstNode> BuildNode(unique_ptr<LogicalOperator> &op) {
		switch (op->type) {
		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_GET: {
			const LogicalGet &get = op->Cast<LogicalGet>();
			auto catalog_entry = get.GetTable();
			const size_t table_index = get.table_index;
			const string catalog_name = get.GetTable()->schema.ParentCatalog().GetName();
			const string schema_name = catalog_entry->schema.name;
			const string table_name = catalog_entry.get()->name;

			vector<string> column_names;
			vector<string> cte_column_names;
			vector<string> table_filters;

			const vector<ColumnBinding> col_binds = op->GetColumnBindings();
			const auto col_ids = get.GetColumnIds();

			for (size_t i = 0; i < col_binds.size(); ++i) {
				if (i >= col_ids.size()) {
					// ROWID-only scan (e.g. COUNT(*)) — register dummy entry.
					const ColumnBinding &cb = col_binds[i];
					column_map[MappableColumnBinding(cb)] = make_uniq<ColStruct>(table_index, "rowid", "");
					continue;
				}
				const idx_t idx = col_ids[i].GetPrimaryIndex();
				const string col_name = get.names[idx];
				const ColumnBinding &cb = col_binds[i];
				auto col_struct = make_uniq<ColStruct>(table_index, col_name, "");
				column_names.push_back(col_name);
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
				column_map[MappableColumnBinding(cb)] = std::move(col_struct);
			}

			// Pushdown table filters (rare, but present in some plans).
			if (!get.table_filters.filters.empty()) {
				for (auto &filter : get.table_filters.filters) {
					table_filters.push_back(filter.second->ToString(get.names[filter.first]));
				}
			}

			return make_uniq<AstGetNode>(catalog_name, schema_name, table_name, table_index, std::move(column_names),
			                             std::move(cte_column_names), std::move(table_filters));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_FILTER: {
			const LogicalFilter &filter_op = op->Cast<LogicalFilter>();
			vector<string> conditions;
			for (const unique_ptr<Expression> &expr : filter_op.expressions) {
				conditions.emplace_back(ExpressionToAliasedString(expr));
			}
			return make_uniq<AstFilterNode>(std::move(conditions));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			const LogicalProjection &proj = op->Cast<LogicalProjection>();
			const size_t table_index = proj.table_index;
			vector<string> expressions;
			vector<string> cte_column_names;

			for (size_t i = 0; i < proj.expressions.size(); ++i) {
				const unique_ptr<Expression> &expr = proj.expressions[i];
				const ColumnBinding new_cb = ColumnBinding(table_index, i);

				if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
					BoundColumnRefExpression &bcr = expr->Cast<BoundColumnRefExpression>();
					unique_ptr<ColStruct> &desc = column_map.at(MappableColumnBinding(bcr.binding));
					expressions.push_back(desc->ToUniqueColumnName());
					auto new_col = make_uniq<ColStruct>(table_index, desc->column_name, desc->alias);
					cte_column_names.push_back(new_col->ToUniqueColumnName());
					column_map[MappableColumnBinding(new_cb)] = std::move(new_col);
				} else {
					string expr_str = ExpressionToAliasedString(expr);
					expressions.emplace_back(expr_str);
					string scalar_alias = expr->HasAlias() ? expr->GetAlias() : "scalar_" + std::to_string(i);
					auto new_col = make_uniq<ColStruct>(table_index, expr_str, std::move(scalar_alias));
					cte_column_names.push_back(new_col->ToUniqueColumnName());
					column_map[MappableColumnBinding(new_cb)] = std::move(new_col);
				}
			}

			return make_uniq<AstProjectNode>(std::move(expressions), std::move(cte_column_names), table_index);
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			const LogicalAggregate &agg = op->Cast<LogicalAggregate>();
			const idx_t group_table_index = agg.group_index;
			const idx_t agg_table_index = agg.aggregate_index;
			vector<string> group_names;
			vector<string> agg_expressions;
			vector<string> cte_column_names;

			// GROUP BY columns
			for (size_t i = 0; i < agg.groups.size(); ++i) {
				const unique_ptr<Expression> &g = agg.groups[i];
				if (g->type != ExpressionType::BOUND_COLUMN_REF) {
					throw NotImplementedException("AstBuilder: only BOUND_COLUMN_REF supported in GROUP BY");
				}
				BoundColumnRefExpression &bcr = g->Cast<BoundColumnRefExpression>();
				unique_ptr<ColStruct> &desc = column_map.at(MappableColumnBinding(bcr.binding));
				group_names.push_back(desc->ToUniqueColumnName());
				auto new_col = make_uniq<ColStruct>(group_table_index, desc->column_name, desc->alias);
				cte_column_names.push_back(new_col->ToUniqueColumnName());
				column_map[MappableColumnBinding(ColumnBinding(group_table_index, i))] = std::move(new_col);
			}

			// Aggregate expressions
			for (size_t i = 0; i < agg.expressions.size(); ++i) {
				const unique_ptr<Expression> &expr = agg.expressions[i];
				if (expr->type != ExpressionType::BOUND_AGGREGATE) {
					throw NotImplementedException("AstBuilder: only BOUND_AGGREGATE supported in aggregates");
				}
				const BoundAggregateExpression &ba = expr->Cast<BoundAggregateExpression>();
				std::ostringstream agg_str;
				agg_str << ba.function.name << "(";
				if (ba.IsDistinct()) {
					agg_str << "DISTINCT ";
				}
				vector<string> child_exprs;
				for (const unique_ptr<Expression> &c : ba.children) {
					child_exprs.push_back(ExpressionToAliasedString(c));
				}
				// Join child expressions with commas
				for (size_t ci = 0; ci < child_exprs.size(); ++ci) {
					if (ci > 0)
						agg_str << ", ";
					agg_str << child_exprs[ci];
				}
				agg_str << ")";
				string agg_alias = "aggregate_" + std::to_string(i);
				agg_expressions.push_back(agg_str.str());
				auto new_col = make_uniq<ColStruct>(agg_table_index, agg_str.str(), std::move(agg_alias));
				cte_column_names.push_back(new_col->ToUniqueColumnName());
				column_map[MappableColumnBinding(ColumnBinding(agg_table_index, i))] = std::move(new_col);
			}

			return make_uniq<AstAggregateNode>(std::move(group_names), std::move(agg_expressions),
			                                   std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			const LogicalComparisonJoin &join_op = op->Cast<LogicalComparisonJoin>();
			vector<string> conditions;
			for (const auto &cond : join_op.conditions) {
				string lhs = ExpressionToAliasedString(cond.left);
				string rhs = ExpressionToAliasedString(cond.right);
				string cmp = ExpressionTypeToOperator(cond.comparison);
				conditions.push_back("(" + lhs + " " + cmp + " " + rhs + ")");
			}
			// collect output column names from column_map (already populated by children)
			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				unique_ptr<ColStruct> &col_struct = column_map.at(MappableColumnBinding(cb));
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
			}
			return make_uniq<AstJoinNode>(join_op.join_type, std::move(conditions), std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_UNION: {
			const LogicalSetOperation &set_op = op->Cast<LogicalSetOperation>();
			const size_t table_index = set_op.table_index;
			vector<string> cte_column_names;
			const auto &lhs_bindings = op->children[0]->GetColumnBindings();
			const auto &union_bindings = op->GetColumnBindings();
			for (size_t i = 0; i < lhs_bindings.size(); ++i) {
				const unique_ptr<ColStruct> &lhs_col = column_map.at(MappableColumnBinding(lhs_bindings[i]));
				auto new_col = make_uniq<ColStruct>(table_index, lhs_col->column_name, lhs_col->alias);
				cte_column_names.push_back(new_col->ToUniqueColumnName());
				column_map[MappableColumnBinding(union_bindings[i])] = std::move(new_col);
			}
			return make_uniq<AstUnionNode>(set_op.setop_all, std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_INSERT:
			throw NotImplementedException("AstBuilder: LOGICAL_INSERT is not yet implemented");
		default:
			throw NotImplementedException("AstBuilder: operator '%s' is not yet implemented",
			                              LogicalOperatorToString(op->type));
		}
	}

	//--------------------------------------------------------------------------
	// RecursiveTraversal
	//
	// Bottom-up: process all children first, attach them to the current node,
	// then create the current node.
	//--------------------------------------------------------------------------
	unique_ptr<AstNode> RecursiveTraversal(unique_ptr<LogicalOperator> &op) {
		// 1. Recurse into children first (post-order).
		vector<unique_ptr<AstNode>> child_nodes;
		for (auto &child : op->children) {
			child_nodes.push_back(RecursiveTraversal(child));
		}
		// 2. Build this node (column_map is now populated by children).
		unique_ptr<AstNode> node = BuildNode(op);
		// 3. Attach children to preserve the tree structure.
		for (auto &c : child_nodes) {
			node->children.push_back(std::move(c));
		}
		return node;
	}

public:
	explicit AstBuilder() = default;

	/// Entry point: walk the plan and return the AST root.
	unique_ptr<AstNode> Build(unique_ptr<LogicalOperator> &plan) {
		return RecursiveTraversal(plan);
	}
};

//==============================================================================
// Phase 1 entry point
//==============================================================================
unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	AstBuilder builder;
	return builder.Build(plan);
}

//==============================================================================
// ParseSqlDialect
//==============================================================================
SqlDialect ParseSqlDialect(const string &value) {
	if (value == "duckdb" || value == "DUCKDB") {
		return SqlDialect::DUCKDB;
	}
	if (value == "postgres" || value == "POSTGRES" || value == "postgresql" || value == "POSTGRESQL") {
		return SqlDialect::POSTGRES;
	}
	throw InvalidInputException("Unknown lpts_dialect '%s'. Valid values: 'duckdb', 'postgres'", value);
}

//==============================================================================
// AstFlattener — Phase 2 helper
//
// Walks the AST in post-order and produces a flat CteList that is identical
// to what LogicalPlanToSql::LogicalPlanToCteList() would produce.
//
// Each AstNode type maps to a specific CteNode type. CTE names are assigned
// using the same monotonically increasing counter as the original pipeline,
// generating scan_N, filter_N, projection_N, etc.
//==============================================================================
class AstFlattener {
private:
	size_t node_count = 0;
	vector<unique_ptr<CteNode>> cte_nodes;
	SqlDialect dialect; // Controls dialect-specific SQL rendering.

	/// Compute the CTE name for a given node type and index.
	static string CteName(const AstNode &node, size_t index) {
		const string &type = node.NodeType();
		if (type == "Get") {
			return "scan_" + std::to_string(index);
		} else if (type == "Filter") {
			return "filter_" + std::to_string(index);
		} else if (type == "Project") {
			return "projection_" + std::to_string(index);
		} else if (type == "Aggregate") {
			return "aggregate_" + std::to_string(index);
		} else if (type == "Join") {
			return "join_" + std::to_string(index);
		} else if (type == "Union") {
			return "union_" + std::to_string(index);
		} else {
			return "node_" + std::to_string(index);
		}
	}

	//--------------------------------------------------------------------------
	// FlattenNode: post-order walk → produce CteNode for each AstNode
	//--------------------------------------------------------------------------
	unique_ptr<CteNode> FlattenNode(const AstNode &ast_node) {
		// 1. Recurse into children first (post-order), collecting their indices
		//    and CTE names for use when constructing the parent's CteNode.
		vector<size_t> children_indices;
		for (const auto &child : ast_node.children) {
			unique_ptr<CteNode> child_cte = FlattenNode(*child);
			children_indices.push_back(child_cte->idx);
			cte_nodes.push_back(std::move(child_cte));
		}

		// 2. Assign an index to this node.
		const size_t my_index = node_count++;

		// 3. Create the CteNode matching this AstNode type.
		const string &type = ast_node.NodeType();

		if (type == "Get") {
			const AstGetNode &get = static_cast<const AstGetNode &>(ast_node);
			// Dialect-specific table reference:
			//   DuckDB:   catalog.schema.table  (e.g. memory.main.users)
			//   Postgres: table                 (e.g. users)
			string catalog_out = (dialect == SqlDialect::POSTGRES) ? "" : get.catalog;
			string schema_out = (dialect == SqlDialect::POSTGRES) ? "" : get.schema;
			return make_uniq<GetNode>(my_index, get.cte_column_names, catalog_out, schema_out, get.table_name,
			                          get.table_index, get.table_filters, get.column_names);
		}

		if (type == "Filter") {
			const AstFilterNode &filter = static_cast<const AstFilterNode &>(ast_node);
			// FilterNode has no explicit CTE column list (it does SELECT * FROM child).
			const string &child_cte_name = cte_nodes[children_indices[0]]->cte_name;
			return make_uniq<FilterNode>(my_index, vector<string>(), child_cte_name, filter.conditions);
		}

		if (type == "Project") {
			const AstProjectNode &proj = static_cast<const AstProjectNode &>(ast_node);
			const string &child_cte_name = cte_nodes[children_indices[0]]->cte_name;
			return make_uniq<ProjectNode>(my_index, proj.cte_column_names, child_cte_name, proj.expressions,
			                              proj.table_index);
		}

		if (type == "Aggregate") {
			const AstAggregateNode &agg = static_cast<const AstAggregateNode &>(ast_node);
			const string &child_cte_name = cte_nodes[children_indices[0]]->cte_name;
			return make_uniq<AggregateNode>(my_index, agg.cte_column_names, child_cte_name, agg.group_by_columns,
			                                agg.aggregate_expressions);
		}

		if (type == "Join") {
			const AstJoinNode &join = static_cast<const AstJoinNode &>(ast_node);
			const string &left_cte_name = cte_nodes[children_indices[0]]->cte_name;
			const string &right_cte_name = cte_nodes[children_indices[1]]->cte_name;
			return make_uniq<JoinNode>(my_index, join.cte_column_names, left_cte_name, right_cte_name, join.join_type,
			                           join.conditions);
		}

		if (type == "Union") {
			const AstUnionNode &u = static_cast<const AstUnionNode &>(ast_node);
			const string &left_cte_name = cte_nodes[children_indices[0]]->cte_name;
			const string &right_cte_name = cte_nodes[children_indices[1]]->cte_name;
			return make_uniq<UnionNode>(my_index, u.cte_column_names, left_cte_name, right_cte_name, u.is_union_all);
		}

		// Operators not yet implemented.
		throw NotImplementedException("AstFlattener: node type '%s' is not yet implemented", type);
	}

public:
	explicit AstFlattener(SqlDialect dialect = SqlDialect::DUCKDB) : dialect(dialect) {
	}

	/// Flatten the AST rooted at `root` into a CteList.
	/// The root node is handled specially (it produces the FinalReadNode).
	unique_ptr<CteList> Flatten(const AstNode &root) {
		// FlattenNode handles the entire subtree (including root's children)
		// bottom-up via its internal recursion. Do NOT manually iterate
		// root.children here — that would double-flatten them.
		unique_ptr<CteNode> last_cte = FlattenNode(root);

		// Build the FinalReadNode: maps CTE column names back to original names.
		vector<string> final_column_list;
		const string &type = root.NodeType();
		const vector<string> &cte_cols = (type == "Project")
		                                     ? static_cast<const AstProjectNode &>(root).cte_column_names
		                                     : last_cte->cte_column_list;

		for (const string &cte_col : cte_cols) {
			// cte_col = "t1_name"  →  strip "tN_" prefix to get user-visible name.
			const size_t underscore_pos = cte_col.find('_');
			if (underscore_pos != string::npos && underscore_pos + 1 < cte_col.size()) {
				final_column_list.push_back(cte_col.substr(underscore_pos + 1));
			} else {
				final_column_list.push_back(cte_col);
			}
		}

		const size_t final_index = node_count++;
		auto final_node = make_uniq<FinalReadNode>(final_index, last_cte->cte_name, last_cte->cte_column_list,
		                                           std::move(final_column_list));
		cte_nodes.push_back(std::move(last_cte));
		return make_uniq<CteList>(std::move(cte_nodes), std::move(final_node));
	}
};

//==============================================================================
// Phase 2 entry point
//==============================================================================
unique_ptr<CteList> AstToCteList(const AstNode &root, SqlDialect dialect) {
	AstFlattener flattener(dialect);
	return flattener.Flatten(root);
}

} // namespace duckdb
