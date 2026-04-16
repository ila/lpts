#include "storage/ducklake_scan.hpp"

//==============================================================================
// lpts_pipeline.cpp
//
// Implements the two-phase pipeline:
//   Phase 1: Logical Plan → AST   (LogicalPlanToAst)
//   Phase 2: AST → CTE List       (AstToCteList)
//
// The AST is a dialect-agnostic intermediate representation. AstBuilder
// mirrors the bookkeeping in the old CreateCteNode — it maintains
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
#include "duckdb/main/connection.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_lambda_expression.hpp"
#include "duckdb/planner/expression/bound_lambdaref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
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
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//==============================================================================
// AstBuilder — Phase 1 helper
//
// Walks the LogicalOperator tree bottom-up and constructs AstNode objects.
// Maintains a column_map so expressions can be
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
			string base = alias.empty() ? column_name : alias;
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
	};

	/// SQL dialect for expression serialization (function renaming, etc.)
	SqlDialect dialect = SqlDialect::DUCKDB;

	/// Client context for runtime queries (e.g. DuckLake current snapshot).
	ClientContext &context;

	/// Cache: DuckLake catalog name → current snapshot_id.
	/// Populated lazily on first DuckLake scan per catalog.
	unordered_map<string, idx_t> ducklake_current_snapshots;

	/// Query the current snapshot_id for a DuckLake catalog.
	idx_t GetDuckLakeCurrentSnapshot(const string &catalog_name) {
		auto it = ducklake_current_snapshots.find(catalog_name);
		if (it != ducklake_current_snapshots.end()) {
			return it->second;
		}
		Connection con(*context.db);
		auto result = con.Query("SELECT id FROM " + catalog_name + ".current_snapshot()");
		idx_t snap_id = DConstants::INVALID_INDEX;
		if (!result->HasError() && result->RowCount() > 0) {
			snap_id = result->GetValue(0, 0).GetValue<idx_t>();
		}
		ducklake_current_snapshots[catalog_name] = snap_id;
		return snap_id;
	}

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

	//--------------------------------------------------------------------------
	// ExpressionToAliasedString
	//
	// Converts a bound DuckDB expression into a SQL string, replacing internal
	// ColumnBinding references with their CTE column names via column_map.
	// Converts a bound Expression into a SQL string.
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
			// Dialect-specific function name remapping.
			string func_name = func_expr.function.name;
			if (dialect == SqlDialect::POSTGRES) {
				if (func_name == "strptime") {
					func_name = "to_timestamp";
				} else if (func_name == "strftime") {
					func_name = "to_char";
				}
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
			// Operators: use infix notation (e.g. "a + b").
			// Some plan rewrites (like AVG decomposition) create operator functions
			// without setting is_operator. Fall back to name-based detection.
			bool is_infix = func_expr.is_operator ||
			                (child_count == 2 && (func_name == "/" || func_name == "+" || func_name == "-" ||
			                                      func_name == "*" || func_name == "%" || func_name == "||"));
			if (is_infix && child_count == 2) {
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
		case ExpressionClass::BOUND_CASE: {
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
		case ExpressionClass::BOUND_OPERATOR: {
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
		default:
			throw NotImplementedException("Unsupported expression for ExpressionToAliasedString: %s",
			                              ExpressionTypeToString(expression->type));
		}
		return expr_str.str();
	}

	//--------------------------------------------------------------------------
	// IsCompressedMaterializationProjection
	//
	// Returns true if the projection contains only __internal_compress_* or
	// __internal_decompress_* function calls (plus pass-through column refs).
	// These are DuckDB optimizer-internal compressed materialization nodes
	// that cannot appear in user-facing SQL.
	//--------------------------------------------------------------------------
	static bool IsCompressedMaterializationProjection(const LogicalProjection &proj) {
		if (proj.expressions.empty()) {
			return false;
		}
		bool has_internal_func = false;
		for (auto &expr : proj.expressions) {
			if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
				continue; // pass-through is fine
			}
			if (expr->type == ExpressionType::BOUND_FUNCTION) {
				auto &func = expr->Cast<BoundFunctionExpression>();
				const string &name = func.function.name;
				if (name.rfind("__internal_compress_", 0) == 0 || name.rfind("__internal_decompress_", 0) == 0) {
					has_internal_func = true;
					continue;
				}
			}
			return false; // non-passthrough, non-internal expression
		}
		return has_internal_func;
	}

	//--------------------------------------------------------------------------
	// BuildNode
	//
	// Creates an AstNode for the given operator. Children must already be
	// processed and attached. Returns the AstNode with column_map updated for
	// this operator's output columns so parent operators can resolve them.
	// Returns nullptr for nodes that should be skipped (e.g. compressed
	// materialization projections).
	//--------------------------------------------------------------------------
	unique_ptr<AstNode> BuildNode(unique_ptr<LogicalOperator> &op) {
		switch (op->type) {
		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_GET: {
			const LogicalGet &get = op->Cast<LogicalGet>();
			auto catalog_entry = get.GetTable();
			const idx_t table_index = get.table_index;
			string catalog_name;
			string schema_name;
			string table_name;

			LPTS_DEBUG_PRINT("[LPTS-AST] GET: function.name='" + get.function.name +
			                 "' params=" + std::to_string(get.parameters.size()) +
			                 " named_params=" + std::to_string(get.named_parameters.size()) +
			                 " catalog_entry=" + string(catalog_entry ? "valid" : "null"));
			for (size_t pi = 0; pi < get.parameters.size(); pi++) {
				LPTS_DEBUG_PRINT("[LPTS-AST] GET:   param[" + std::to_string(pi) +
				                 "]=" + get.parameters[pi].ToString());
			}
			for (auto &np : get.named_parameters) {
				LPTS_DEBUG_PRINT("[LPTS-AST] GET:   named '" + np.first + "'=" + np.second.ToString());
			}
			auto params_map = get.ParamsToString();
			for (auto &ps : params_map) {
				LPTS_DEBUG_PRINT("[LPTS-AST] GET:   ParamsToString '" + ps.first + "'='" + ps.second + "'");
			}

			// Check for DuckLake snapshot-range scans (insertions/deletions) and time travel.
			bool is_ducklake_change_scan = false;
			bool is_ducklake_time_travel = false;
			idx_t ducklake_snapshot_id = 0;
			if (get.function.name == "ducklake_scan" && get.function.function_info) {
				auto &func_info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
				// AT VERSION: only emit for explicit time-travel scans, NOT for
				// regular current-snapshot scans. Every ducklake_scan has a valid
				// snapshot_id (the current transaction's), but emitting AT VERSION
				// for those would pin stored queries (e.g. MV definitions) to a
				// specific snapshot instead of reading current data.
				// Detection: compare the scan's snapshot against the transaction's
				// current snapshot (same pattern as ducklake_scan.cpp:158).
				if (func_info.scan_type == DuckLakeScanType::SCAN_TABLE) {
					// Detect explicit time travel by comparing the scan's snapshot
					// against the catalog's current snapshot. Regular scans use the
					// current snapshot; AT VERSION scans use a historical one.
					auto catalog_entry = get.GetTable();
					if (catalog_entry) {
						string cat_name = catalog_entry->ParentCatalog().GetName();
						idx_t current_snap = GetDuckLakeCurrentSnapshot(cat_name);
						if (current_snap != DConstants::INVALID_INDEX &&
						    func_info.snapshot.snapshot_id != current_snap) {
							is_ducklake_time_travel = true;
							ducklake_snapshot_id = func_info.snapshot.snapshot_id;
						}
					}
				}
				if (func_info.scan_type == DuckLakeScanType::SCAN_INSERTIONS ||
				    func_info.scan_type == DuckLakeScanType::SCAN_DELETIONS) {
					is_ducklake_change_scan = true;
					string func_name = (func_info.scan_type == DuckLakeScanType::SCAN_INSERTIONS)
					                       ? "ducklake_table_insertions"
					                       : "ducklake_table_deletions";
					std::ostringstream func_str;
					func_str << func_name << "(";
					for (size_t i = 0; i < get.parameters.size(); i++) {
						if (i > 0) {
							func_str << ", ";
						}
						func_str << get.parameters[i].ToSQLString();
					}
					func_str << ")";
					table_name = func_str.str();
					LPTS_DEBUG_PRINT("[LPTS-AST] GET: DuckLake change scan -> " + table_name);
				}
			}

			if (!is_ducklake_change_scan) {
				if (catalog_entry) {
					catalog_name = catalog_entry->schema.ParentCatalog().GetName();
					schema_name = catalog_entry->schema.name;
					table_name = catalog_entry.get()->name;
					if (is_ducklake_time_travel) {
						table_name += " AT (VERSION => " + std::to_string(ducklake_snapshot_id) + ")";
					}
				} else {
					// Table function without catalog entry (e.g. range(), read_csv())
					std::ostringstream func_str;
					func_str << get.function.name << "(";
					for (size_t i = 0; i < get.parameters.size(); i++) {
						if (i > 0) {
							func_str << ", ";
						}
						func_str << get.parameters[i].ToSQLString();
					}
					func_str << ")";
					table_name = func_str.str();
				}
			}

			vector<string> column_names;
			vector<string> cte_column_names;
			vector<string> table_filters;

			const vector<ColumnBinding> col_binds = op->GetColumnBindings();
			const auto col_ids = get.GetColumnIds();

			LPTS_DEBUG_PRINT("[LPTS-AST] GET: col_binds=" + std::to_string(col_binds.size()) + " col_ids=" +
			                 std::to_string(col_ids.size()) + " names=" + std::to_string(get.names.size()));
			for (size_t di = 0; di < col_ids.size(); di++) {
				LPTS_DEBUG_PRINT("[LPTS-AST] GET:   col_id[" + std::to_string(di) +
				                 "] primary=" + std::to_string(col_ids[di].GetPrimaryIndex()) +
				                 " virtual=" + std::to_string(col_ids[di].IsVirtualColumn()));
			}

			for (size_t i = 0; i < col_binds.size(); ++i) {
				const ColumnBinding &cb = col_binds[i];
				// The binding's column_index tells us which entry in col_ids
				// this output column corresponds to. When projection_ids is set
				// (optimizer removed unused columns), the binding index may
				// differ from the loop index.
				const idx_t col_id_idx = cb.column_index;
				if (col_id_idx >= col_ids.size()) {
					// ROWID-only scan (e.g. COUNT(*)) — register dummy entry.
					column_map[MappableColumnBinding(cb)] = make_uniq<ColStruct>(table_index, "rowid", "");
					continue;
				}
				string col_name;
				if (col_ids[col_id_idx].IsVirtualColumn()) {
					// Virtual columns (snapshot_id, rowid, etc.) — look up in virtual_columns map
					auto vit = get.virtual_columns.find(col_ids[col_id_idx].GetPrimaryIndex());
					col_name = (vit != get.virtual_columns.end()) ? vit->second.name : "";
					if (col_name.empty()) {
						col_name = "rowid";
					}
				} else {
					const idx_t idx = col_ids[col_id_idx].GetPrimaryIndex();
					col_name = get.names[idx];
				}
				auto col_struct = make_uniq<ColStruct>(table_index, col_name, "");
				column_names.push_back(col_name);
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
				column_map[MappableColumnBinding(cb)] = std::move(col_struct);
			}

			// COUNT(*) / ROWID-only scans: emit a dummy column so the CTE body
			// is valid SQL (SELECT 1 FROM ...). This covers both empty scans and
			// virtual-column-only scans (e.g. DuckLake ROWID for COUNT(*)).
			bool has_real_column = false;
			for (auto &cn : column_names) {
				if (cn != "rowid") {
					has_real_column = true;
					break;
				}
			}
			if (!has_real_column) {
				column_names.clear();
				cte_column_names.clear();
				column_names.push_back("1");
				cte_column_names.push_back("t" + std::to_string(table_index) + "_dummy");
			}

			// Pushdown table filters (rare, but present in some plans).
			if (!get.table_filters.filters.empty()) {
				for (auto &entry : get.table_filters.filters) {
					table_filters.push_back(entry.second->ToString(get.names[entry.first]));
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
			const idx_t table_index = proj.table_index;

			// Skip compressed materialization projections (compress/decompress).
			// These contain __internal_compress_* or __internal_decompress_* functions
			// that are internal-only and cannot appear in user-facing SQL.
			// We remap bindings to point through to the source columns and return nullptr
			// to signal RecursiveTraversal to pass through the child node directly.
			if (IsCompressedMaterializationProjection(proj)) {
				LPTS_DEBUG_PRINT("[LPTS-AST] Skipping compressed materialization projection (table_index=" +
				                 std::to_string(table_index) + ")");
				for (size_t i = 0; i < proj.expressions.size(); ++i) {
					const ColumnBinding new_cb(table_index, i);
					const unique_ptr<Expression> &expr = proj.expressions[i];
					if (expr->type == ExpressionType::BOUND_FUNCTION) {
						// compress/decompress: remap to child's source column,
						// preserving the source's table_index so the column name
						// matches the CTE that actually defines it.
						auto &func = expr->Cast<BoundFunctionExpression>();
						D_ASSERT(!func.children.empty());
						auto &child = func.children[0];
						if (child->type == ExpressionType::BOUND_COLUMN_REF) {
							auto &bcr = child->Cast<BoundColumnRefExpression>();
							auto &src = column_map.at(MappableColumnBinding(bcr.binding));
							column_map[MappableColumnBinding(new_cb)] =
							    make_uniq<ColStruct>(src->table_index, src->column_name, src->alias);
						}
					} else if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
						// pass-through column ref: preserve source's table_index
						auto &bcr = expr->Cast<BoundColumnRefExpression>();
						auto &src = column_map.at(MappableColumnBinding(bcr.binding));
						column_map[MappableColumnBinding(new_cb)] =
						    make_uniq<ColStruct>(src->table_index, src->column_name, src->alias);
					}
				}
				return nullptr;
			}

			vector<string> expressions;
			vector<string> cte_column_names;
			unordered_set<string> seen_names;

			for (size_t i = 0; i < proj.expressions.size(); ++i) {
				const unique_ptr<Expression> &expr = proj.expressions[i];
				const ColumnBinding new_cb = ColumnBinding(table_index, i);

				if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
					BoundColumnRefExpression &bcr = expr->Cast<BoundColumnRefExpression>();
					unique_ptr<ColStruct> &desc = column_map.at(MappableColumnBinding(bcr.binding));
					expressions.push_back(desc->ToUniqueColumnName());
					string col_name = desc->column_name;
					string alias = desc->alias;
					// Deduplicate: joins can produce same-named columns from different tables.
					// Build unique CTE column name; append _N suffix on collision.
					string base = alias.empty() ? col_name : alias;
					string deduped = base;
					string unique_name = "t" + to_string(table_index) + "_" + deduped;
					for (size_t suffix = i; seen_names.count(unique_name); suffix++) {
						deduped = base + "_" + to_string(suffix);
						unique_name = "t" + to_string(table_index) + "_" + deduped;
					}
					seen_names.insert(unique_name);
					if (alias.empty()) {
						col_name = deduped;
					} else {
						alias = deduped;
					}
					auto new_col = make_uniq<ColStruct>(table_index, col_name, alias);
					cte_column_names.push_back(new_col->ToUniqueColumnName());
					column_map[MappableColumnBinding(new_cb)] = std::move(new_col);
				} else {
					string expr_str = ExpressionToAliasedString(expr);
					expressions.emplace_back(expr_str);
					string scalar_alias = expr->HasAlias() ? expr->GetAlias() : "scalar_" + std::to_string(i);
					string unique_name = "t" + to_string(table_index) + "_" + scalar_alias;
					for (size_t suffix = i; seen_names.count(unique_name); suffix++) {
						scalar_alias = "scalar_" + to_string(i) + "_" + to_string(suffix);
						unique_name = "t" + to_string(table_index) + "_" + scalar_alias;
					}
					seen_names.insert(unique_name);
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
				// Replace internal aggregate variants with their user-facing equivalents.
				// sum_no_overflow is used by compressed materialization but is internal-only.
				string agg_name = ba.function.name;
				if (agg_name == "sum_no_overflow") {
					agg_name = "sum";
				}
				agg_str << agg_name << "(";
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

			// MARK joins produce an extra boolean column indicating match existence.
			// In SQL: LEFT JOIN + (right_key IS NOT NULL) as a computed column.
			// We register it with a clean alias so parent CTEs can reference it.
			if (join_op.join_type == JoinType::MARK) {
				ColumnBinding mark_cb(join_op.mark_index, 0);
				string mark_expr;
				if (!join_op.conditions.empty()) {
					mark_expr = "(" + ExpressionToAliasedString(join_op.conditions[0].right) + " IS NOT NULL)";
				} else {
					mark_expr = "true";
				}
				auto mark_col = make_uniq<ColStruct>(join_op.mark_index, mark_expr, "_mark");
				column_map[MappableColumnBinding(mark_cb)] = std::move(mark_col);
			}

			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				unique_ptr<ColStruct> &col_struct = column_map.at(MappableColumnBinding(cb));
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
			}

			// Convert MARK join to LEFT join for SQL output
			JoinType sql_join_type = join_op.join_type;
			string mark_expr;
			if (sql_join_type == JoinType::MARK) {
				sql_join_type = JoinType::LEFT;
				// Extract the mark expression we registered in column_map
				ColumnBinding mark_cb(join_op.mark_index, 0);
				auto it = column_map.find(MappableColumnBinding(mark_cb));
				if (it != column_map.end()) {
					mark_expr = it->second->column_name; // contains the IS NOT NULL expression
				}
			}
			return make_uniq<AstJoinNode>(sql_join_type, std::move(conditions), std::move(cte_column_names),
			                              std::move(mark_expr));
		}

		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			vector<string> cross_condition = {"(TRUE)"};
			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				unique_ptr<ColStruct> &col_struct = column_map.at(MappableColumnBinding(cb));
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
			}
			return make_uniq<AstJoinNode>(JoinType::INNER, std::move(cross_condition), std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_UNION: {
			const LogicalSetOperation &set_op = op->Cast<LogicalSetOperation>();
			const idx_t table_index = set_op.table_index;
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
		case LogicalOperatorType::LOGICAL_ORDER_BY: {
			const LogicalOrder &order_op = op->Cast<LogicalOrder>();
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
					break;
				}
				order_items.push_back(std::move(col_str));
			}
			// Pass bindings through from child.
			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				cte_column_names.push_back(column_map.at(MappableColumnBinding(cb))->ToUniqueColumnName());
			}
			return make_uniq<AstOrderNode>(std::move(order_items), std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_LIMIT: {
			const LogicalLimit &limit_op = op->Cast<LogicalLimit>();
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
				offset_str =
				    ExpressionToAliasedString(const_cast<BoundLimitNode &>(limit_op.offset_val).GetExpression());
			} else if (limit_op.offset_val.Type() != LimitNodeType::UNSET) {
				throw NotImplementedException("LPTS: unsupported OFFSET node type");
			}
			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				cte_column_names.push_back(column_map.at(MappableColumnBinding(cb))->ToUniqueColumnName());
			}
			return make_uniq<AstLimitNode>(std::move(limit_str), std::move(offset_str), std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_DISTINCT: {
			// LogicalDistinct passes bindings unchanged from child.
			vector<string> cte_column_names;
			for (const ColumnBinding &cb : op->GetColumnBindings()) {
				cte_column_names.push_back(column_map.at(MappableColumnBinding(cb))->ToUniqueColumnName());
			}
			return make_uniq<AstDistinctNode>(std::move(cte_column_names));
		}

		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_INSERT: {
			const LogicalInsert &insert_op = op->Cast<LogicalInsert>();
			return make_uniq<AstInsertNode>(insert_op.table.name, insert_op.on_conflict_info.action_type);
		}
		//----------------------------------------------------------------------
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
			// The optimizer replaced a subtree with an empty result (e.g. LIMIT 0).
			// Generate a scan-like node that returns zero rows by adding WHERE false.
			const LogicalEmptyResult &empty = op->Cast<LogicalEmptyResult>();
			vector<string> column_names;
			vector<string> cte_column_names;
			for (size_t i = 0; i < empty.bindings.size(); i++) {
				string col_name = "c" + std::to_string(i);
				auto col_struct = make_uniq<ColStruct>(empty.bindings[i].table_index, col_name, "");
				cte_column_names.push_back(col_struct->ToUniqueColumnName());
				column_names.push_back("NULL::" + empty.return_types[i].ToString());
				column_map[MappableColumnBinding(empty.bindings[i])] = std::move(col_struct);
			}
			// Emit a dummy scan with WHERE false to produce zero rows
			vector<string> filters = {"false"};
			return make_uniq<AstGetNode>("", "", "__empty__", 0, std::move(column_names), std::move(cte_column_names),
			                             std::move(filters));
		}

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
		if (op->type == LogicalOperatorType::LOGICAL_UNION && op->children.size() >= 2) {
			// UNION ALL: scope column_map to prevent sibling children from overwriting
			// each other's entries when subtrees share table indices.
			child_nodes.push_back(RecursiveTraversal(op->children[0]));
			// Save column_map after first child; restore before each subsequent child
			std::map<MappableColumnBinding, unique_ptr<ColStruct>> saved_map;
			for (auto &entry : column_map) {
				saved_map[entry.first] =
				    make_uniq<ColStruct>(entry.second->table_index, entry.second->column_name, entry.second->alias);
			}
			for (size_t ci = 1; ci < op->children.size(); ci++) {
				child_nodes.push_back(RecursiveTraversal(op->children[ci]));
			}
			column_map = std::move(saved_map);
		} else {
			for (auto &child : op->children) {
				child_nodes.push_back(RecursiveTraversal(child));
			}
		}
		// 2. Build this node (column_map is now populated by children).
		unique_ptr<AstNode> node = BuildNode(op);
		// A nullptr return means this node should be skipped (e.g. compressed
		// materialization projection). Pass through the single child directly.
		if (!node) {
			D_ASSERT(child_nodes.size() == 1);
			return std::move(child_nodes[0]);
		}
		// 3. Attach children to preserve the tree structure.
		for (auto &c : child_nodes) {
			node->children.push_back(std::move(c));
		}
		return node;
	}

public:
	AstBuilder(ClientContext &_context, SqlDialect _dialect = SqlDialect::DUCKDB)
	    : dialect(_dialect), context(_context) {
	}

	/// Entry point: walk the plan and return the AST root.
	unique_ptr<AstNode> Build(unique_ptr<LogicalOperator> &plan) {
		return RecursiveTraversal(plan);
	}
};

//==============================================================================
// Phase 1 entry point
//==============================================================================
unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan, SqlDialect dialect) {
	AstBuilder builder(context, dialect);
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
// as CTE node objects that can be serialized to SQL.
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
		// 1. Recurse into children first (post-order), remembering each child's CTE
		//    name so the parent can reference it by name. We keep the name (not the
		//    child's idx) because UNION flattening may insert intermediate CTEs into
		//    cte_nodes; once that happens, a child's idx no longer equals its vector
		//    position, and `cte_nodes[idx]` silently picks up the wrong CTE.
		vector<string> children_names;
		for (const auto &child : ast_node.children) {
			unique_ptr<CteNode> child_cte = FlattenNode(*child);
			children_names.push_back(child_cte->cte_name);
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
			return make_uniq<FilterNode>(my_index, vector<string>(), children_names[0], filter.conditions);
		}

		if (type == "Project") {
			const AstProjectNode &proj = static_cast<const AstProjectNode &>(ast_node);
			return make_uniq<ProjectNode>(my_index, proj.cte_column_names, children_names[0], proj.expressions,
			                              proj.table_index);
		}

		if (type == "Aggregate") {
			const AstAggregateNode &agg = static_cast<const AstAggregateNode &>(ast_node);
			return make_uniq<AggregateNode>(my_index, agg.cte_column_names, children_names[0], agg.group_by_columns,
			                                agg.aggregate_expressions);
		}

		if (type == "Join") {
			const AstJoinNode &join = static_cast<const AstJoinNode &>(ast_node);
			return make_uniq<JoinNode>(my_index, join.cte_column_names, children_names[0], children_names[1],
			                           join.join_type, join.conditions, join.mark_expression);
		}

		if (type == "Union") {
			const AstUnionNode &u = static_cast<const AstUnionNode &>(ast_node);
			if (children_names.size() == 2) {
				return make_uniq<UnionNode>(my_index, u.cte_column_names, children_names[0], children_names[1],
				                            u.is_union_all);
			}
			// N-ary UNION: chain as left-deep binary UNIONs
			// (A UNION B UNION C) → UNION(UNION(A, B), C)
			string prev_cte_name = children_names[0];
			for (size_t ci = 1; ci < children_names.size(); ci++) {
				const string &right_cte_name = children_names[ci];
				if (ci < children_names.size() - 1) {
					// Intermediate union — create a CTE and add to cte_nodes
					size_t intermediate_index = node_count++;
					auto intermediate = make_uniq<UnionNode>(intermediate_index, u.cte_column_names, prev_cte_name,
					                                         right_cte_name, u.is_union_all);
					prev_cte_name = intermediate->cte_name;
					cte_nodes.push_back(std::move(intermediate));
				} else {
					// Final union — use the current my_index
					return make_uniq<UnionNode>(my_index, u.cte_column_names, prev_cte_name, right_cte_name,
					                            u.is_union_all);
				}
			}
			// Shouldn't reach here, but just in case
			throw InternalException("AstFlattener: empty UNION chain");
		}

		if (type == "Order") {
			const AstOrderNode &o = static_cast<const AstOrderNode &>(ast_node);
			return make_uniq<OrderNode>(my_index, o.cte_column_names, children_names[0], o.order_items);
		}

		if (type == "Limit") {
			const AstLimitNode &l = static_cast<const AstLimitNode &>(ast_node);
			return make_uniq<LimitNode>(my_index, l.cte_column_names, children_names[0], l.limit_str, l.offset_str);
		}

		if (type == "Distinct") {
			const AstDistinctNode &d = static_cast<const AstDistinctNode &>(ast_node);
			return make_uniq<DistinctNode>(my_index, d.cte_column_names, children_names[0]);
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
		const string &type = root.NodeType();

		// INSERT INTO: the root is an AstInsertNode wrapping a child plan.
		if (type == "Insert") {
			const AstInsertNode &ins = static_cast<const AstInsertNode &>(root);
			D_ASSERT(root.children.size() == 1);
			unique_ptr<CteNode> last_cte = FlattenNode(*root.children[0]);
			const size_t final_index = node_count++;
			auto insert_node =
			    make_uniq<InsertNode>(final_index, ins.target_table, last_cte->cte_name, ins.action_type);
			cte_nodes.push_back(std::move(last_cte));
			return make_uniq<CteList>(std::move(cte_nodes), std::move(insert_node));
		}

		// Regular SELECT: FlattenNode handles the entire subtree bottom-up.
		unique_ptr<CteNode> last_cte = FlattenNode(root);

		// Build the FinalReadNode: maps CTE column names back to original names.
		vector<string> final_column_list;
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
