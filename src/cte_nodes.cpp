//==============================================================================
// cte_nodes.cpp
//
// CTE node class implementations: ToQuery() for each node type, and CteList
// serialization to a SQL string.
//==============================================================================

#include "cte_nodes.hpp"
#include "lpts_helpers.hpp"
#include "lpts_debug.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
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
		// For table functions, add column aliases so renamed columns resolve correctly.
		// Skip for DuckLake functions — the _tf alias mismatches when virtual columns
		// (snapshot_id, rowid) are in the SELECT but not in the function's output schema.
		if (table_name.find('(') != string::npos && table_name != "(SELECT 1)" && !column_names.empty() &&
		    table_name.find("ducklake_table_") == string::npos) {
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
		// Wrap each condition in parentheses to preserve precedence
		// when conditions containing OR are ANDed together.
		for (size_t i = 0; i < conditions.size(); i++) {
			if (i > 0) {
				get_str << " AND ";
			}
			get_str << "(" << conditions[i] << ")";
		}
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
	// For MARK→LEFT joins, the last column is a computed mark expression.
	if (!mark_expression.empty() && !cte_column_list.empty()) {
		vector<string> select_cols(cte_column_list.begin(), cte_column_list.end() - 1);
		select_cols.push_back(mark_expression);
		join_str << "SELECT " << VecToSeparatedList(select_cols) << " FROM ";
	} else {
		join_str << "SELECT " << VecToSeparatedList(cte_column_list) << " FROM ";
	}
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
	// MARK→LEFT joins: deduplicate the right side to prevent left-row multiplication
	// when the RHS has duplicate matching values. IN subquery semantics treat the RHS
	// as a set, so (SELECT DISTINCT * FROM rhs) preserves correctness.
	if (!mark_expression.empty()) {
		LPTS_DEBUG_PRINT("[LPTS-CTE] MARK join: wrapping right CTE '" + right_cte_name +
		                 "' in SELECT DISTINCT to prevent duplicate rows");
		join_str << "(SELECT DISTINCT * FROM " << right_cte_name << ") AS _rhs_dedup";
	} else {
		join_str << right_cte_name;
	}
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
string CteList::ToQuery(const bool use_newlines, const vector<string> &output_names) {
	// Override final column aliases if output_names are provided
	if (!output_names.empty()) {
		auto *fr = dynamic_cast<FinalReadNode *>(final_node.get());
		if (fr) {
			for (size_t i = 0; i < output_names.size() && i < fr->final_column_list.size(); i++) {
				if (!output_names[i].empty()) {
					fr->final_column_list[i] = output_names[i];
				}
			}
		}
	}

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

} // namespace duckdb
