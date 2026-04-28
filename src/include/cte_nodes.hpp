#pragma once

#include "duckdb.hpp"

namespace duckdb {

//==============================================================================
// CTE List Node Hierarchy
//==============================================================================
//
// The conversion pipeline is: Logical Plan → CTE List → SQL String.
//
// There is no AST involved. The CTE list is a flat, ordered list of CTEs
// (Common Table Expressions). Each logical operator from DuckDB's plan becomes
// one CTE. Dependencies between CTEs are expressed through name references
// (e.g. a filter CTE reads FROM its child scan CTE by name), not through
// parent-child pointers.
//
// The bottom-up traversal of the logical plan guarantees that each CTE only
// references CTEs defined before it.
//
// Example: "SELECT name FROM users WHERE age > 25" produces:
//
//   WITH scan_0(t0_name, t0_age) AS (SELECT name, age FROM memory.main.users),
//        filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (25)),
//        projection_2(t2_name) AS (SELECT t0_name FROM filter_1)
//   SELECT t2_name AS name FROM projection_2;
//
// Class hierarchy:
//   CteBaseNode (base)
//   ├── RootNode (virtual) — terminal nodes that produce the final query
//   │   ├── FinalReadNode — the closing SELECT that renames CTE columns back
//   │   └── InsertNode — INSERT INTO ... SELECT * FROM <cte>
//   └── CteNode (virtual) — intermediate nodes, each becomes a WITH clause
//       ├── GetNode        — table scan
//       ├── FilterNode     — WHERE clause
//       ├── ProjectNode    — column selection / expressions
//       ├── AggregateNode  — GROUP BY + aggregate functions
//       ├── JoinNode       — INNER/LEFT/RIGHT/OUTER JOIN
//       ├── UnionNode      — UNION / UNION ALL
//       └── ExceptNode     — EXCEPT / EXCEPT ALL
//==============================================================================

/// Base node for all nodes in the CTE list. Virtual class.
class CteBaseNode {
public:
	virtual ~CteBaseNode() = default;
	/// Produce the SQL fragment for this node (the body inside a CTE's AS (...)).
	virtual string ToQuery() = 0;
	// Constructor.
	explicit CteBaseNode(const size_t index) : idx(index) {
	}
	const size_t idx; // Unique index used for naming (e.g. scan_0, filter_1).
};

/// Virtual class for terminal/root nodes (SELECT result or INSERT).
/// These are NOT wrapped in a CTE — they appear as the final statement.
class RootNode : public CteBaseNode {
public:
	explicit RootNode(const size_t index) : CteBaseNode(index) {
	}
};

/// Final node specifically for SELECT queries.
/// Renames CTE column names back to the original column names the user expects.
class FinalReadNode : public RootNode {
	// Attributes.
	string child_cte_name;
	vector<string> child_cte_column_list;

public:
	vector<string> final_column_list;
	~FinalReadNode() override = default;
	// Constructor. Creates the root representation of a SELECT node.
	FinalReadNode(const size_t index, string _child_cte_name, vector<string> _child_cte_column_list,
	              vector<string> _final_column_list)
	    : RootNode(index), child_cte_name(std::move(_child_cte_name)),
	      child_cte_column_list(std::move(_child_cte_column_list)), final_column_list(std::move(_final_column_list)) {
	}
	// Functions
	string ToQuery() override;
};

/// Node for insertion queries. Cannot be a CTE.
class InsertNode : public RootNode {
	// Attributes.
	string target_table;
	string child_cte_name; // If "insert into t_name values (...)", not defined.
	OnConflictAction action_type;

public:
	~InsertNode() override = default;
	// Constructor.
	InsertNode(const size_t index, string _target_table, string _child_cte_name,
	           const OnConflictAction conflict_action_type)
	    : RootNode(index), target_table(std::move(_target_table)), child_cte_name(std::move(_child_cte_name)),
	      action_type(conflict_action_type) {
	}
	// Functions.
	string ToQuery() override;
};

/// Node for update queries. Cannot be a CTE. (Not yet implemented.)
class UpdateNode : public RootNode {
public:
	~UpdateNode() override = default;
};

/// Node for deletion queries. Cannot be a CTE. (Not yet implemented.)
class DeleteNode : public RootNode {
public:
	~DeleteNode() override = default;
};

/// Virtual class for intermediate CTE nodes. Each becomes a WITH clause.
class CteNode : public CteBaseNode {
public:
	~CteNode() override = default;
	// Explicitly delete copy constructor to avoid issues.
	CteNode(const CteNode &) = delete;
	CteNode &operator=(const CteNode &) = delete;
	// Constructor.
	explicit CteNode(const size_t index, string name, vector<string> col_list)
	    : CteBaseNode(index), cte_name(std::move(name)), cte_column_list(std::move(col_list)) {
	}
	// Requires ToQuery() to be implemented by derived classes.
	/// Create a CTE-like string for the Node (excluding the WITH keyword).
	/// Example output: "scan_0(t0_name, t0_age) AS (SELECT name, age FROM ...)"
	string ToCteQuery();
	// Attributes.
	/// The name of the CTE (e.g. "scan_0", "filter_1").
	string cte_name;
	/// The "external" names of the CTE columns (the names ancestors use to reference them).
	vector<string> cte_column_list;
};

class GetNode : public CteNode {
	// Attributes.
	string catalog;
	string schema;
	string table_name;
	size_t table_index;
	vector<string> table_filters;
	vector<string> column_names;

public:
	~GetNode() override = default;
	// Constructor.
	explicit GetNode(const size_t index, vector<string> cte_column_names, string _catalog, string _schema,
	                 string _table_name, const size_t _table_index, vector<string> _table_filters,
	                 vector<string> _column_names)
	    : CteNode(index, "scan_" + std::to_string(index), std::move(cte_column_names)), catalog(std::move(_catalog)),
	      schema(std::move(_schema)), table_name(std::move(_table_name)), table_index(_table_index),
	      table_filters(std::move(_table_filters)), column_names(std::move(_column_names)) {
	}
	// Functions.
	string ToQuery() override;
};

class FilterNode : public CteNode {
	// Attributes.
	string child_cte_name;
	vector<string> conditions;

public:
	~FilterNode() override = default;
	// Constructor.
	FilterNode(const size_t index, vector<string> cte_column_names, string _child_cte_name, vector<string> _conditions)
	    : CteNode(index, "filter_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)), conditions(std::move(_conditions)) {
	}
	// Functions.
	string ToQuery() override;
};

class ProjectNode : public CteNode {
	// Attributes.
	string child_cte_name;
	vector<string> column_names;
	size_t table_index;

public:
	~ProjectNode() override = default;
	// Constructor.
	ProjectNode(const size_t index, vector<string> cte_column_names, string _child_cte_name,
	            vector<string> _column_names, const size_t _table_index)
	    : CteNode(index, "projection_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)), column_names(std::move(_column_names)),
	      table_index(_table_index) {
	}
	// Functions.
	string ToQuery() override;
};

class AggregateNode : public CteNode {
	// Attributes.
	string child_cte_name;
	vector<string> group_by_columns; // If empty, is scalar aggregate (e.g. count(*) with no GROUP BY).
	vector<string> aggregate_expressions;

public:
	~AggregateNode() override = default;
	// Constructor.
	AggregateNode(const size_t index, vector<string> cte_column_names, string _child_cte_name,
	              vector<string> _group_names, vector<string> _aggregate_names)
	    : CteNode(index, "aggregate_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)), group_by_columns(std::move(_group_names)),
	      aggregate_expressions(std::move(_aggregate_names)) {
	}
	// Functions.
	string ToQuery() override;
};

class JoinNode : public CteNode {
	// Attributes.
	string left_cte_name, right_cte_name;
	JoinType join_type;
	vector<string> join_conditions;
	string mark_expression; ///< For MARK→LEFT conversion: computed boolean column expression.

public:
	~JoinNode() override = default;
	// Constructor.
	JoinNode(const size_t index, vector<string> cte_column_names, string _left_cte_name, string _right_cte_name,
	         JoinType _join_type, vector<string> _join_conditions, string _mark_expression = "")
	    : CteNode(index, "join_" + std::to_string(index), std::move(cte_column_names)),
	      left_cte_name(std::move(_left_cte_name)), right_cte_name(std::move(_right_cte_name)), join_type(_join_type),
	      join_conditions(std::move(_join_conditions)), mark_expression(std::move(_mark_expression)) {
	}
	// Functions.
	string ToQuery() override;
};

class UnionNode : public CteNode {
	// Attributes.
	string left_cte_name;
	string right_cte_name;
	const bool is_union_all; // Whether to use "UNION ALL" or just "UNION".
public:
	~UnionNode() override = default;
	// Constructor.
	UnionNode(const size_t index, vector<string> cte_column_names, string _left_cte_name, string _right_cte_name,
	          const bool union_all)
	    : CteNode(index, "union_" + std::to_string(index), std::move(cte_column_names)),
	      left_cte_name(std::move(_left_cte_name)), right_cte_name(std::move(_right_cte_name)),
	      is_union_all(union_all) {
	}
	// Functions.
	string ToQuery() override;
};

class ExceptNode : public CteNode {
	// Attributes.
	string left_cte_name;
	string right_cte_name;
	const bool is_except_all; // Whether to use "EXCEPT ALL" or just "EXCEPT".
public:
	~ExceptNode() override = default;
	// Constructor.
	ExceptNode(const size_t index, vector<string> cte_column_names, string _left_cte_name, string _right_cte_name,
	           const bool except_all)
	    : CteNode(index, "except_" + std::to_string(index), std::move(cte_column_names)),
	      left_cte_name(std::move(_left_cte_name)), right_cte_name(std::move(_right_cte_name)),
	      is_except_all(except_all) {
	}
	// Functions.
	string ToQuery() override;
};

/// ORDER BY node — wraps the child CTE with an ORDER BY clause.
class OrderNode : public CteNode {
	string child_cte_name;
	vector<string> order_items; ///< e.g. "t1_age DESC", "t0_name ASC"
public:
	~OrderNode() override = default;
	OrderNode(const size_t index, vector<string> cte_column_names, string _child_cte_name, vector<string> _order_items)
	    : CteNode(index, "order_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)), order_items(std::move(_order_items)) {
	}
	string ToQuery() override;
};

/// LIMIT / OFFSET node — wraps the child CTE with LIMIT and optional OFFSET.
class LimitNode : public CteNode {
	string child_cte_name;
	string limit_str;  ///< e.g. "10" or "" if no LIMIT
	string offset_str; ///< e.g. "5"  or "" if no OFFSET
public:
	~LimitNode() override = default;
	LimitNode(const size_t index, vector<string> cte_column_names, string _child_cte_name, string _limit_str,
	          string _offset_str)
	    : CteNode(index, "limit_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)), limit_str(std::move(_limit_str)),
	      offset_str(std::move(_offset_str)) {
	}
	string ToQuery() override;
};

/// DISTINCT node — wraps the child CTE with SELECT DISTINCT.
class DistinctNode : public CteNode {
	string child_cte_name;

public:
	~DistinctNode() override = default;
	DistinctNode(const size_t index, vector<string> cte_column_names, string _child_cte_name)
	    : CteNode(index, "distinct_" + std::to_string(index), std::move(cte_column_names)),
	      child_cte_name(std::move(_child_cte_name)) {
	}
	string ToQuery() override;
};

/// DELIM_GET node — SELECT DISTINCT of correlated columns from the outer query CTE.
/// Generated as part of DELIM_JOIN decorrelation. Produces distinct correlation key values
/// that are fed into the inner subquery, enabling a non-correlated join.
class DelimGetNode : public CteNode {
	string source_cte_name;     ///< The left-side (outer) CTE to SELECT DISTINCT from.
	vector<string> source_cols; ///< Column names to project from source_cte (e.g. "t3_p_partkey").

public:
	~DelimGetNode() override = default;
	DelimGetNode(const size_t index, vector<string> cte_column_names, string _source_cte_name,
	             vector<string> _source_cols)
	    : CteNode(index, "scan_" + std::to_string(index), std::move(cte_column_names)),
	      source_cte_name(std::move(_source_cte_name)), source_cols(std::move(_source_cols)) {
	}
	string ToQuery() override;
};

/// The complete CTE list: an ordered list of CTE nodes + one final (root) node.
/// Calling ToQuery() serializes the whole thing into a single SQL string.
class CteList {
	// Attributes.
	vector<unique_ptr<CteNode>> nodes; ///< Ordered list of CTEs (leaf-to-root).
	unique_ptr<RootNode> final_node;   ///< The closing statement (SELECT or INSERT).

public:
	// Constructor.
	CteList(vector<unique_ptr<CteNode>> _nodes, unique_ptr<RootNode> _final_node)
	    : nodes(std::move(_nodes)), final_node(std::move(_final_node)) {
	}
	/// Serialize the CTE list into a SQL query string.
	/// If `use_newlines` is true, the string uses newlines between CTEs for readability.
	/// If `output_names` is provided, use them for the final SELECT column aliases
	/// (overriding the default CTE-derived names).
	string ToQuery(bool use_newlines, const vector<string> &output_names = {});
};

} // namespace duckdb
