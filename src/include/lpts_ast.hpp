#pragma once

#include "duckdb.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace duckdb {

//==============================================================================
// AST Node Hierarchy
//==============================================================================
//
// The AST is a dialect-agnostic intermediate representation between DuckDB's
// LogicalOperator tree and the flat CTE list used for SQL generation.
//
// Pipeline: Logical Plan → AST → CTE List → SQL String
//
// Each AST node captures the semantic content of a query operation (table name,
// column names, expressions, join type, etc.) without committing to any SQL
// dialect. The CTE serialization layer is where dialect-specific SQL keywords
// get applied.
//
// Class hierarchy:
//   AstNode (abstract base)
//   ├── AstGetNode        — table scan       (fully implemented)
//   ├── AstFilterNode     — WHERE clause     (skeleton + TODO)
//   ├── AstProjectNode    — column selection  (skeleton + TODO)
//   ├── AstAggregateNode  — GROUP BY         (skeleton + TODO)
//   ├── AstJoinNode       — JOIN             (skeleton + TODO)
//   ├── AstUnionNode      — UNION            (skeleton + TODO)
//   └── AstInsertNode     — INSERT INTO      (skeleton + TODO)
//==============================================================================

/// Abstract base class for all AST nodes.
class AstNode {
public:
	virtual ~AstNode() = default;

	/// Child nodes forming the tree structure.
	vector<unique_ptr<AstNode>> children;

	/// Pretty-print this node (and its subtree) with indentation.
	virtual string ToString(int indent = 0) const = 0;

	/// Return the type name of this node (e.g. "Get", "Filter").
	virtual string NodeType() const = 0;

	/// Return key-value pairs for box-rendered extra info (used by RenderAstTree).
	virtual InsertionOrderPreservingMap<string> GetExtraInfo() const;

protected:
	/// Helper: produce an indentation string of `indent` spaces.
	static string Indent(int indent);
};

/// Table scan node. Fully implemented as a worked example.
class AstGetNode : public AstNode {
public:
	string catalog;
	string schema;
	string table_name;
	size_t table_index;
	vector<string> column_names;     ///< Physical column names (e.g. "age", "name").
	vector<string> cte_column_names; ///< CTE-scoped names (e.g. "t0_age", "t0_name").
	vector<string> table_filters;    ///< Pushdown filters (as SQL strings).

	AstGetNode(string catalog, string schema, string table_name, size_t table_index, vector<string> column_names,
	           vector<string> cte_column_names, vector<string> table_filters)
	    : catalog(std::move(catalog)), schema(std::move(schema)), table_name(std::move(table_name)),
	      table_index(table_index), column_names(std::move(column_names)),
	      cte_column_names(std::move(cte_column_names)), table_filters(std::move(table_filters)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Get";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// WHERE clause node.
class AstFilterNode : public AstNode {
public:
	vector<string> conditions; ///< Filter expressions (e.g. "age > 25").

	explicit AstFilterNode(vector<string> conditions) : conditions(std::move(conditions)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Filter";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// Column selection / expression node.
class AstProjectNode : public AstNode {
public:
	vector<string> expressions;      ///< Projected expressions / column references (child CTE names).
	vector<string> cte_column_names; ///< CTE-scoped output names (e.g. "t1_name").
	size_t table_index;

	AstProjectNode(vector<string> expressions, vector<string> cte_column_names, size_t table_index)
	    : expressions(std::move(expressions)), cte_column_names(std::move(cte_column_names)), table_index(table_index) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Project";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// GROUP BY + aggregate functions node.
class AstAggregateNode : public AstNode {
public:
	vector<string> group_by_columns;      ///< GROUP BY column references (child CTE names).
	vector<string> aggregate_expressions; ///< Aggregate function strings (e.g. "sum(t0_amount)").
	vector<string> cte_column_names;      ///< CTE-scoped output names (e.g. "t2_region", "t3_aggregate_0").

	AstAggregateNode(vector<string> group_by_columns, vector<string> aggregate_expressions,
	                 vector<string> cte_column_names)
	    : group_by_columns(std::move(group_by_columns)), aggregate_expressions(std::move(aggregate_expressions)),
	      cte_column_names(std::move(cte_column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Aggregate";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// JOIN node.
class AstJoinNode : public AstNode {
public:
	JoinType join_type;
	vector<string> conditions;       ///< Join conditions as strings (e.g. "(t0_id = t1_user_id)").
	vector<string> cte_column_names; ///< All output column names (left ++ right + mark if MARK join).
	string mark_expression;          ///< For MARK→LEFT conversion: "(rhs_key IS NOT NULL)" expression.

	AstJoinNode(JoinType join_type, vector<string> conditions, vector<string> cte_column_names,
	            string mark_expression = "")
	    : join_type(join_type), conditions(std::move(conditions)), cte_column_names(std::move(cte_column_names)),
	      mark_expression(std::move(mark_expression)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Join";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// UNION / UNION ALL node.
class AstUnionNode : public AstNode {
public:
	bool is_union_all;
	vector<string> cte_column_names; ///< Output column names derived from the left-hand side.

	AstUnionNode(bool is_union_all, vector<string> cte_column_names)
	    : is_union_all(is_union_all), cte_column_names(std::move(cte_column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Union";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// INSERT INTO node.
class AstInsertNode : public AstNode {
public:
	string target_table;
	OnConflictAction action_type;

	AstInsertNode(string target_table, OnConflictAction action_type)
	    : target_table(std::move(target_table)), action_type(action_type) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Insert";
	}
	InsertionOrderPreservingMap<string> GetExtraInfo() const override;
};

/// ORDER BY node — wraps child with ORDER BY clause.
class AstOrderNode : public AstNode {
public:
	vector<string> order_items;      ///< e.g. "t1_age DESC", "t0_name ASC"
	vector<string> cte_column_names; ///< passthrough from child.

	AstOrderNode(vector<string> order_items, vector<string> cte_column_names)
	    : order_items(std::move(order_items)), cte_column_names(std::move(cte_column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Order";
	}
};

/// LIMIT / OFFSET node.
class AstLimitNode : public AstNode {
public:
	string limit_str;                ///< e.g. "10", or empty if no LIMIT.
	string offset_str;               ///< e.g. "5", or empty if no OFFSET.
	vector<string> cte_column_names; ///< passthrough from child.

	AstLimitNode(string limit_str, string offset_str, vector<string> cte_column_names)
	    : limit_str(std::move(limit_str)), offset_str(std::move(offset_str)),
	      cte_column_names(std::move(cte_column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Limit";
	}
};

/// SELECT DISTINCT node.
class AstDistinctNode : public AstNode {
public:
	vector<string> cte_column_names; ///< passthrough from child.

	explicit AstDistinctNode(vector<string> cte_column_names) : cte_column_names(std::move(cte_column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Distinct";
	}
};

/// Materialized CTE node: wraps a CTE body + outer query.
/// children[0] = CTE body AST, children[1] = outer query AST.
/// Used to correctly order Phase-2 flattening so CTE_REF nodes can look up the body CTE name.
class AstMaterializedCteNode : public AstNode {
public:
	idx_t cte_table_index; ///< table_index from LogicalMaterializedCTE (key for CTE_REF lookup).

	explicit AstMaterializedCteNode(idx_t cte_table_index_p) : cte_table_index(cte_table_index_p) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "MaterializedCte";
	}
};

/// CTE reference node: a scan of a previously-defined materialized CTE body.
/// Has no children. The body CTE name is resolved in Phase 2 via cte_table_index.
class AstCteRefNode : public AstNode {
public:
	idx_t cte_table_index;           ///< Matches AstMaterializedCteNode::cte_table_index.
	vector<string> cte_column_names; ///< Output column names for this CTE scan (e.g. "tN_supplier_no").

	AstCteRefNode(idx_t cte_table_index_p, vector<string> cte_column_names_p)
	    : cte_table_index(cte_table_index_p), cte_column_names(std::move(cte_column_names_p)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "CteRef";
	}
};

/// DELIM_GET node: a duplicate-eliminated scan driven by a parent DELIM_JOIN.
/// In SQL: SELECT DISTINCT {source_col_names} FROM {left_cte}.
/// Has no children. The source CTE name is registered by the parent AstDelimJoinNode in Phase 2.
class AstDelimGetNode : public AstNode {
public:
	idx_t table_index;               ///< Unique table index for this DELIM_GET.
	vector<string> cte_column_names; ///< Exposed column names (e.g. "tN_p_partkey").
	vector<string> source_col_names; ///< Column names to SELECT from the left CTE (e.g. "t3_p_partkey").

	AstDelimGetNode(idx_t table_index_p, vector<string> cte_column_names_p, vector<string> source_col_names_p)
	    : table_index(table_index_p), cte_column_names(std::move(cte_column_names_p)),
	      source_col_names(std::move(source_col_names_p)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "DelimGet";
	}
};

/// DELIM_JOIN node: a duplicate-eliminating join used to decorrelate subqueries.
/// children[0] = outer (left) subtree, children[1] = inner (right) subtree containing AstDelimGetNode(s).
/// In Phase 2, flattened as: left CTEs, then DELIM_GET CTEs (SELECT DISTINCT from left), then right CTEs, then JOIN.
class AstDelimJoinNode : public AstNode {
public:
	JoinType join_type;
	vector<string> conditions;         ///< Join conditions as strings.
	vector<string> cte_column_names;   ///< Output column names.
	vector<idx_t> delim_table_indices; ///< table_indices of ALL AstDelimGetNodes in the right subtree.

	string mark_expression; ///< For MARK→LEFT conversion: "(rhs_key IS NOT NULL)" or empty.

	AstDelimJoinNode(JoinType join_type_p, vector<string> conditions_p, vector<string> cte_column_names_p,
	                 vector<idx_t> delim_table_indices_p, string mark_expression_p = "")
	    : join_type(join_type_p), conditions(std::move(conditions_p)), cte_column_names(std::move(cte_column_names_p)),
	      delim_table_indices(std::move(delim_table_indices_p)), mark_expression(std::move(mark_expression_p)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "DelimJoin";
	}
};

/// Pretty-print the full AST tree starting from the given root node.
string PrintAst(const AstNode &root);

} // namespace duckdb
