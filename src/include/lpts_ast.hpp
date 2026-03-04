#pragma once

#include "duckdb.hpp"

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
	vector<string> column_names;

	AstGetNode(string catalog, string schema, string table_name, size_t table_index, vector<string> column_names)
	    : catalog(std::move(catalog)), schema(std::move(schema)), table_name(std::move(table_name)),
	      table_index(table_index), column_names(std::move(column_names)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Get";
	}
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
};

/// Column selection / expression node.
class AstProjectNode : public AstNode {
public:
	vector<string> expressions; ///< Projected expressions or column references.
	size_t table_index;

	AstProjectNode(vector<string> expressions, size_t table_index)
	    : expressions(std::move(expressions)), table_index(table_index) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Project";
	}
};

/// GROUP BY + aggregate functions node.
class AstAggregateNode : public AstNode {
public:
	vector<string> group_by_columns;      ///< GROUP BY column references.
	vector<string> aggregate_expressions; ///< Aggregate expressions (e.g. "sum(amount)").

	AstAggregateNode(vector<string> group_by_columns, vector<string> aggregate_expressions)
	    : group_by_columns(std::move(group_by_columns)), aggregate_expressions(std::move(aggregate_expressions)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Aggregate";
	}
};

/// JOIN node.
class AstJoinNode : public AstNode {
public:
	JoinType join_type;
	vector<string> conditions; ///< Join conditions (e.g. "a.id = b.id").

	AstJoinNode(JoinType join_type, vector<string> conditions)
	    : join_type(join_type), conditions(std::move(conditions)) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Join";
	}
};

/// UNION / UNION ALL node.
class AstUnionNode : public AstNode {
public:
	bool is_union_all;

	explicit AstUnionNode(bool is_union_all) : is_union_all(is_union_all) {
	}

	string ToString(int indent = 0) const override;
	string NodeType() const override {
		return "Union";
	}
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
};

/// Pretty-print the full AST tree starting from the given root node.
string PrintAst(const AstNode &root);

} // namespace duckdb
