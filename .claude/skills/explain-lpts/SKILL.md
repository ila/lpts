---
name: explain-lpts
description: Comprehensive reference for the LPTS pipeline — LogicalPlan to AST to CTE List to SQL string. Covers AST node hierarchy, CTE node hierarchy, column binding maps, expression resolution, and operator-specific conversion logic. Auto-loaded when discussing the pipeline, AST nodes, CTE generation, or operator handling.
---

# LPTS Pipeline: Comprehensive Reference

## 1. Overview

LPTS (**L**ogical **P**lan **T**o **S**QL) converts DuckDB's internal logical plan for a SQL
query into an equivalent SQL string made of CTEs (Common Table Expressions). Each CTE
corresponds to one operator from the plan.

The pipeline has three phases:

```
Logical Plan  →  AST  →  CTE List  →  SQL String
```

Phase 1 (`LogicalPlanToAst`) walks DuckDB's `LogicalOperator` tree and builds a
dialect-agnostic AST. Phase 2 (`AstToCteList`) flattens the AST into an ordered list
of `CteNode` objects. Phase 3 (`CteList::ToQuery`) serializes the flat list into SQL.

## 2. Phase 1: LogicalPlanToAst

**File:** `src/lpts_pipeline.cpp`
**Signature:** `unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan)`

Walks the `LogicalOperator` tree recursively. For each node:
1. Recursively process children first (bottom-up)
2. Extract operator-specific data (table name, column bindings, expressions, join type, etc.)
3. Create the corresponding `AstNode` and attach children

### 2.1 Column Binding Map

The key bookkeeping challenge is tracking `ColumnBinding` → CTE column name across operators.
A `ColumnBinding` is a `(table_index, column_index)` pair identifying a column in the plan.

Each `AstGetNode` (table scan) establishes the initial mapping: physical column names are
prefixed with `t<table_index>_` to create CTE-scoped names. Downstream operators (filter,
project, aggregate) use `ExpressionToAliasedString()` to resolve `ColumnBinding` references
into CTE column names.

### 2.2 Operator Extraction Logic

The reference implementation is `CreateCteNode()` in `src/logical_plan_to_sql.cpp`. For each
operator type, the extraction pattern is:

**LOGICAL_GET (table scan):**
- Extract catalog, schema, table name from `LogicalGet`
- Extract column names from `get.names` + `get.column_ids`
- Extract table filters from `get.table_filters` (pushdown filters)
- Generate CTE column names: `t<table_index>_<column_name>`

**LOGICAL_FILTER (WHERE clause):**
- Extract filter expressions from `LogicalFilter::expressions`
- Convert each expression to a SQL string using `ExpressionToAliasedString()`
- Passthrough: child CTE column names are inherited

**LOGICAL_PROJECTION (column selection):**
- Extract projected expressions from `LogicalProjection::expressions`
- Each expression becomes a column in the SELECT list
- Generate new CTE column names: `t<table_index>_<name>`

**LOGICAL_AGGREGATE_AND_GROUP_BY:**
- Extract group-by columns from `LogicalAggregate::groups`
- Extract aggregate expressions from `LogicalAggregate::expressions`
- Generate CTE column names for both groups and aggregates

**LOGICAL_COMPARISON_JOIN:**
- Extract join type from `LogicalComparisonJoin::join_type`
- Extract join conditions from `LogicalComparisonJoin::conditions`
- Combine left and right child CTE column names

**LOGICAL_UNION:**
- Extract `is_union_all` flag
- CTE column names derived from the left-hand side

**LOGICAL_INSERT:**
- Extract target table name
- Extract `OnConflictAction` (NOTHING, REPLACE, etc.)

**LOGICAL_ORDER_BY:**
- Extract `BoundOrderByNode` entries (expression + order type)
- Passthrough child CTE column names

**LOGICAL_LIMIT:**
- Extract limit and offset expressions (may be constants or bound expressions)
- Passthrough child CTE column names

**LOGICAL_DISTINCT:**
- Passthrough child CTE column names

### 2.3 Expression Resolution

`ExpressionToAliasedString()` (in `src/logical_plan_to_sql.cpp`) converts DuckDB bound
expressions into SQL strings:

| Expression Type | Example Output |
|---|---|
| `BoundColumnRefExpression` | `t0_age` (resolved via column binding map) |
| `BoundConstantExpression` | `CAST(25 AS INTEGER)` |
| `BoundComparisonExpression` | `(t0_age) > (CAST(25 AS INTEGER))` |
| `BoundFunctionExpression` | `sum(t0_amount)` |
| `BoundCastExpression` | `CAST(t0_val AS VARCHAR)` |
| `BoundReferenceExpression` | Used in aggregates to reference group-by columns |

## 3. AST Node Hierarchy

**File:** `src/include/lpts_ast.hpp`

```
AstNode (abstract base, has vector<unique_ptr<AstNode>> children)
├── AstGetNode        — table scan
│     Fields: catalog, schema, table_name, table_index, column_names,
│             cte_column_names, table_filters
├── AstFilterNode     — WHERE clause
│     Fields: conditions (vector<string>)
├── AstProjectNode    — column selection
│     Fields: expressions, cte_column_names, table_index
├── AstAggregateNode  — GROUP BY + aggregates
│     Fields: group_by_columns, aggregate_expressions, cte_column_names
├── AstJoinNode       — JOIN
│     Fields: join_type, conditions, cte_column_names
├── AstUnionNode      — UNION / UNION ALL
│     Fields: is_union_all, cte_column_names
├── AstInsertNode     — INSERT INTO
│     Fields: target_table, action_type
├── AstOrderNode      — ORDER BY
│     Fields: order_items, cte_column_names
├── AstLimitNode      — LIMIT / OFFSET
│     Fields: limit_str, offset_str, cte_column_names
└── AstDistinctNode   — SELECT DISTINCT
      Fields: cte_column_names
```

Each node has:
- `children` vector — tree structure (mirrors logical plan)
- `ToString(int indent)` — pretty-print for debugging
- `NodeType()` — returns type name string
- `GetExtraInfo()` — key-value pairs for box-rendered tree printer

## 4. Phase 2: AstToCteList

**File:** `src/lpts_pipeline.cpp`
**Signature:** `unique_ptr<CteList> AstToCteList(const AstNode &root, SqlDialect dialect)`

Walks the AST in post-order (children first, then parent). For each `AstNode`:
1. Recursively process children
2. Create the corresponding `CteNode` with an incremental index
3. The root node produces either a `FinalReadNode` (SELECT) or `InsertNode` (INSERT)

### 4.1 CTE Naming Convention

CTEs are named by their operator type and index:
- `scan_0`, `scan_3` — GetNode
- `filter_1` — FilterNode
- `projection_2` — ProjectNode
- `aggregate_4` — AggregateNode
- `join_inner_5` — JoinNode (inner)
- `union_6` — UnionNode
- `order_7` — OrderNode
- `limit_8` — LimitNode
- `distinct_9` — DistinctNode

### 4.2 CTE Node Hierarchy

**File:** `src/include/logical_plan_to_sql.hpp`

```
CteBaseNode (base)
├── RootNode (virtual) — the final statement
│   ├── FinalReadNode — closing SELECT that renames CTE columns back to original names
│   └── InsertNode    — INSERT INTO ... SELECT * FROM <last_cte>
└── CteNode (virtual) — one CTE in the WITH clause
    ├── GetNode        — SELECT col1, col2 FROM table
    ├── FilterNode     — SELECT * FROM <child> WHERE ...
    ├── ProjectNode    — SELECT expr1, expr2 FROM <child>
    ├── AggregateNode  — SELECT group_cols, agg_exprs FROM <child> GROUP BY ...
    ├── JoinNode       — SELECT * FROM <left> JOIN <right> ON ...
    └── UnionNode      — SELECT * FROM <left> UNION [ALL] SELECT * FROM <right>
```

Each CteNode has:
- `ToQuery()` — produces the SQL fragment inside `AS (...)`
- `ToCteQuery()` — wraps with the CTE name and column list
- `cte_name` — the name used in the WITH clause
- `child_cte_name` — reference to the input CTE

## 5. Phase 3: CTE List to SQL

`CteList::ToQuery(true)` serializes all CTEs into a single SQL string:

```sql
WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```

The `FinalReadNode` at the end renames CTE-internal column names back to the
original user-facing column names (e.g., `t1_name AS name`).

## 6. Dialect Support

**Enum:** `SqlDialect` in `src/include/lpts_pipeline.hpp`
**Values:** `DUCKDB` (default), `POSTGRES`

Dialect differences handled in `AstToCteList`:

| Aspect | DuckDB | Postgres |
|---|---|---|
| Table references | `memory.main.users` (fully-qualified) | `users` (unqualified) |
| Date parsing | `strptime(col, '%Y-%m-%d')` | `to_date(col, 'YYYY-MM-DD')` |
| Boolean literals | `true` | `TRUE` |

Set via: `SET lpts_dialect = 'postgres';`

## 7. Key Source Files

| File | Purpose |
|---|---|
| `src/lpts_pipeline.cpp` | `LogicalPlanToAst` and `AstToCteList` implementations |
| `src/logical_plan_to_sql.cpp` | Reference implementation: `CreateCteNode()` is the canonical operator extraction logic |
| `src/include/lpts_ast.hpp` | AST node class hierarchy |
| `src/lpts_ast.cpp` | AST `ToString()` implementations |
| `src/lpts_ast_renderer.cpp` | Box-rendered ASCII tree printer for AST debugging |
| `src/include/lpts_pipeline.hpp` | Pipeline function declarations + `SqlDialect` enum |
| `src/include/logical_plan_to_sql.hpp` | CTE node class hierarchy |
| `src/lpts_helpers.cpp` | Utility functions (`VecToSeparatedList`, `EscapeSingleQuotes`, etc.) |
| `src/include/lpts_debug.hpp` | Debug flag and `LPTS_DEBUG_PRINT` macro |
