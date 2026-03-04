# LPTS Onboarding: Adding the AST Layer

## What is LPTS?

LPTS (**L**ogical **P**lan **T**o **S**QL) is a DuckDB extension that takes a SQL query, reads DuckDB's internal *logical plan* for that query, and converts it back into an equivalent SQL string. The output is a chain of CTEs (Common Table Expressions), where each CTE corresponds to one operator from the plan.

### What is a logical plan?

When DuckDB receives a SQL query, it doesn't execute it directly. It first parses the text into a syntax tree, then *plans* the execution: it figures out which tables to scan, where to apply filters, how to join, etc. The result is a tree of **logical operators**. For example:

```
SQL:  SELECT name FROM users WHERE age > 25

Logical Plan (simplified):

  PROJECTION [name]          ← pick only the "name" column
       │
    FILTER [age > 25]        ← discard rows where age <= 25
       │
    GET [users]              ← scan the "users" table
```

Each node in this tree is a C++ object of type `LogicalOperator`. DuckDB has many subclasses: `LogicalGet`, `LogicalFilter`, `LogicalProjection`, `LogicalAggregate`, `LogicalComparisonJoin`, etc. You can find these in the DuckDB source under `duckdb/planner/operator/`.

### Why convert a plan back to SQL?

There are several use cases:

1. **Readable query plans.** DuckDB has `EXPLAIN`, but its output is terse and aimed at database developers. LPTS produces a readable SQL equivalent that any SQL user can follow — each CTE maps to one operator, so you can trace the execution step by step.

2. **Cross-database migration.** If you have a query that runs on DuckDB and you want to run it on Postgres (or vice versa), you need to translate dialect-specific syntax. If we have a dialect-agnostic intermediate representation, we can produce DuckDB SQL or Postgres SQL from the same structure.

3. **Multi-dialect SQL generation.** A single query can be rendered into different SQL dialects depending on the target database. This is the main motivation for the AST layer you will build.

4. **Query rewriting / forced execution order.** Because the output is a CTE chain that mirrors the plan, it effectively "locks in" an execution order. In rare cases where DuckDB's optimizer makes a poor choice, you could feed the LPTS output back to force a specific operator ordering.

---

## The current pipeline (without AST)

The current code converts a logical plan to SQL in two steps:

```
Logical Plan  →  CTE List  →  SQL String
```

### Entry point: `src/lpts_extension.cpp`

This file registers two user-facing functions:

- **`PRAGMA lpts('query')`** — line 25, `LptsPragmaFunction`. Takes a SQL string, parses it, plans it, and converts the plan to a SQL string.
- **`lpts_query('query')`** — line 67, `LptsTableBind`. Same thing, but as a table function so you can do `SELECT * FROM lpts_query(...)`.

Both functions do the same thing internally:

```cpp
// Parse the user's SQL and build DuckDB's logical plan.
Parser parser;
parser.ParseQuery(query);
Planner planner(context);
planner.CreatePlan(parser.statements[0]->Copy());

// Convert the logical plan to a CTE list, then to a SQL string.
LogicalPlanToSql lpts(context, planner.plan);
auto cte_list = lpts.LogicalPlanToCteList();
string result_sql = cte_list->ToQuery(true);
```

### The converter: `src/logical_plan_to_sql.cpp`

The `LogicalPlanToSql` class (declared in `src/include/logical_plan_to_sql.hpp`) does all the work:

1. **`LogicalPlanToCteList()`** (line 548) — the main entry point. Walks the plan tree bottom-up.
2. **`RecursiveTraversal()`** (line 531) — processes children first, then the current node.
3. **`CreateCteNode()`** (line 298) — a big `switch` on the operator type. For each type (GET, FILTER, PROJECTION, AGGREGATE, JOIN, UNION), it extracts the relevant data and creates the corresponding `CteNode`.
4. **`ExpressionToAliasedString()`** (line 236) — converts DuckDB's bound expressions (comparisons, column refs, casts, etc.) into SQL strings.

### The CTE node hierarchy: `src/include/logical_plan_to_sql.hpp`

The CTE list is made of node objects:

```
CteBaseNode (base)
├── RootNode (virtual) — the final statement
│   ├── FinalReadNode — closing SELECT that renames columns
│   └── InsertNode — INSERT INTO ... SELECT * FROM <cte>
└── CteNode (virtual) — one CTE in the WITH clause
    ├── GetNode        — table scan
    ├── FilterNode     — WHERE clause
    ├── ProjectNode    — column selection
    ├── AggregateNode  — GROUP BY
    ├── JoinNode       — JOIN
    └── UnionNode      — UNION / UNION ALL
```

Each `CteNode` has a `ToQuery()` method that produces the SQL fragment inside `AS (...)`, and a `ToCteQuery()` method that wraps it with the CTE name and column list.

### Worked example

Given:
```sql
SELECT name FROM users WHERE age > 25
```

DuckDB's logical plan (bottom-up):
```
LOGICAL_PROJECTION (table_index=1, expressions=[t0_name])
    LOGICAL_FILTER (expressions=[t0_age > 25])
        LOGICAL_GET (table=users, columns=[age, name])
```

The converter produces this CTE list:
```
CteNode[0]: GetNode      — scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users)
CteNode[1]: FilterNode   — filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER)))
CteNode[2]: ProjectNode  — projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
RootNode:   FinalReadNode — SELECT t1_name AS name FROM projection_2
```

Final SQL:
```sql
WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```

Notice how each logical operator became exactly one CTE, and the bottom-up traversal guarantees that every CTE only references CTEs defined before it.

---

## The new pipeline (with AST)

Your task is to insert an **AST (Abstract Syntax Tree)** layer between the logical plan and the CTE list:

```
Logical Plan  →  AST  →  CTE List  →  SQL String
```

The AST is a **dialect-agnostic** tree. It captures *what* the query does (scan this table, filter by this condition, project these columns) without committing to *how* it is written in SQL. The CTE serialization layer is where dialect-specific keywords get applied.

This means the same AST can produce different SQL output depending on a **dialect flag**:

```
                     ┌──→  CTE List (DuckDB)  →  DuckDB SQL
Logical Plan → AST ──┤
                     └──→  CTE List (Postgres) →  Postgres SQL
```

### What is the AST?

The AST is a tree of C++ node objects. Each node corresponds to one query operation and stores the extracted data (table name, column names, expressions, join type, etc.). Unlike the CTE list, AST nodes have **parent-child pointers** (via `children` vector) — the tree structure mirrors the logical plan.

The AST node hierarchy (defined in `src/include/lpts_ast.hpp`):

```
AstNode (abstract base, has vector<unique_ptr<AstNode>> children)
├── AstGetNode        — table scan
├── AstFilterNode     — WHERE clause
├── AstProjectNode    — column selection
├── AstAggregateNode  — GROUP BY
├── AstJoinNode       — JOIN
├── AstUnionNode      — UNION
└── AstInsertNode     — INSERT INTO
```

Each node has:
- A `children` vector for the tree structure.
- A `ToString(int indent)` method for pretty-printing.
- A `NodeType()` method that returns the type name.

### Worked example (with AST)

Same query: `SELECT name FROM users WHERE age > 25`

**Step 1: Logical Plan → AST** (`LogicalPlanToAst`)

```
AstProjectNode (expressions=["t0_name"], table_index=1)
  └── AstFilterNode (conditions=["(t0_age) > (CAST(25 AS INTEGER))"])
        └── AstGetNode (memory.main.users, table_index=0, columns=[age, name])
```

The AST mirrors the logical plan structure as a tree. It stores the same data that `CreateCteNode()` currently extracts, but in a tree rather than a flat list.

**Step 2: AST → CTE List** (`AstToCteList`)

Flatten the tree into an ordered list of CTEs by walking it bottom-up (post-order traversal). Each `AstNode` becomes a `CteNode`. The same CTE list as before is produced.

**Step 3: CTE List → SQL String** (unchanged)

The existing `CteList::ToQuery()` serializes it. This step is already implemented and does not change.

### The pipeline functions: `src/include/lpts_pipeline.hpp`

Two functions are declared:

```cpp
/// Phase 1: Convert a DuckDB LogicalOperator tree into a dialect-agnostic AST.
unique_ptr<AstNode> LogicalPlanToAst(ClientContext &context, unique_ptr<LogicalOperator> &plan);

/// Phase 2: Convert an AST into a flat CTE list (future: dialect-aware).
unique_ptr<CteList> AstToCteList(const AstNode &root);
```

Both currently throw `NotImplementedException`. Their stub implementations are in `src/lpts_pipeline.cpp`.

---

## Your tasks

### Task 1: Implement `LogicalPlanToAst` in `src/lpts_pipeline.cpp`

Your task is to walk the `LogicalOperator` tree and build the corresponding AST.

Use the existing `CreateCteNode()` method in `src/logical_plan_to_sql.cpp` (line 298) as your reference. That method already extracts the right data for each operator type — your job is to put that data into `AstNode` objects instead of `CteNode` objects, and to preserve the tree structure (via `children`) instead of flattening into a list.

Concretely, for each `LogicalOperator` type:
- `LOGICAL_GET` → create an `AstGetNode` with catalog, schema, table name, table index, and column names.
- `LOGICAL_FILTER` → create an `AstFilterNode` with the filter conditions.
- `LOGICAL_PROJECTION` → create an `AstProjectNode` with the projected expressions and table index.
- `LOGICAL_AGGREGATE_AND_GROUP_BY` → create an `AstAggregateNode` with group-by columns and aggregate expressions.
- `LOGICAL_COMPARISON_JOIN` → create an `AstJoinNode` with join type and conditions.
- `LOGICAL_UNION` → create an `AstUnionNode` with the `is_union_all` flag.
- `LOGICAL_INSERT` → create an `AstInsertNode` with target table and conflict action.

Recursively process children first, then attach them to the current node's `children` vector.

You will also need to handle the `column_map` bookkeeping (tracking column bindings across operators) — look at how `CreateCteNode()` does this.

### Task 2: Implement `AstToCteList` in `src/lpts_pipeline.cpp`

Your task is to flatten an AST tree into a `CteList`.

Walk the AST in post-order (children first, then parent). For each `AstNode`, create the corresponding `CteNode` and assign it an incremental index. The root node should produce either a `FinalReadNode` (for SELECT queries) or an `InsertNode` (for INSERT queries).

The output should be identical to what `LogicalPlanToSql::LogicalPlanToCteList()` currently produces — use its tests to verify (see `test/sql/`).

### Task 3: Wire the new pipeline into `src/lpts_extension.cpp`

Once both functions are working, update the entry points (`LptsPragmaFunction` and `LptsTableBind`) to use the new pipeline:

```cpp
// Old:
LogicalPlanToSql lpts(context, planner.plan);
auto cte_list = lpts.LogicalPlanToCteList();

// New:
auto ast = LogicalPlanToAst(context, planner.plan);
auto cte_list = AstToCteList(*ast);
```

The existing tests in `test/sql/` must still pass after this change.

### Task 4: Add a dialect setting to `src/lpts_extension.cpp`

Add a SQL dialect enum and a way for the user to select a dialect. The target dialects are **DuckDB** and **Postgres**.

Your task is to:

1. Define an enum (e.g., `enum class SqlDialect { DUCKDB, POSTGRES };`) — you can put this in `lpts_pipeline.hpp` or a new header.

2. Register a DuckDB setting (using `ExtensionLoader`) that lets users choose the dialect, for example:
   ```sql
   SET lpts_dialect = 'postgres';
   ```

3. Pass the dialect to `AstToCteList` (update its signature to accept a `SqlDialect` parameter). For now, the function can ignore the dialect and always produce DuckDB SQL — the point is to have the plumbing in place.

4. Eventually, `AstToCteList` should produce dialect-specific SQL (e.g., Postgres does not support `CAST(25 AS INTEGER)` in the same way, `memory.main.users` should just be `users`, etc.). You do not need to implement all dialect differences right now — just handle one or two differences to prove the concept works.

### Task 5: Flesh out AST `ToString()` methods

The `AstGetNode::ToString()` in `src/lpts_ast.cpp` is fully implemented as a reference. The other node types have skeleton implementations. Your task is to make them print useful information (the actual expressions, conditions, column names, etc.) so that `PrintAst()` produces a readable tree. This is useful for debugging.

---

## Building and testing

```bash
# Build (from project root):
make release

# Run tests:
make test_release

# Try it interactively:
./build/release/duckdb
D CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
D PRAGMA lpts('SELECT name FROM users WHERE age > 25');
```

The existing tests are in `test/sql/select.test`, `test/sql/group_by.test`, and `test/sql/join.test`. They use the `lpts_query()` table function and compare output strings. Your changes must keep these passing.

---

## File map

| File | Purpose |
|---|---|
| `src/lpts_extension.cpp` | Extension entry point. Registers `PRAGMA lpts` and `lpts_query`. |
| `src/logical_plan_to_sql.cpp` | Current converter: logical plan → CTE list → SQL. |
| `src/include/logical_plan_to_sql.hpp` | CTE node class hierarchy + `LogicalPlanToSql` class. |
| `src/include/lpts_ast.hpp` | **New.** AST node class hierarchy. |
| `src/lpts_ast.cpp` | **New.** AST `ToString()` implementations. |
| `src/include/lpts_pipeline.hpp` | **New.** Pipeline function declarations (`LogicalPlanToAst`, `AstToCteList`). |
| `src/lpts_pipeline.cpp` | **New.** Pipeline function stubs (your main work goes here). |
| `src/lpts_helpers.cpp` | Utility functions (`VecToSeparatedList`, `EscapeSingleQuotes`, etc.). |
| `src/include/lpts_helpers.hpp` | Utility function declarations. |
| `test/sql/*.test` | SQL logic tests — must keep passing. |
| `CMakeLists.txt` | Build config — already includes the new source files. |
