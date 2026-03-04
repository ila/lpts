# LPTS Onboarding: Adding the AST Layer

## What is LPTS?

LPTS (**L**ogical **P**lan **T**o **S**QL) is a [DuckDB extension](https://duckdb.org/docs/extensions/overview) that takes a SQL query, reads DuckDB's internal *logical plan* for that query, and converts it back into an equivalent SQL string made of CTEs (Common Table Expressions).

```
D CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
D PRAGMA lpts('SELECT name FROM users WHERE age > 25');
┌──────────────────────────────────────────────────────────────────────────────┐
│ sql                                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),  │
│ filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),  │
│ projection_2(t1_name) AS (SELECT t0_name FROM filter_1)                     │
│ SELECT t1_name AS name FROM projection_2;                                    │
└──────────────────────────────────────────────────────────────────────────────┘
```

Each CTE corresponds to one operator from the plan. You can read the output top-to-bottom and understand exactly what DuckDB does: scan the table, filter rows, pick columns.

## How is this useful?

1. **Readable query plans.** DuckDB has `EXPLAIN`, but its output is aimed at database developers. LPTS produces a readable SQL equivalent that any SQL user can follow — each CTE maps to one operator, so you can trace the execution step by step.

2. **Cross-database migration.** A query that runs on DuckDB may need different syntax to run on Postgres. If we have a dialect-agnostic intermediate representation, we can produce DuckDB SQL or Postgres SQL from the same structure. For example:

   | | DuckDB | Postgres |
   |---|---|---|
   | Table reference | `memory.main.users` | `users` |
   | Date parsing | `strptime('2024-01-01', '%Y-%m-%d')` | `to_date('2024-01-01', 'YYYY-MM-DD')` |
   | Boolean literal | `true` | `TRUE` |
   | List type | `INTEGER[]` | `integer[]` |

3. **Multi-dialect SQL generation.** This is the main motivation for the AST layer you will build — a single AST can be serialized into different SQL dialects depending on a dialect flag.

4. **Forced execution order.** Because the CTE chain mirrors the plan, it "locks in" an operator ordering. In rare cases where a database's optimizer makes a poor choice, you could feed the LPTS output back to force a specific execution order.

### Why CTEs?

We use CTEs (not subqueries) because they produce a **flat, readable list**, easy to compile. Each operator becomes one named CTE, and later CTEs reference earlier ones by name. Subqueries would nest deeply and become unreadable. The CTE chain also maps 1:1 to the logical plan, making it easy to verify correctness.

---

## Background: DuckDB logical plans

When DuckDB receives a SQL query, it doesn't execute it directly. It first parses the text, then *plans* the execution: it figures out which tables to scan, where to apply filters, how to join, etc. The result is a tree of **logical operators**:

```
SQL:  SELECT name FROM users WHERE age > 25

Logical Plan (simplified):

  PROJECTION [name]          ← pick only the "name" column
       │
    FILTER [age > 25]        ← discard rows where age <= 25
       │
    GET [users]              ← scan the "users" table
```

In C++, each node is a subclass of [`LogicalOperator`](https://duckdb.org/docs/dev/building/overview). DuckDB has many: `LogicalGet`, `LogicalFilter`, `LogicalProjection`, `LogicalAggregate`, `LogicalComparisonJoin`, etc. You can find them in the DuckDB source under `duckdb/planner/operator/`.

The DuckDB C++ API types you will encounter most:

| Type | What it is |
|---|---|
| `LogicalOperator` | Base class for plan nodes. Has a `type` enum and a `children` vector. |
| `ColumnBinding` | A `(table_index, column_index)` pair identifying a column in the plan. |
| `Expression` | Base class for expressions (comparisons, column refs, constants, casts). |
| `ClientContext` | DuckDB's session state — passed around for catalog lookups. |
| `unique_ptr<T>` | DuckDB uses `std::unique_ptr` everywhere for ownership. |
| `make_uniq<T>(...)` | DuckDB's equivalent of `std::make_unique`. |

See also: [DuckDB Extension Development](https://duckdb.org/docs/extensions/overview), [DuckDB C++ API](https://duckdb.org/docs/api/cpp/overview).

---

## The current pipeline

```
Logical Plan  →  CTE List  →  SQL String
```

### Entry points: `src/lpts_extension.cpp`

Two user-facing functions are registered, both doing the same thing internally:

**`PRAGMA lpts('query')`** (line 25) — for interactive use:
```
D PRAGMA lpts('SELECT * FROM users');
```

**`lpts_query('query')`** (line 67) — table function for programmatic use and tests:
```
D SELECT * FROM lpts_query('SELECT * FROM users');
┌──────────────────────────────────────────────────────────────────────────────┐
│ sql                                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ WITH scan_0 (t0_id, t0_name, t0_age) AS (SELECT id, name, age FROM ...),   │
│ ...                                                                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

Both call the same C++ code:

```cpp
// 1. Parse the user's SQL and build DuckDB's logical plan.
Parser parser;                                    // DuckDB's SQL parser
parser.ParseQuery(query);                         // parse the string into a statement
Planner planner(context);                         // the planner needs the session context
planner.CreatePlan(parser.statements[0]->Copy()); // build the LogicalOperator tree
// planner.plan is now a unique_ptr<LogicalOperator> — the root of the plan tree.

// 2. Convert the logical plan to a CTE list, then to a SQL string.
LogicalPlanToSql lpts(context, planner.plan);     // create the converter
auto cte_list = lpts.LogicalPlanToCteList();      // walk the plan → flat CTE list
string result_sql = cte_list->ToQuery(true);      // serialize CTEs → SQL string
```

### The converter: `src/logical_plan_to_sql.cpp`

The `LogicalPlanToSql` class (declared in `src/include/logical_plan_to_sql.hpp`) does all the work:

1. **`LogicalPlanToCteList()`** (line 548) — main entry point. Walks the plan tree bottom-up.
2. **`RecursiveTraversal()`** (line 531) — processes children first, then the current node.
3. **`CreateCteNode()`** (line 298) — a big `switch` on the operator type. For each type (GET, FILTER, PROJECTION, AGGREGATE, JOIN, UNION), it extracts the relevant data and creates the corresponding `CteNode`.
4. **`ExpressionToAliasedString()`** (line 236) — converts DuckDB's bound expressions (comparisons, column refs, casts) into SQL strings, replacing internal `ColumnBinding` references with CTE column names.

### The CTE node hierarchy: `src/include/logical_plan_to_sql.hpp`

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

The converter walks this bottom-up and produces a flat CTE list:
```
CteNode[0]: GetNode      → scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users)
CteNode[1]: FilterNode   → filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER)))
CteNode[2]: ProjectNode  → projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
RootNode:   FinalReadNode → SELECT t1_name AS name FROM projection_2
```

Final SQL:
```sql
WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```

Each logical operator became exactly one CTE. The bottom-up traversal guarantees that every CTE only references CTEs defined before it.

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

The AST is a tree of C++ node objects (defined in `src/include/lpts_ast.hpp`). Each node corresponds to one query operation and stores the extracted data. Unlike the flat CTE list, AST nodes have **parent-child pointers** via a `children` vector — the tree structure mirrors the logical plan.

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
- A `ToString(int indent)` method for pretty-printing (useful for debugging).
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

Flatten the tree into an ordered list of CTEs by walking it bottom-up (post-order traversal). Each `AstNode` becomes a `CteNode`. The same CTE list as before is produced:

```
scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```

**Step 3: CTE List → SQL String** (unchanged, already implemented)

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

You can implement these **incrementally**. You do not need to support all operator types at once — start with GET + PROJECTION (enough for `SELECT * FROM table`), verify it works, then add FILTER, then AGGREGATE, etc. The existing tests in `test/sql/` will tell you which operators each test needs.

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

The output should be identical to what `LogicalPlanToSql::LogicalPlanToCteList()` currently produces — use the tests to verify.

### Task 3: Wire the new pipeline into `src/lpts_extension.cpp`

Update the entry points (`LptsPragmaFunction` and `LptsTableBind`) to use the new pipeline:

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

1. Define an enum `SqlDialect` — you can put this in `lpts_pipeline.hpp` or a new header.

2. Register a DuckDB setting (using `ExtensionLoader`) that lets users choose the dialect:
   ```
   D SET lpts_dialect = 'duckdb';
   D PRAGMA lpts('SELECT strptime(date_col, ''%Y-%m-%d'') FROM events');
   -- WITH scan_0(t0_date_col) AS (SELECT date_col FROM memory.main.events),
   -- projection_1(t1_scalar_0) AS (SELECT strptime(t0_date_col, '%Y-%m-%d') FROM scan_0)
   -- SELECT scalar_0 FROM projection_1;

   D SET lpts_dialect = 'postgres';
   D PRAGMA lpts('SELECT strptime(date_col, ''%Y-%m-%d'') FROM events');
   -- WITH scan_0(t0_date_col) AS (SELECT date_col FROM events),
   -- projection_1(t1_scalar_0) AS (SELECT to_date(t0_date_col, 'YYYY-MM-DD') FROM scan_0)
   -- SELECT scalar_0 FROM projection_1;
   ```

3. Pass the dialect to `AstToCteList` (update its signature to accept a `SqlDialect` parameter). For now, the function can ignore the dialect and always produce DuckDB SQL — the point is to have the plumbing in place.

4. Eventually, `AstToCteList` should produce dialect-specific SQL. You do not need to implement all dialect differences — just handle one or two (e.g., table references, cast syntax) to prove the concept works.

### Task 5: Flesh out AST `ToString()` methods

The `AstGetNode::ToString()` in `src/lpts_ast.cpp` is fully implemented as a reference. The other node types have skeleton implementations. Your task is to make them print useful information (expressions, conditions, column names, etc.) so that `PrintAst()` produces a readable tree. This is useful for debugging.

---

## Building and testing

Build with:
```bash
GEN=ninja make release
```

Run the tests:
```bash
make test_release
```

Try it interactively:
```
$ ./build/release/duckdb
D CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
D PRAGMA lpts('SELECT * FROM users');
┌──────────────────────────────────────────────────────────────────────────────┐
│ sql                                                                          │
├──────────────────────────────────────────────────────────────────────────────┤
│ WITH scan_0 (t0_id, t0_name, t0_age) AS (SELECT id, name, age FROM ...),   │
│ projection_1 (t1_id, t1_name, t1_age) AS (SELECT t0_id, t0_name, ...),     │
│ SELECT t1_id AS id, t1_name AS name, t1_age AS age FROM projection_1;       │
└──────────────────────────────────────────────────────────────────────────────┘
```

### Debug flag

To enable verbose debug output, edit `src/include/lpts_debug.hpp` and change:
```cpp
#define LPTS_DEBUG 0    // change to 1
```

This activates `LPTS_DEBUG_PRINT(...)` calls throughout the codebase, which print detailed info about each operator being processed (column bindings, table indices, CTE names, etc.). Useful when you are implementing `LogicalPlanToAst` and need to see what data DuckDB provides for each operator. Remember to set it back to `0` before committing.

### Incremental development

You do not need to implement all operators before testing. A suggested order:

1. **GET + PROJECTION** — enough for `SELECT * FROM table` and `SELECT col FROM table`.
2. **FILTER** — now `SELECT * FROM table WHERE ...` works. All tests in `test/sql/select.test` should pass.
3. **AGGREGATE** — `test/sql/group_by.test` should pass.
4. **JOIN + UNION** — `test/sql/join.test` should pass.

At each step, run `make test_release` — tests for operators you haven't implemented yet will fail (with `NotImplementedException`), but the ones you have implemented should pass.

### Test files

| Test file | Operators needed |
|---|---|
| `test/sql/select.test` | GET, FILTER, PROJECTION |
| `test/sql/group_by.test` | GET, PROJECTION, AGGREGATE |
| `test/sql/join.test` | GET, PROJECTION, JOIN, UNION |

---

## File map

| File | Purpose |
|---|---|
| `src/lpts_extension.cpp` | Extension entry point. Registers `PRAGMA lpts` and `lpts_query`. |
| `src/logical_plan_to_sql.cpp` | Current converter: logical plan → CTE list → SQL. Your reference implementation. |
| `src/include/logical_plan_to_sql.hpp` | CTE node class hierarchy + `LogicalPlanToSql` class. |
| `src/include/lpts_ast.hpp` | **New.** AST node class hierarchy. |
| `src/lpts_ast.cpp` | **New.** AST `ToString()` implementations. |
| `src/include/lpts_pipeline.hpp` | **New.** Pipeline function declarations (`LogicalPlanToAst`, `AstToCteList`). |
| `src/lpts_pipeline.cpp` | **New.** Pipeline function stubs — your main work goes here. |
| `src/include/lpts_debug.hpp` | Debug flag (`LPTS_DEBUG`) and `LPTS_DEBUG_PRINT` macro. |
| `src/lpts_helpers.cpp` | Utility functions (`VecToSeparatedList`, `EscapeSingleQuotes`, etc.). |
| `src/include/lpts_helpers.hpp` | Utility function declarations. |
| `test/sql/*.test` | SQL logic tests — must keep passing. |
| `CMakeLists.txt` | Build config — already includes the new source files. |
