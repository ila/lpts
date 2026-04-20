# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working with the user

**Before making any changes**, sync the repo:
```bash
git pull                                      # pull latest commits
git submodule update --init --recursive       # update submodules to pinned commits
```

When you're stuck — either unable to fix a bug after 2-3 attempts, or tempted to work around the actual problem by redefining the objective — **stop and ask the user for directions**. Explain clearly what the specific problem is (e.g., "AstToCteList produces wrong column names for a 3-way join — should I fix the AstJoinNode output ordering or adjust the CTE binding map?"). The user knows this codebase deeply and can often point you to the right solution in one sentence. Do not silently change the goal, declare something impossible, or add bloated workarounds without consulting first. We work as a team.

Always test your changes with real queries (e.g., create a table, run `PRAGMA lpts(...)`, check the SQL output, then run `PRAGMA lpts_exec(...)` to verify results) before declaring success, not just unit tests.

Never execute git commands that could lose code. Always ask the user for permission on those.

## Development rules

- **New features must have tests.** Ask the user whether to create a new test file or extend an existing one in `test/sql/`.
- **Never remove a failing test to "fix" a failure.** If a test fails, fix the underlying bug. Tests exist for a reason.
- **Only change `src/` and `test/` files** unless explicitly told otherwise. Do not touch CMakeLists.txt, Makefile, vcpkg.json, or any other project infrastructure files.
- **Every `lpts_check` test in a test file MUST verify round-trip correctness**, not just that the function runs without error. The result must be `true` — meaning the LPTS-generated SQL, when executed, returns the same bag of results as the original query.
- **Before implementing anything, search the existing codebase** for similar patterns or solutions. Check `src/logical_plan_to_sql.cpp` (`CreateCteNode`) as the canonical reference for operator-specific logic. Reuse before reinventing.
- **Use helper functions.** Factor shared logic into helpers. Check `src/lpts_helpers.cpp` and `src/include/lpts_helpers.hpp` for existing utilities (`VecToSeparatedList`, `EscapeSingleQuotes`, etc.).
- **Never edit the `duckdb/` submodule.** The DuckDB source is read-only. All LPTS logic lives in `src/` and `test/`.
- **The `duckdb/` submodule must stay on release tag `v1.5.0`.** Never advance or downgrade it without explicit instruction. If it drifts, run `git -C duckdb checkout v1.5.0` to restore it.
- **Add `LPTS_DEBUG_PRINT` statements** at key processing points (entry into each operator case, before/after CTE node creation, at pipeline boundaries). Use the existing macro from `src/include/lpts_debug.hpp` — it is compiled out when `LPTS_DEBUG` is 0.
- **The existing `logical_plan_to_sql.cpp` is your reference.** When implementing new AST or pipeline logic, use `CreateCteNode()` in `src/logical_plan_to_sql.cpp` as the ground truth for how each operator's data should be extracted and serialized.

## What is LPTS?

LPTS (**L**ogical **P**lan **T**o **S**QL) is a DuckDB extension that takes a SQL query, reads DuckDB's internal *logical plan* for that query, and converts it back into an equivalent SQL string made of CTEs (Common Table Expressions). Each CTE corresponds to one operator from the plan.

```
D PRAGMA lpts('SELECT name FROM users WHERE age > 25');
-- WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
-- filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
-- projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
-- SELECT t1_name AS name FROM projection_2;
```

## Build & Test

```bash
GEN=ninja make          # build (release)
make format-fix         # auto-format all source files (run before committing)

make shell              # launch DuckDB shell with lpts extension loaded
make unittest           # run all SQL logic tests
```

Build outputs go to `build/release/`. DuckDB is a git submodule in `duckdb/`.

### Single test

```bash
build/release/test/unittest "test/sql/select.test"
```

### Interactive testing

```bash
make shell
# Inside the shell:
D CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
D PRAGMA lpts('SELECT name FROM users WHERE age > 25');
D PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');   -- must return true
D PRAGMA lpts_exec('SELECT name FROM users WHERE age > 25');    -- must match original
```

## Architecture

The pipeline has three phases:

```
Logical Plan  →  AST  →  CTE List  →  SQL String
```

### Phase 1: `LogicalPlanToAst` (`src/lpts_pipeline.cpp`)

Walks DuckDB's `LogicalOperator` tree and builds a dialect-agnostic AST. Each `LogicalOperator` node becomes a corresponding `AstNode` with parent-child relationships preserved via the `children` vector.

### Phase 2: `AstToCteList` (`src/lpts_pipeline.cpp`)

Flattens the AST tree (post-order / bottom-up) into an ordered list of `CteNode` objects. Each `AstNode` becomes one CTE. The result is identical to what `LogicalPlanToSql::LogicalPlanToCteList()` produced before the AST layer was introduced.

### Phase 3: CTE List → SQL String (already implemented)

`CteList::ToQuery(true)` serializes the flat list into a WITH ... SELECT SQL string.

### Entry points: `src/lpts_extension.cpp`

| Function | Usage | Description |
|---|---|---|
| `PRAGMA lpts('query')` | Interactive | Returns CTE SQL for the given query |
| `lpts_query('query')` | Table function / tests | Same as above, usable in SELECT |
| `PRAGMA lpts_exec('query')` | Interactive / tests | Runs LPTS-transformed query, returns results |
| `PRAGMA lpts_check('query')` | Tests | Round-trip check: returns `true` if LPTS output matches original query |

Both `lpts` and `lpts_query` call the same pipeline:
```cpp
auto ast = LogicalPlanToAst(context, planner.plan);
SqlDialect dialect = ReadDialect(context);
auto cte_list = AstToCteList(*ast, dialect);
string result_sql = cte_list->ToQuery(true);
```

### Dialect support

A session setting controls output dialect:
```sql
SET lpts_dialect = 'postgres';  -- or 'duckdb' (default)
PRAGMA lpts('SELECT * FROM users');
```

The `SqlDialect` enum is defined in `src/include/lpts_pipeline.hpp`.

### AST node hierarchy (`src/include/lpts_ast.hpp`)

```
AstNode (abstract base, has vector<unique_ptr<AstNode>> children)
├── AstGetNode        — table scan
├── AstFilterNode     — WHERE clause
├── AstProjectNode    — column selection
├── AstAggregateNode  — GROUP BY + aggregates
├── AstJoinNode       — JOIN
├── AstUnionNode      — UNION / UNION ALL
├── AstInsertNode     — INSERT INTO
├── AstOrderNode      — ORDER BY
├── AstLimitNode      — LIMIT / OFFSET
└── AstDistinctNode   — SELECT DISTINCT
```

### CTE node hierarchy (`src/include/logical_plan_to_sql.hpp`)

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

## Key Source Files

| File | Purpose |
|---|---|
| `src/lpts_extension.cpp` | Extension entry point. Registers all pragmas, table functions, and the `lpts_dialect` setting. |
| `src/logical_plan_to_sql.cpp` | **Reference implementation.** The original converter (logical plan → CTE list). Use `CreateCteNode()` as the canonical reference for operator extraction logic. |
| `src/include/logical_plan_to_sql.hpp` | CTE node class hierarchy + `LogicalPlanToSql` class declaration. |
| `src/include/lpts_ast.hpp` | AST node class hierarchy (`AstGetNode`, `AstFilterNode`, etc.). |
| `src/lpts_ast.cpp` | AST `ToString()` implementations (and `PrintAst()`). `AstGetNode::ToString()` is the fully implemented reference. |
| `src/lpts_ast_renderer.cpp` | Box-rendered ASCII tree printer for AST debugging. |
| `src/include/lpts_pipeline.hpp` | Pipeline function declarations: `LogicalPlanToAst`, `AstToCteList`, `SqlDialect`, `ParseSqlDialect`. |
| `src/lpts_pipeline.cpp` | Pipeline function implementations — main work area for the AST layer. |
| `src/include/lpts_debug.hpp` | Debug flag (`LPTS_DEBUG`) and `LPTS_DEBUG_PRINT` macro. |
| `src/lpts_helpers.cpp` | Utility functions (`VecToSeparatedList`, `EscapeSingleQuotes`, etc.). |
| `src/include/lpts_helpers.hpp` | Utility function declarations. |
| `test/sql/*.test` | SQL logic tests — must always pass. |

## Testing

### Existing test files

| Test file | Operators covered |
|---|---|
| `test/sql/select.test` | GET, FILTER, PROJECTION |
| `test/sql/group_by.test` | GET, PROJECTION, AGGREGATE |
| `test/sql/having.test` | AGGREGATE + FILTER (HAVING) |
| `test/sql/join.test` | GET, PROJECTION, JOIN, UNION |
| `test/sql/union.test` | UNION / UNION ALL |
| `test/sql/order_limit.test` | ORDER BY, LIMIT, OFFSET |
| `test/sql/distinct.test` | SELECT DISTINCT |
| `test/sql/functions.test` | Scalar functions, casts |
| `test/sql/lambda.test` | Lambda expressions |
| `test/sql/print_ast.test` | AST `ToString()` output |
| `test/sql/pragmas.test` | `lpts_exec`, `lpts_check` round-trip correctness |

### Test structure conventions

Each test uses the DuckDB SQL logic test format:
```
# name: test/sql/example.test
# description: what this tests
# group: [sql]

require lpts

statement ok
CREATE TABLE t (id INT, val INT);

query I
PRAGMA lpts_check('SELECT * FROM t');
----
true

query II
PRAGMA lpts_exec('SELECT id, val FROM t');
----
1    10
```

### Key test functions

- **`PRAGMA lpts_check('query')`** — the primary correctness test. Returns `true` if the LPTS-generated SQL, when executed, produces the exact same bag of results as the original query. **Every new test must include at least one `lpts_check`.**
- **`PRAGMA lpts_exec('query')`** — executes the LPTS-generated SQL and returns results. Use to verify concrete output values.
- **`lpts_query('query')`** — returns the generated SQL string. Use when you need to assert the exact SQL structure.

### TPC-H coverage queries

The file `tpch-umbra-flat-cte-list.txt` (in the repository root or provided separately) contains all 22 TPC-H queries paired with their expected flat-CTE SQL output as generated by Umbra. These are high-value regression tests for multi-table joins, aggregations, subqueries, and complex filters.

When adding TPC-H tests:
- Use `PRAGMA lpts_check(...)` to verify round-trip correctness on TPC-H tables
- TPC-H tables are: `lineitem`, `orders`, `customer`, `part`, `partsupp`, `supplier`, `nation`, `region`
- Each test file should `LOAD tpch` and call `CALL dbgen(sf=0.01)` to generate data at a small scale factor
- Focus on queries that exercise operators the current AST layer supports (start with Q1 for aggregates, Q3/Q5 for multi-way joins)

Example TPC-H test structure:
```sql
# name: test/sql/tpch.test
# description: TPC-H queries via lpts_check for operator coverage
# group: [sql]

require lpts
require tpch

statement ok
CALL dbgen(sf=0.01);

query I
PRAGMA lpts_check('SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST(''1998-09-02'' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus');
----
true
```

### SQL Storm coverage queries

The `SQL-Storm-queries/` directory contains 1000 SQL queries (files `100.sql` through `10999.sql`) over TPC-H schema tables. These are LLM-generated complex queries including CTEs, multi-way joins, window functions, and FULL OUTER JOIN — useful for stress-testing LPTS operator coverage.

When adding SQL Storm tests:
- Select a representative sample (e.g., 10–20 queries that exercise different operator combinations)
- Prioritize queries using only operators the AST layer currently supports (JOIN, GROUP BY, FILTER, PROJECTION)
- Use `lpts_check` to verify correctness; queries that hit unsupported operators will throw `NotImplementedException` (expected during incremental development)
- Document which operators each test query exercises in a comment above the test

## Debugging

Set `#define LPTS_DEBUG 1` in `src/include/lpts_debug.hpp` for verbose stderr trace output. This activates `LPTS_DEBUG_PRINT(...)` throughout the codebase. Remember to set it back to `0` before committing.

```cpp
// src/include/lpts_debug.hpp
#define LPTS_DEBUG 1   // set to 1 for debug output, 0 for production
```

Use `EXPLAIN` to inspect DuckDB's logical plan before implementing a new operator:
```sql
EXPLAIN SELECT ...;
```

Use `PRAGMA lpts(...)` to see the full CTE SQL output, and `PRAGMA print_ast(...)` if available to visualize the AST tree.

## Code style (clang-format / clang-tidy)

Run `make format-fix` to auto-format. The project uses DuckDB's `.clang-format` (LLVM-based):

- **Classes/Enums**: `CamelCase` (e.g., `AstGetNode`, `SqlDialect`)
- **Functions**: `CamelCase` (e.g., `LogicalPlanToAst`, `AstToCteList`)
- **Variables/parameters/members**: `lower_case` (e.g., `table_index`, `cte_column_names`)
- **Constants/static/constexpr**: `UPPER_CASE`
- **Macros**: `UPPER_CASE` (e.g., `LPTS_DEBUG_PRINT`)
- **Tabs for indentation**, width 4
- **Column limit**: 120
- **Braces**: same line as statement (K&R / Allman-attached)
- **Pointers**: right-aligned (`int *ptr`)
- **No short functions on single line**

## Configuration options

| Setting | Type | Default | Description |
|---|---|---|---|
| `lpts_dialect` | VARCHAR | `"duckdb"` | SQL output dialect: `"duckdb"` or `"postgres"` |

## DDL / usage examples

```sql
-- Convert a query to CTE SQL (interactive)
PRAGMA lpts('SELECT name FROM users WHERE age > 25');

-- Convert via table function (for tests / scripting)
SELECT sql FROM lpts_query('SELECT name FROM users WHERE age > 25');

-- Round-trip correctness check
PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');  -- returns true

-- Execute the LPTS-generated SQL, return results
PRAGMA lpts_exec('SELECT name FROM users WHERE age > 25');

-- Switch to Postgres dialect
SET lpts_dialect = 'postgres';
PRAGMA lpts('SELECT * FROM users');
```
# Project Notes

## Build

- Use `GEN=ninja make` for builds
- Makefile includes `extension-ci-tools/makefiles/duckdb_extension.Makefile`
- Extension name: `lpts`

## Project Structure
- DuckDB extension project
- Submodules: `duckdb`, `extension-ci-tools`
