# LPTS — Logical Plan To String

A DuckDB extension that converts a SQL query's logical plan back into a SQL string representation. This is useful for understanding how DuckDB internally represents queries and for compiler/optimizer research.

The extension takes an input SQL query, runs it through DuckDB's parser and planner to obtain a logical plan, then converts that plan into an intermediate representation (IR) composed of CTEs, and finally serializes it back to a SQL string.

## Building

```sh
GEN=ninja make
```

The main binaries that will be built are:
```
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/lpts/lpts.duckdb_extension
```

## Usage

Start the DuckDB shell with the extension loaded:
```sh
./build/release/duckdb
```

### PRAGMA syntax

```sql
-- First create some tables to query against
CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DECIMAL);

-- Convert a SELECT query's logical plan to SQL
PRAGMA lpts('SELECT * FROM users');

-- More complex example with joins
PRAGMA lpts('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id');

-- Aggregation example
PRAGMA lpts('SELECT name, count(*) FROM users GROUP BY name');
```

### Table function syntax

For programmatic use, the `lpts_query` table function returns the result as a row:
```sql
SELECT * FROM lpts_query('SELECT * FROM users WHERE age > 25');
```

## How it works

1. The input SQL query is parsed using DuckDB's `Parser`
2. The parsed statement is planned using DuckDB's `Planner` to produce a `LogicalOperator` tree
3. `LogicalPlanToSql` traverses the logical plan recursively, converting each operator into an IR node (`GetNode`, `FilterNode`, `ProjectNode`, `AggregateNode`, `JoinNode`, etc.)
4. Each IR node becomes a CTE (Common Table Expression) in the output
5. The final IR is serialized back into a SQL string with CTEs

### Supported operators

- `LOGICAL_GET` — table scans (with filter pushdown)
- `LOGICAL_FILTER` — WHERE clauses
- `LOGICAL_PROJECTION` — column selection and expressions
- `LOGICAL_AGGREGATE_AND_GROUP_BY` — aggregates and GROUP BY
- `LOGICAL_COMPARISON_JOIN` — joins (INNER, LEFT, RIGHT, OUTER)
- `LOGICAL_UNION` — UNION / UNION ALL
- `LOGICAL_INSERT` — INSERT statements

## Project structure

```
src/
  include/
    lpts_extension.hpp          # Extension class declaration
    logical_plan_to_sql.hpp     # IR node classes and LogicalPlanToSql converter
    lpts_helpers.hpp            # String utility functions
  lpts_extension.cpp            # Extension entry point, pragma and table function registration
  logical_plan_to_sql.cpp       # IR node ToQuery() implementations and plan traversal
  lpts_helpers.cpp              # Helper function implementations
test/
  sql/
    select.test                 # SELECT, filter tests
    group_by.test               # Aggregate and GROUP BY tests
    join.test                   # JOIN and UNION tests
```

## Running tests

```sh
make test
```

## Limitations

- Tables referenced in the input query must exist in the current database context
- Not all logical operator types are supported yet (e.g. DELETE, UPDATE, subqueries)
- The output SQL uses CTE-based decomposition, which may differ from the original query structure

## Setting up CLion

### Opening project
Configuring CLion with this extension requires a little work. Firstly, make sure that the DuckDB submodule is available.
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`, and CMake Options to which the following flag should be added, with the path to the extension CMakeList:

```
-DDUCKDB_EXTENSION_CONFIGS=<path_to_the_exentension_CMakeLists.txt>
```

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
