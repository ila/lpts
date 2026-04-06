---
name: run-tests
description: Build and run LPTS test suite (or a specific test file).
---

## Instructions

1. Build if needed: `GEN=ninja make 2>&1 | tail -5`
2. Run all tests: `make unittest 2>&1`
   - Or a specific test: `build/release/test/unittest "test/sql/<test_name>.test" 2>&1`
3. Report: number of assertions passed/failed
4. If any fail, show the failing test name and expected vs actual values
5. If a test crashes (segfault, abort), investigate the crash — never skip or remove the test

## Test files

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
| `test/sql/cast.test` | CAST expressions |
| `test/sql/print_ast.test` | AST ToString() output |
| `test/sql/pragmas.test` | lpts_exec, lpts_check round-trip correctness |

## Key test functions

- **`PRAGMA lpts_check('query')`** — Primary correctness check. Returns `true` if LPTS output matches original query via bag equality. Every test must include at least one.
- **`PRAGMA lpts_exec('query')`** — Executes LPTS-generated SQL. Use to verify concrete output values.
- **`lpts_query('query')`** — Returns the generated SQL string. Use to assert exact SQL structure.

## TPC-H tests

TPC-H tests require the `tpch` extension. Structure:
```
require lpts
require tpch

statement ok
CALL dbgen(sf=0.01);

query I
PRAGMA lpts_check('<tpch_query>');
----
true
```

## SQL Storm tests

The `SQL-Storm-queries/` directory contains 1000 complex queries over TPC-H tables.
Select representative samples that exercise different operator combinations.
Use `lpts_check` to verify correctness. Unsupported operators will throw
`NotImplementedException` (expected during incremental development).

## Debugging test failures

1. Set `#define LPTS_DEBUG 1` in `src/include/lpts_debug.hpp` for verbose pipeline trace
2. Use `PRAGMA lpts('...')` in `make shell` to see the generated SQL
3. Use `PRAGMA print_ast('...')` to visualize the AST tree
4. Use `EXPLAIN <query>` to inspect DuckDB's logical plan
5. Remember to set `LPTS_DEBUG` back to `0` before committing
