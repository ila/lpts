---
name: write-docs
description: Style guide for writing LPTS documentation. Auto-loaded when writing, editing, or reviewing docs, README, user guides, or SQL reference pages. Blends Snowflake's structured clarity with DuckDB's concise, example-first approach.
---

# LPTS Documentation Style Guide

This guide defines how to write documentation for LPTS. The style blends
**Snowflake's structured clarity** (hierarchical sections, thorough parameter docs)
with **DuckDB's concise, example-first approach** (lead with code, short sentences,
practical tone).

---

## 1. Core Principles

1. **Lead with examples.** Show what it does before explaining how it works.
   DuckDB puts Examples before Syntax. Follow that: example first, reference second.
2. **One idea per sentence.** Keep sentences to 15-25 words. Break longer thoughts
   into two sentences. Never nest more than one subordinate clause.
3. **Address the reader as "you".** Not "the user" or "one". Active voice, imperative
   mood for instructions: "Run the query" not "The query can be run".
4. **No filler.** Cut "In order to", "It should be noted that", "Please note that".
   Just state the fact.
5. **Be precise about behavior.** Don't say "may" when you mean "does". Don't say
   "should" when you mean "must". Reserve "may" for genuine uncertainty.
6. **Document every feature and every known limitation.** If a feature exists, it must
   appear in the docs. If a limitation exists, it must appear in a Limitations section.
   Undocumented features and silent limitations are bugs.

---

## 2. Page Structure

### 2.1 Conceptual / User Guide Pages

```
# Page Title                          <- H1: noun phrase, no verb
                                      <- 2-3 sentence intro: what this is + why it matters
## How It Works                       <- H2: explain the mechanism
### Sub-concept                       <- H3: one per distinct idea
## When to Use This                   <- H2: use cases, decision criteria
## Limitations                        <- H2: always present, even if short
## Examples                           <- H2: 2-4 progressively complex examples
## See Also                           <- H2: links to related pages
```

### 2.2 SQL Reference Pages

```
# STATEMENT NAME                      <- H1: the SQL command
                                      <- 1-2 sentence description
## Examples                           <- H2: FIRST, before syntax
## Syntax                             <- H2: formal syntax block
## Parameters                         <- H2: every parameter documented
## Usage Notes                        <- H2: behavioral details, edge cases
## Limitations                        <- H2: what doesn't work
## See Also                           <- H2: related commands
```

---

## 3. Syntax Blocks

Use fenced SQL code blocks. Follow Snowflake's notation for formal syntax:

```sql
PRAGMA lpts('<select_statement>');
```

**Notation conventions:**
- `<placeholder>` -- user-supplied value (angle brackets)
- `[ optional ]` -- optional clause (square brackets)
- `{ choice1 | choice2 }` -- choose one (braces + pipe)
- Keywords in UPPERCASE, identifiers in lowercase

**For informal examples** (the Examples section), use real values:

```sql
PRAGMA lpts('SELECT name FROM users WHERE age > 25');
```

---

## 4. Examples

### 4.1 Structure

1. **Start simple.** First example: the most common, minimal use case.
2. **Build up.** Each subsequent example adds one concept (filter, join, aggregate).
3. **Show setup AND result.** Include the CREATE TABLE, query, and output.
4. **Use realistic data.** Not `foo/bar/baz`. Use `users`, `orders`, `products`.

### 4.2 Format

```sql
-- Create a table and convert a query to CTE SQL
CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 22), (3, 'Carol', 28);

PRAGMA lpts('SELECT name FROM users WHERE age > 25');
```

```text
WITH scan_0(t0_age, t0_name) AS (SELECT age, name FROM memory.main.users),
filter_1 AS (SELECT * FROM scan_0 WHERE (t0_age) > (CAST(25 AS INTEGER))),
projection_2(t1_name) AS (SELECT t0_name FROM filter_1)
SELECT t1_name AS name FROM projection_2;
```

```sql
-- Verify round-trip correctness
PRAGMA lpts_check('SELECT name FROM users WHERE age > 25');
```

```text
true
```

### 4.3 SQL Comment Style

- Use `--` comments, not `/* */`
- Comment above the statement, not inline
- Describe *what* and *why*, not the SQL syntax itself
- Start with a verb: "Create...", "Convert...", "Verify..."

---

## 5. Callout Boxes

Three types, used sparingly:

### Note -- Additional context
```markdown
> **Note:** Each CTE corresponds to exactly one operator from DuckDB's logical plan.
```

### Important -- Can cause incorrect results if ignored
```markdown
> **Important:** The `lpts_check` function compares bag equality. ORDER BY differences
> alone will not cause a mismatch.
```

### Tip -- Non-obvious efficiency or convenience trick
```markdown
> **Tip:** Use `SET lpts_dialect = 'postgres'` to generate Postgres-compatible SQL
> without catalog/schema prefixes in table references.
```

**Rules:**
- Maximum 2-3 callouts per page
- Never put critical information *only* in a callout
- Never stack two callouts back-to-back

---

## 6. Tables

Use tables for structured reference data:

```markdown
| Function | Usage | Description |
|---|---|---|
| `PRAGMA lpts('query')` | Interactive | Returns CTE SQL for the given query |
| `lpts_query('query')` | Table function | Same as above, usable in SELECT |
| `PRAGMA lpts_exec('query')` | Testing | Runs LPTS-transformed query |
| `PRAGMA lpts_check('query')` | Testing | Round-trip correctness check |
```

- Left-align text columns
- Use backticks for code values
- Keep descriptions to one line

---

## 7. Limitations Sections

Every feature page must have a Limitations section:

```markdown
## Limitations

- Window functions are not yet supported.
- FULL OUTER JOIN, CROSS JOIN, and SEMI JOIN may throw NotImplementedException.
- CTEs in the source query are expanded by DuckDB's planner before LPTS processes
  them -- the output will not preserve the original CTE structure.
- Very complex nested expressions may produce verbose output.
```

**Rules:**
- Use a bulleted list, one limitation per bullet
- State what is NOT supported, then what happens instead
- Don't apologize or promise future fixes
- Order by importance (most impactful first)

---

## 8. Writing Checklist

Before publishing any documentation page, verify:

- [ ] First section is Examples (for reference pages) or a 2-3 sentence intro (for guides)
- [ ] Every code block uses `sql` or `text` language tags
- [ ] Every example shows both the command AND the result
- [ ] Callouts are Note/Important/Tip only, max 3 per page
- [ ] Limitations section exists and is accurate
- [ ] No sentence exceeds 30 words
- [ ] "You/your" used, never "the user" or "one"
- [ ] Active voice throughout
- [ ] No filler phrases

---

## 9. File Organization

**`README.md`** (project root) -- The landing page. Contains:
- One-paragraph project description
- Feature highlights (bulleted)
- Quick-start example (create table, run PRAGMA lpts, check result)
- Build instructions
- Link to `docs/` for detailed documentation

**`docs/`** -- Detailed documentation, organized by topic:

```
docs/
├── getting-started.md          <- Quick start guide
├── user-guide/
│   ├── cte-pipeline.md         <- Conceptual: how the pipeline works
│   ├── ast-layer.md            <- Conceptual: AST nodes and tree structure
│   ├── dialect-support.md      <- Conceptual: DuckDB vs Postgres output
│   └── debugging.md            <- Using LPTS_DEBUG, print_ast, EXPLAIN
├── sql-reference/
│   ├── pragma-lpts.md
│   ├── pragma-lpts-exec.md
│   ├── pragma-lpts-check.md
│   ├── lpts-query.md
│   └── configuration.md
└── limitations.md              <- Global limitations page
```
