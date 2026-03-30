---
name: Use GEN=ninja for builds
description: Always use GEN=ninja make instead of plain make when building this project
type: feedback
---

Use `GEN=ninja make` instead of plain `make` when building this project.

**Why:** The user strongly prefers ninja builds — using plain make was rejected.

**How to apply:** Every time you run a build command in this repo, prefix with `GEN=ninja`.