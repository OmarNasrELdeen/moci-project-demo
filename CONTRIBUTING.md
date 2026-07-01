# Contributing / Git Standards

This project uses **GitHub Flow** with protected `main`.

## Branching

- `main` — always deployable, protected, no direct pushes.
- Feature branches: `feature/<short-description>` (e.g. `feature/bronze-jdbc-extractor`)
- Fixes: `fix/<short-description>`
- Chores/infra: `chore/<short-description>`
- Docs: `docs/<short-description>`

## Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary>

[optional body]
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`, `perf`

Examples:
- `feat(bronze): add incremental JDBC extractor for orders table`
- `fix(silver): correct dedup key for customers`
- `chore(repo): add pre-commit config`

## Pull requests

- One logical change per PR. Keep PRs small and reviewable.
- PR title follows Conventional Commits format.
- Fill in the PR template (what/why/how tested).
- CI (lint + unit tests) must pass before merge.
- Squash-merge into `main`.

## Code style

- Python formatted with `black`, linted with `ruff`.
- Type hints required on public functions.
- Run `pre-commit install` once after cloning to enable local hooks.

## Tests

- New extract/transform/load logic must include unit tests under `tests/`.
- Use `pytest` + `chispa` for Spark DataFrame comparisons.
