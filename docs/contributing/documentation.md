# Updating Documentation

Read [`../DOCUMENTATION_GUIDE.md`](../DOCUMENTATION_GUIDE.md) for the canonical
writing, placement, boundary, and verification rules. This page is the short
contributor workflow.

1. Identify the reader, task, owning app, and documentation type.
2. Find the canonical page in the [documentation map](../README.md).
3. Inspect only the owning code, tests, and structure page.
4. Update the smallest complete source of truth; link to it from overviews instead
   of copying the details.
5. Review public/internal and compile-time/runtime boundaries.
6. Check headings, links, examples, defaults, limits, and failure behavior.
7. Run `git diff --check` plus only the focused checks required by changed examples
   or source files.

Public package guidance belongs in `apps/favn/README.md` and
`apps/favn/guides/`. Current capabilities belong in `docs/FEATURES.md`; future
work belongs in `docs/ROADMAP.md` and GitHub issues. Reports, refactor plans, and
archives are historical evidence, not active requirements.
