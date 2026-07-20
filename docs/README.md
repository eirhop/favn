# Favn Documentation

This directory contains product, architecture, contributor, and operational
documentation for Favn. Start with the section that matches the work you are
doing instead of searching the entire tree.

## Current documentation

| Area | Use it for |
| --- | --- |
| [`architecture/`](architecture/) | System-wide design decisions and runtime boundaries. |
| [`storage/`](storage/) | Persistence contracts and database implementations. |
| [`production/`](production/) | Deployment contracts, security, and operator runbooks. |
| [`operators/`](operators/) | Control-plane behavior visible to operators. |
| [`structure/`](structure/) | Ownership maps for each umbrella application. |
| [`contributing/`](contributing/) | Contributor workflows and documentation rules. |
| [`FEATURES.md`](FEATURES.md) | Implemented product capability inventory. |
| [`ROADMAP.md`](ROADMAP.md) | Forward-looking product direction. |

The public user guides and HexDocs sources live under
[`apps/favn/guides/`](../apps/favn/guides/). Internal implementation documents
must not be presented as public API.

For the current path to a supported release, start with
[`production/README.md`](production/README.md).

## PostgreSQL storage

PostgreSQL 18 is Favn's production, development, and integration-test
control-plane database. The canonical technical documentation is:

1. [`storage/postgresql/architecture.md`](storage/postgresql/architecture.md) —
   ownership, boot composition, write/read paths, tenancy, and scaling model.
2. [`storage/postgresql/data-model.md`](storage/postgresql/data-model.md) —
   table catalog and Mermaid ER diagrams.
3. [`storage/postgresql/testing.md`](storage/postgresql/testing.md) — test tiers
   and clean-build expectations.
4. [`production/postgresql_operator_runbook.md`](production/postgresql_operator_runbook.md)
   — deployment and operational procedures.

The longer
[`architecture/postgresql-control-plane-storage-v2.md`](architecture/postgresql-control-plane-storage-v2.md)
is the detailed design and decision record. It explains why the implementation
has its current invariants; it is not the quickest implementation reference.

## Historical material

- [`report/`](report/) contains point-in-time audits. Findings may already be
  resolved; use Git history and current technical docs before acting on them.
- [`refactor/`](refactor/) contains historical migration plans and completed phase
  notes; it is not an active backlog.
- [`archive/`](archive/) contains explicitly retired material.
- `DOCUMENTATION_PLAN.md` records the original documentation initiative and is
  not the current navigation entry point.

When documentation conflicts, executable contracts and migrations are the final
authority. Update the canonical technical document in the same change that
alters those contracts.
