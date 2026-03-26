Dev review feedback:
- Duplicate type from asset.ex -> @valid_kinds [:materialized, :view, :ephemeral]. please reuse type from asset.ex
- In tests add logging of what test and results so that i can see results when running tests. 

Maintenance checklist (required on every API or roadmap change):
- Update `README.md` as the canonical source for release status, roadmap, and feature/limitation matrix changes.
- Update `lib/flux.ex` moduledoc as the canonical source for API behavior/contracts and usage examples.



Natural next steps:
1. Implement dependency existence/graph construction on top of __flux_assets__/0
2. Add a registry layer for Flux.list_assets/0
3. Add richer asset validation rules once cross-module planning starts

If you want, I can do the next step and start the dependency graph layer on top of the new Flux.Assets metadata.
