# `apps/favn_test_support`

Purpose:

- shared cross-app test fixtures and small loader helpers for umbrella apps

Visibility:

- internal test-support only

Current responsibilities:

- own reusable fixture source files under `priv/fixtures/**`
- expose dependency-light helper APIs for fixture path lookup and compilation
- provide deterministic fixture group module mappings used by migration tests

Primary helper API (`FavnTestSupport.Fixtures`):

- `fixture_path!/1`
- `compile_fixture!/1`
- `compile_fixtures!/1`
- `modules!/1`

What belongs here:

- shared fixture source used by multiple owner apps
- helper modules that only deal with fixture metadata/loading

What should stay local in owner apps:

- app-specific runtime setup and teardown
- app-specific assertion helpers and behavior semantics
- app-specific test harnesses in `apps/<owner_app>/test/support`

Allowed dependencies in Phase 1:

- keep this app dependency-light so low-level apps can consume it in tests

Must not depend on in Phase 1:

- production runtime paths

Current status:

- shared fixture substrate implemented for migration prep
