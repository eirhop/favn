# favn_test_support

Purpose: dependency-light shared test fixtures, fixture compilation, and test
helpers used across umbrella apps through test-only dependencies.

Code:
- `apps/favn_test_support/lib/favn_test_support/`
- shared fixture sources under `apps/favn_test_support/priv/fixtures/`

Tests:
- `apps/favn_test_support/test/`

Use when adding cross-app fixtures, shared builders, or setup helpers needed by
more than one owner app. Keep app-specific support local to that app.
