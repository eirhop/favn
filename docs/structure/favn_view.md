# favn_view

Purpose: thin Phoenix/LiveView UI boundary app for Favn. It owns browser-facing
routes, LiveViews, controllers, components, endpoint configuration, and Phoenix
assets.

Ownership rules:

- `apps/favn_view/lib/favn_view/` contains the endpoint, router, LiveViews,
  controllers, components, and web helpers.
- `apps/favn_view/assets/` contains the standard Phoenix-generated asset setup.
- `apps/favn_view/test/` contains endpoint, controller, and LiveView tests.
- `favn_view` must call backend behavior only through the public orchestrator
  facade. It must not depend on storage, scheduler, runner, persistence, repo,
  manifest compiler, plugin, adapter, or low-level orchestrator internals.
