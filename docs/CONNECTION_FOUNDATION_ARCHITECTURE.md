# Favn.Connection Foundation Architecture (v0.4 Step 1)

## Purpose

This document defines the first implementation-ready design for `Favn.Connection`.

Scope is intentionally limited to:

- connection definition shape
- connection definition provider behaviour
- boot-time registration and validation
- runtime config merge rules
- public lookup and inspection APIs

This document does **not** define SQL execution or concrete backend implementations.

---

## 1) Recommended architecture

Adopt a **behaviour + definition struct + boot registry** architecture:

1. Connection providers are explicit modules implementing `Favn.Connection`.
2. Providers return a canonical `%Favn.Connection.Definition{}`.
3. Runtime values come from app config (`config :favn, connections: ...`).
4. Boot loader derives defaults from `definition.config_schema`, merges runtime values, validates, and stores
   resolved connections in a registry process.
5. Public `Favn` APIs read from that registry.

### Why this is the best first step

- idiomatic Elixir (behaviours + structs + OTP process)
- explicit, easy-to-debug registration
- clear split between static design-time definition and runtime secrets
- low magic (no DSL required for first version)
- strong base for future SQL adapter work

---

## 2) Proposed module tree

```text
lib/
  favn/
    connection.ex                        # behaviour + provider contract docs
    connection/
      definition.ex                      # canonical definition struct + type
      resolved.ex                        # validated runtime connection payload
      error.ex                           # normalized error structs/types
      validator.ex                       # pure validation/merge logic
      loader.ex                          # boot loader from app config -> resolved entries
      registry.ex                        # GenServer lookup storage for resolved connections
```

Notes:

- Keep modules under `Favn.Connection.*` as requested.
- Keep merge/validation pure (`validator.ex`) so tests are cheap and deterministic.
- Registry should only store validated data; all heavy checks happen before insert.

---

## 3) Behaviour boundary

Use one callback initially:

```elixir
defmodule Favn.Connection do
  @moduledoc """
  Behaviour for connection definition providers.

  Provider modules declare static connection metadata.
  Runtime values are supplied separately via config.
  """

  alias Favn.Connection.Definition

  @callback definition() :: Definition.t()
end
```

### Decision

- Keep callback surface as `definition/0` only.
- Require `definition/0` to always return `%Definition{}` (pure, declarative, total).
- Loader/validator own all runtime/config error handling.

No lifecycle callbacks here yet (`connect`, `disconnect`, `ping`) because that belongs to `Favn.SQL.Adapter` phase.

---

## 4) Canonical structs

### `Favn.Connection.Definition`

Static contract returned by provider module.

```elixir
defmodule Favn.Connection.Definition do
  @enforce_keys [:name, :adapter, :config_schema]
  defstruct [
    :name,
    :adapter,
    :module,
    config_schema: [],
    doc: nil,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          adapter: module(),
          module: module() | nil,
          config_schema: [field()],
          doc: String.t() | nil,
          metadata: map()
        }

  @type field_type ::
          :string
          | :atom
          | :boolean
          | :integer
          | :float
          | :path
          | :module
          | {:in, [term()]}
          | {:custom, (term() -> :ok | {:error, term()})}

  @type field :: %{
          required(:key) => atom(),
          optional(:required) => boolean(),
          optional(:default) => term(),
          optional(:secret) => boolean(),
          optional(:type) => field_type()
        }
end
```

Field intent:

- `name`: registry key and public lookup name.
- `adapter`: SQL adapter module reference (must be module atom).
- `module`: populated by loader to source provider module.
- `config_schema`: explicit field contract used to derive required/default/secret/type rules.
- `doc`: optional user-facing description.
- `metadata`: free-form introspection metadata.

Example schema:

```elixir
config_schema: [
  %{key: :database, required: true, type: :path},
  %{key: :read_only, required: false, default: false, type: :boolean},
  %{key: :password, required: true, secret: true, type: :string}
]
```

### `Favn.Connection.Resolved`

Validated and merged shape stored in registry.

```elixir
defmodule Favn.Connection.Resolved do
  @enforce_keys [:name, :adapter, :module, :config]
  defstruct [
    :name,
    :adapter,
    :module,
    :config,
    required_keys: [],
    secret_fields: [],
    schema_keys: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          name: atom(),
          adapter: module(),
          module: module(),
          config: map(),
          required_keys: [atom()],
          secret_fields: [atom()],
          schema_keys: [atom()],
          metadata: map()
        }
end
```

---

## 5) Config shape

```elixir
config :favn,
  connection_modules: [
    MyApp.FavnConnections.Warehouse,
    MyApp.FavnConnections.Analytics
  ],
  connections: [
    warehouse: [
      database: System.fetch_env!("WAREHOUSE_DB_PATH"),
      read_only: false
    ],
    analytics: [
      host: System.fetch_env!("ANALYTICS_HOST"),
      user: System.fetch_env!("ANALYTICS_USER"),
      password: System.fetch_env!("ANALYTICS_PASSWORD")
    ]
  ]
```

### Merge rules

For each definition `name`:

1. derive schema key set from `definition.config_schema`
2. build defaults map from schema fields with `:default`
3. overlay runtime `connections[name]`
4. validate by schema (`required`, `type`, `allowed keys`, custom validator)
5. produce final `config` map in `%Resolved{}`

### Hard rules

- Runtime config may only provide keys declared in `config_schema`.
- Runtime keys **cannot override structural fields** (`name`, `adapter`, `config_schema`, etc.).
- Adapter is definition-only and immutable at runtime.
- Unknown runtime keys fail validation by default (strict mode via schema key set).

This strictness avoids silent misconfiguration and typo drift.

---

## 6) Startup/loading flow

Load connections before scheduler/runtime services become available.

### Startup boundary decision (locked)

**Decision A:** SQL asset compilation/discovery must **not** depend on resolved connection payloads.

- Asset registry and graph index may compile/discover SQL assets by connection **name** only.
- Resolved connection values are required at execution/use time, not compile/discovery time.
- This keeps current startup order stable and avoids coupling asset discovery to secret-bearing runtime config resolution.

If a future feature requires compile-time connection presence, that must be a new explicit RFC and startup-order change.

1. `Favn.Application.start/2` loads asset/pipeline registries as today.
2. `Favn.Connection.Loader.load/0` runs before runtime/scheduler children start.
3. Loader reads:
   - `:connection_modules`
   - `:connections`
4. For each module:
   - verify module is loaded
   - verify behaviour implementation
   - call `definition/0`
   - normalize `%Definition{module: provider_module}`
5. Validate all definitions:
   - unique names
   - valid struct fields/types
6. Merge runtime config + schema-validate merged result.
7. Start `Favn.Connection.Registry` with resolved map.
8. If any error exists, fail app boot with normalized startup error.

### Boot failure policy

Invalid connection definitions/config should **fail boot**.

Rationale: this is foundational infra for SQL assets; failing fast is safer than deferred runtime errors.

---

## 7) Validation rules

## Definition validation

- provider module must export `definition/0`
- returned value must be `%Favn.Connection.Definition{}`
- `name` must be atom
- `adapter` must be module atom
- `config_schema` must be a non-empty list of field maps
- every field must include atom `:key`
- field keys must be unique
- `required`, `default`, `secret` values must match expected types when present
- `type` value must be one of supported field types

## Merge/runtime validation

- runtime top-level `connections` must be keyword/map keyed by connection name atoms
- each runtime entry must be keyword/map with atom keys
- unknown keys are rejected against schema key set
- required keys must exist after merge
- required keys may be `nil` only if explicitly allowed by field validator (default behavior: nil is invalid for required keys)
- typed fields must pass type validation
- `{:custom, validator}` fields must return `:ok` or `{:error, reason}`

## Duplicate handling

- duplicate provider modules: de-duplicate by module identity before load
- duplicate connection names across definitions: boot error

---

## 8) Public Favn API

Expose read-only APIs for list/fetch/inspect.

```elixir
@spec list_connections() :: [Favn.Connection.Resolved.t()]
@spec get_connection(atom()) :: {:ok, Favn.Connection.Resolved.t()} | {:error, :not_found}
@spec get_connection!(atom()) :: Favn.Connection.Resolved.t()
@spec connection_registered?(atom()) :: boolean()
```

Recommended behavior:

- `list_connections/0`: sanitized output by default (secret fields redacted)
- `get_connection/1`: sanitized shape by default
- `get_connection!/1`: raises `Favn.Connection.NotFoundError`
- `connection_registered?/1`: fast boolean lookup

Keep API small and predictable. Do not expose raw secret-bearing config through the public `Favn` facade, and do not expose mutation/registration runtime APIs in first version.

---

## 9) Registry API (internal)

```elixir
defmodule Favn.Connection.Registry do
  @spec start_link(keyword()) :: GenServer.on_start()
  @spec list() :: [Favn.Connection.Resolved.t()]
  @spec fetch(atom()) :: {:ok, Favn.Connection.Resolved.t()} | :error
  @spec registered?(atom()) :: boolean()
end
```

State shape:

```elixir
%{
  by_name: %{warehouse: %Favn.Connection.Resolved{}, analytics: ...},
  ordered_names: [:warehouse, :analytics]
}
```

Deterministic ordering improves docs/UI snapshots and test stability.

---

## 10) Normalized error model

Use typed exceptions + normalized tagged tuples internally.

Suggested modules:

- `Favn.Connection.Error`
- `Favn.Connection.ConfigError`
- `Favn.Connection.DefinitionError`
- `Favn.Connection.DuplicateNameError`

Internal return shape from loader/validator:

```elixir
{:ok, %{atom() => Favn.Connection.Resolved.t()}}
{:error, [%Favn.Connection.Error{}]}
```

`%Favn.Connection.Error{}` fields:

- `:type` (`:invalid_module | :invalid_definition | :duplicate_name | :missing_required | :unknown_keys | :invalid_type | :invalid_adapter`)
- `:connection` (name or nil)
- `:module` (provider module or nil)
- `:details` (map)
- `:message` (human-readable)

App boot should raise a single `Favn.Connection.ConfigError` containing aggregated child errors.

---

## 11) Test plan

## Unit tests (`validator`, `loader`)

- valid definition accepted
- non-behaviour module rejected
- invalid `definition/0` return rejected
- duplicate connection name rejected
- defaults + runtime merge works
- runtime override of defaults works
- runtime attempt to override structural fields rejected
- unknown runtime keys rejected
- missing required keys rejected
- optional no-default field accepted when absent
- typed field validation failures are surfaced
- custom validator failures are surfaced
- secret_fields redaction contract works

## Registry tests

- list deterministic ordering
- fetch existing/non-existing
- registered? behavior

## Application boot tests

- valid config boots successfully
- invalid config fails boot with normalized error
- duplicate name causes boot failure

## Favn facade tests

- `list_connections/0` returns sanitized payload
- `get_connection/1` and `get_connection!/1` behavior
- `connection_registered?/1`

---

## 12) Open questions and tradeoffs

1. **Strict unknown-key validation**
   - Recommended: strict by default.
   - Tradeoff: tighter safety vs less flexibility for experimental adapter keys.

2. **Redaction behavior in public API**
   - Recommended: redacted by default.
   - Tradeoff: deep diagnostics must use internal modules/log-safe tooling, not the public facade.

3. **Allow non-atom names?**
   - Recommended: no; keep atom-only for first version.
   - Tradeoff: simplest and fastest lookup vs dynamic external naming.

---

## 13) Suggested implementation order

1. Add `Favn.Connection.Definition` and `Favn.Connection` behaviour.
2. Add `Favn.Connection.Resolved` + `Validator` (pure logic first).
3. Add `Loader` (config read + module normalization).
4. Add `Registry` process with read-only API.
5. Wire into `Favn.Application` startup order.
6. Add public `Favn` facade connection APIs.
7. Add full tests (unit, registry, boot, facade).
8. Document usage in `README.md` and `lib/favn.ex`.

This sequence keeps correctness and fast feedback first, then integration.
