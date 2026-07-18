defmodule FavnOrchestrator.Persistence.Backend do
  @moduledoc """
  Lifecycle contract for an orchestrator persistence backend.

  Product operations live in the capability stores returned by `stores/0`; this
  behaviour intentionally has no table CRUD callbacks.
  """

  alias FavnOrchestrator.Persistence.Diagnostics
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Readiness
  alias FavnOrchestrator.Persistence.Stores

  @callback child_specs(keyword()) ::
              {:ok, [Supervisor.child_spec()]} | {:error, Error.t()}
  @callback stores() :: Stores.t()
  @callback readiness(keyword()) :: {:ok, Readiness.t()} | {:error, Error.t()}
  @callback diagnostics(keyword()) :: {:ok, Diagnostics.t()} | {:error, Error.t()}
end

defmodule FavnOrchestrator.Persistence.Readiness do
  @moduledoc "Status returned by the production readiness probe."

  @enforce_keys [:status, :ready?, :backend]
  defstruct [:status, :ready?, :backend, checks: %{}]

  @type status :: :ready | :empty_database | :upgrade_required | :incompatible | :unavailable
  @type t :: %__MODULE__{
          status: status(),
          ready?: boolean(),
          backend: module(),
          checks: map()
        }
end

defmodule FavnOrchestrator.Persistence.Diagnostics do
  @moduledoc "Redacted operational diagnostics for one persistence backend."

  @enforce_keys [:backend, :engine, :schema]
  defstruct [:backend, :engine, :schema, :pool, :features, metadata: %{}]

  @type t :: %__MODULE__{
          backend: module(),
          engine: map(),
          schema: map(),
          pool: map() | nil,
          features: map() | nil,
          metadata: map()
        }
end
