defmodule Favn.Storage.Adapter do
  @moduledoc """
  Behaviour for run persistence adapters.

  Run store adapters are the boundary between Favn runtime orchestration and
  concrete persistence backends.

  The default adapter in v0.1 is `Favn.Storage.Adapter.Memory`, which is node-local
  and non-durable. Future adapters (for example Postgres or SQLite) should
  implement this behaviour without changing the `Favn` public API.

  ## Required invariants

    * Run IDs are treated as globally unique identifiers.
    * `put_run/2` is idempotent for the same run ID.
    * `list_runs/2` returns deterministic newest-first ordering by latest persisted write.
    * Adapters should return `{:error, :not_found}` for missing run IDs.

  ## Lifecycle

  Adapters may expose a `child_spec/1` function and return either:

    * `{:ok, child_spec}` when supervision is required
    * `:none` when no process lifecycle is needed
  """

  alias Favn.Run

  @typedoc "Runtime options passed to a concrete run-store adapter."
  @type adapter_opts :: keyword()

  @typedoc "Filter options accepted by `list_runs/2`."
  @type list_opts :: Favn.list_runs_opts()

  @typedoc "Run store errors normalized by the storage facade."
  @type error :: :not_found | :invalid_opts | term()

  @callback child_spec(adapter_opts()) :: {:ok, Supervisor.child_spec()} | :none

  @callback put_run(Run.t(), adapter_opts()) :: :ok | {:error, error()}

  @callback get_run(Favn.run_id(), adapter_opts()) :: {:ok, Run.t()} | {:error, error()}

  @callback list_runs(list_opts(), adapter_opts()) :: {:ok, [Run.t()]} | {:error, error()}
end
