defmodule Favn.Storage.Adapter do
  @moduledoc """
  Low-level storage behaviour accepted by the orchestrator-backed `Favn.Storage`
  facade.

  This contract operates on orchestrator control-plane data, not projected
  `%Favn.Run{}` values. Use `Favn.Storage` for the public run API.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState

  @type adapter_opts :: keyword()
  @type list_opts :: Favn.list_runs_opts()
  @type error :: :not_found | :invalid_opts | term()
  @type scheduler_key :: {module(), atom() | nil}

  @callback child_spec(adapter_opts()) :: {:ok, Supervisor.child_spec()} | :none

  @callback put_manifest_version(Version.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_manifest_version(String.t(), adapter_opts()) ::
              {:ok, Version.t()} | {:error, error()}
  @callback list_manifest_versions(adapter_opts()) :: {:ok, [Version.t()]} | {:error, error()}

  @callback set_active_manifest_version(String.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_active_manifest_version(adapter_opts()) :: {:ok, String.t()} | {:error, error()}

  @callback put_run(RunState.t(), adapter_opts()) :: :ok | {:error, error()}
  @callback get_run(String.t(), adapter_opts()) :: {:ok, RunState.t()} | {:error, error()}
  @callback list_runs(list_opts(), adapter_opts()) :: {:ok, [RunState.t()]} | {:error, error()}
  @callback persist_run_transition(RunState.t(), map(), adapter_opts()) :: :ok | {:error, error()}

  @callback append_run_event(String.t(), map(), adapter_opts()) :: :ok | {:error, error()}
  @callback list_run_events(String.t(), adapter_opts()) :: {:ok, [map()]} | {:error, error()}

  @callback put_scheduler_state(scheduler_key(), map(), adapter_opts()) ::
              :ok | {:error, error()}

  @callback get_scheduler_state(scheduler_key(), adapter_opts()) ::
              {:ok, map() | nil} | {:error, error()}
end
