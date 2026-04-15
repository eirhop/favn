defmodule FavnOrchestrator.Storage.Adapter do
  @moduledoc """
  Storage boundary for orchestrator control-plane state.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.RunState

  @type adapter_opts :: keyword()
  @type scheduler_key :: {module(), atom() | nil}

  @callback child_spec(adapter_opts()) :: {:ok, Supervisor.child_spec()} | :none

  @callback put_manifest_version(Version.t(), adapter_opts()) :: :ok | {:error, term()}
  @callback get_manifest_version(String.t(), adapter_opts()) ::
              {:ok, Version.t()} | {:error, term()}
  @callback list_manifest_versions(adapter_opts()) :: {:ok, [Version.t()]} | {:error, term()}

  @callback set_active_manifest_version(String.t(), adapter_opts()) :: :ok | {:error, term()}
  @callback get_active_manifest_version(adapter_opts()) :: {:ok, String.t()} | {:error, term()}

  @callback put_run(RunState.t(), adapter_opts()) :: :ok | {:error, term()}
  @callback get_run(String.t(), adapter_opts()) :: {:ok, RunState.t()} | {:error, term()}
  @callback list_runs(keyword(), adapter_opts()) :: {:ok, [RunState.t()]} | {:error, term()}

  @callback append_run_event(String.t(), map(), adapter_opts()) :: :ok | {:error, term()}
  @callback list_run_events(String.t(), adapter_opts()) :: {:ok, [map()]} | {:error, term()}

  @callback put_scheduler_state(scheduler_key(), map(), adapter_opts()) :: :ok | {:error, term()}
  @callback get_scheduler_state(scheduler_key(), adapter_opts()) ::
              {:ok, map() | nil} | {:error, term()}
end
