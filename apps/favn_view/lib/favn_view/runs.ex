defmodule FavnView.Runs do
  @moduledoc """
  Thin view-side context over orchestrator run APIs.
  """

  alias FavnOrchestrator

  @spec subscribe_runs() :: :ok | {:error, term()}
  def subscribe_runs, do: FavnOrchestrator.subscribe_runs()

  @spec unsubscribe_runs() :: :ok
  def unsubscribe_runs, do: FavnOrchestrator.unsubscribe_runs()

  @spec subscribe_run(String.t()) :: :ok | {:error, term()}
  def subscribe_run(run_id), do: FavnOrchestrator.subscribe_run(run_id)

  @spec unsubscribe_run(String.t()) :: :ok
  def unsubscribe_run(run_id), do: FavnOrchestrator.unsubscribe_run(run_id)

  @spec list_runs(keyword()) :: {:ok, [Favn.Run.t()]} | {:error, term()}
  def list_runs(opts \\ []), do: FavnOrchestrator.list_runs(opts)

  @spec get_run(String.t()) :: {:ok, Favn.Run.t()} | {:error, term()}
  def get_run(run_id), do: FavnOrchestrator.get_run(run_id)

  @spec list_run_events(String.t(), keyword()) ::
          {:ok, [FavnOrchestrator.RunEvent.t()]} | {:error, term()}
  def list_run_events(run_id, opts \\ []), do: FavnOrchestrator.list_run_events(run_id, opts)

  @spec submit_asset_run(Favn.Ref.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def submit_asset_run(asset_ref, opts \\ []),
    do: FavnOrchestrator.submit_asset_run(asset_ref, opts)

  @spec submit_pipeline_run(module() | [Favn.Ref.t()], keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_pipeline_run(target_or_module, opts \\ []),
    do: FavnOrchestrator.submit_pipeline_run(target_or_module, opts)

  @spec cancel_run(String.t(), map()) :: :ok | {:error, term()}
  def cancel_run(run_id, reason), do: FavnOrchestrator.cancel_run(run_id, reason)

  @spec rerun(String.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def rerun(run_id, opts \\ []), do: FavnOrchestrator.rerun(run_id, opts)
end
