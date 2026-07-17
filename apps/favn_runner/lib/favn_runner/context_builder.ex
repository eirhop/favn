defmodule FavnRunner.ContextBuilder do
  @moduledoc """
  Builds `%Favn.Run.Context{}` values for manifest-backed runner execution.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Run.AssetContext
  alias Favn.Run.Context
  alias Favn.RuntimeConfig.Resolver, as: RuntimeConfigResolver

  @spec build(RunnerWork.t(), Asset.t(), String.t()) :: {:ok, Context.t()} | {:error, term()}
  def build(%RunnerWork{} = work, %Asset{} = asset, execution_id) when is_binary(execution_id) do
    run_id = work.run_id || execution_id
    stage = normalized_stage(work.stage)
    attempt = normalized_attempt(work.attempt)
    max_attempts = normalized_max_attempts(work.max_attempts)

    with {:ok, runtime_config} <- RuntimeConfigResolver.resolve_asset(asset.runtime_config || %{}) do
      {:ok,
       %Context{
         run_id: run_id,
         node_identity: work.node_identity,
         target_refs: [asset.ref],
         asset: %AssetContext{
           ref: asset.ref,
           relation: asset.relation,
           settings: asset.settings || %{}
         },
         runtime_config: runtime_config,
         params: normalized_map(work.params),
         window: RunnerWork.window(work) || Map.get(work.trigger, :window),
         pipeline: work.pipeline,
         run_started_at: normalized_run_started_at(work.run_started_at),
         deadline_at: work.deadline_at,
         stage: stage,
         attempt: attempt,
         max_attempts: max_attempts
       }}
    end
  end

  defp normalized_map(map) when is_map(map), do: map
  defp normalized_map(_other), do: %{}

  defp normalized_run_started_at(%DateTime{} = run_started_at), do: run_started_at
  defp normalized_run_started_at(_other), do: DateTime.utc_now()

  defp normalized_stage(stage) do
    case stage do
      stage when is_integer(stage) and stage >= 0 -> stage
      _other -> 0
    end
  end

  defp normalized_attempt(attempt) do
    case attempt do
      attempt when is_integer(attempt) and attempt > 0 -> attempt
      _other -> 1
    end
  end

  defp normalized_max_attempts(max_attempts) do
    case max_attempts do
      max_attempts when is_integer(max_attempts) and max_attempts > 0 -> max_attempts
      _other -> 1
    end
  end
end
