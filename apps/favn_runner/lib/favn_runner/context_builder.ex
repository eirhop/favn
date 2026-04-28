defmodule FavnRunner.ContextBuilder do
  @moduledoc """
  Builds `%Favn.Run.Context{}` values for manifest-backed runner execution.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Run.Context
  alias Favn.RuntimeConfig.Resolver, as: RuntimeConfigResolver

  @spec build(RunnerWork.t(), Asset.t(), String.t()) :: {:ok, Context.t()} | {:error, term()}
  def build(%RunnerWork{} = work, %Asset{} = asset, execution_id) when is_binary(execution_id) do
    run_id = work.run_id || execution_id
    stage = normalized_stage(work.metadata)
    attempt = normalized_attempt(work.metadata)
    max_attempts = normalized_max_attempts(work.metadata)

    with {:ok, runtime_config} <- RuntimeConfigResolver.resolve_asset(asset.runtime_config || %{}) do
      {:ok,
       %Context{
         run_id: run_id,
         target_refs: [asset.ref],
         current_ref: asset.ref,
         asset: %{ref: asset.ref, relation: asset.relation, config: asset.config || %{}},
         config: runtime_config,
         params: normalized_map(work.params),
         window: Map.get(work.trigger, :window),
         pipeline: Map.get(work.trigger, :pipeline),
         run_started_at: DateTime.utc_now(),
         stage: stage,
         attempt: attempt,
         max_attempts: max_attempts
       }}
    end
  end

  defp normalized_map(map) when is_map(map), do: map
  defp normalized_map(_other), do: %{}

  defp normalized_stage(metadata) when is_map(metadata) do
    case Map.get(metadata, :stage, 0) do
      stage when is_integer(stage) and stage >= 0 -> stage
      _other -> 0
    end
  end

  defp normalized_stage(_metadata), do: 0

  defp normalized_attempt(metadata) when is_map(metadata) do
    case Map.get(metadata, :attempt, 1) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt
      _other -> 1
    end
  end

  defp normalized_attempt(_metadata), do: 1

  defp normalized_max_attempts(metadata) when is_map(metadata) do
    case Map.get(metadata, :max_attempts, 1) do
      max_attempts when is_integer(max_attempts) and max_attempts > 0 -> max_attempts
      _other -> 1
    end
  end

  defp normalized_max_attempts(_metadata), do: 1
end
