defmodule FavnOrchestrator.Storage.Adapter.Memory.TargetStatuses do
  @moduledoc """
  Target-status read model operations for the in-memory adapter.
  """

  alias FavnOrchestrator.Storage.Adapter.Memory.Query
  alias FavnOrchestrator.Storage.Adapter.Memory.State
  alias FavnOrchestrator.TargetStatus

  @doc false
  @spec put(State.t(), TargetStatus.t()) :: State.t()
  def put(%State{} = state, %TargetStatus{} = status) do
    %{state | target_statuses: Map.put(state.target_statuses, key(status), status)}
  end

  @doc false
  def get(%State{} = state, key), do: Query.fetch(state.target_statuses, key)

  @doc false
  def list(%State{} = state, manifest_version_id, target_kind, target_ids) do
    target_ids
    |> Enum.uniq()
    |> Enum.reduce(%{}, fn target_id, statuses ->
      case Map.get(state.target_statuses, {manifest_version_id, target_kind, target_id}) do
        %TargetStatus{} = status -> Map.put(statuses, target_id, status)
        nil -> statuses
      end
    end)
  end

  @doc false
  @spec replace(State.t(), term(), [TargetStatus.t()]) :: {:ok, State.t()} | {:error, term()}
  def replace(%State{} = state, requested_scope, statuses) do
    with {:ok, scope} <- normalize_scope(requested_scope),
         :ok <- validate_rows(scope, statuses) do
      remaining = reject_scope(state.target_statuses, scope)
      next = Enum.reduce(statuses, remaining, &Map.put(&2, key(&1), &1))
      {:ok, %{state | target_statuses: next}}
    end
  end

  @doc false
  @spec delete(State.t(), term()) :: {:ok, State.t()} | {:error, term()}
  def delete(%State{} = state, requested_scope) do
    with {:ok, scope} <- normalize_scope(requested_scope) do
      {:ok, %{state | target_statuses: reject_scope(state.target_statuses, scope)}}
    end
  end

  defp normalize_scope({:manifest_version, manifest_version_id})
       when is_binary(manifest_version_id),
       do: {:ok, {:manifest_version, manifest_version_id}}

  defp normalize_scope({:manifest_version, manifest_version_id, target_kind})
       when is_binary(manifest_version_id) and target_kind in [:asset, :pipeline],
       do: {:ok, {:manifest_version, manifest_version_id, target_kind}}

  defp normalize_scope(scope), do: {:error, {:unsupported_target_status_scope, scope}}

  defp validate_rows(scope, statuses) do
    if Enum.all?(statuses, &in_scope?(&1, scope)),
      do: :ok,
      else: {:error, :target_status_scope_mismatch}
  end

  defp reject_scope(values, scope) do
    values |> Enum.reject(fn {_key, status} -> in_scope?(status, scope) end) |> Map.new()
  end

  defp in_scope?(%TargetStatus{manifest_version_id: id}, {:manifest_version, id}), do: true

  defp in_scope?(
         %TargetStatus{manifest_version_id: id, target_kind: kind},
         {:manifest_version, id, kind}
       ),
       do: true

  defp in_scope?(_status, _scope), do: false

  defp key(status), do: {status.manifest_version_id, status.target_kind, status.target_id}
end
