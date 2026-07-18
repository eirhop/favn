defmodule FavnOrchestrator.RunServer.RetryCheckpoint do
  @moduledoc false

  @type validated :: {:sequential, map()} | {:pipeline, map()} | :none

  @spec validate(map(), :sequential | :pipeline) ::
          {:ok, validated()} | {:error, :invalid_retry_checkpoint}
  def validate(metadata, expected_kind)
      when is_map(metadata) and expected_kind in [:sequential, :pipeline] do
    with {:ok, checkpoint} <- validate_metadata(metadata),
         :ok <- validate_kind(checkpoint, expected_kind) do
      {:ok, checkpoint}
    end
  end

  def validate(_metadata, expected_kind) when expected_kind in [:sequential, :pipeline],
    do: {:ok, :none}

  defp validate_metadata(metadata) do
    case field(metadata, :retry_state) do
      nil -> {:ok, :none}
      state when is_map(state) -> validate_state(state)
      _invalid -> {:error, :invalid_retry_checkpoint}
    end
  end

  defp validate_kind(:none, _expected_kind), do: :ok
  defp validate_kind({kind, _state}, kind), do: :ok

  defp validate_kind({_other_kind, _state}, _expected_kind),
    do: {:error, :invalid_retry_checkpoint}

  defp validate_state(state) do
    case field(state, :kind) do
      :sequential -> validate_sequential(state)
      "sequential" -> validate_sequential(state)
      :pipeline -> validate_pipeline(state)
      "pipeline" -> validate_pipeline(state)
      _invalid -> {:error, :invalid_retry_checkpoint}
    end
  end

  defp validate_sequential(state) do
    retry = field(state, :retry)

    if is_map(retry) and valid_ref?(field(retry, :asset_ref)) and
         is_tuple(field(retry, :node_key)) and present_binary?(field(retry, :asset_step_id)) and
         non_negative_integer?(field(retry, :stage)) and
         positive_integer?(field(retry, :next_attempt)) and
         non_negative_integer?(field(retry, :retry_after_ms)) and
         non_negative_integer?(field(state, :sequential_index)) and
         is_integer(field(state, :next_retry_at)) do
      {:ok, {:sequential, state}}
    else
      {:error, :invalid_retry_checkpoint}
    end
  end

  defp validate_pipeline(state) do
    if positive_integer?(field(state, :checkpoint_sequence)) and
         non_negative_integer?(field(state, :stage_index)) and
         positive_integer?(field(state, :next_attempt)) and
         non_negative_integer?(field(state, :stage)) and
         is_integer(field(state, :next_retry_at)) do
      {:ok, {:pipeline, state}}
    else
      {:error, :invalid_retry_checkpoint}
    end
  end

  defp valid_ref?({module, name}) when is_atom(module) and is_atom(name), do: true
  defp valid_ref?(_value), do: false

  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp non_negative_integer?(value), do: is_integer(value) and value >= 0
  defp present_binary?(value), do: is_binary(value) and value != ""

  defp field(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
