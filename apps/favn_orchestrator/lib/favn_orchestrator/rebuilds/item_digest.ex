defmodule FavnOrchestrator.Rebuilds.ItemDigest do
  @moduledoc false

  alias FavnOrchestrator.Rebuild.Plan

  @fields [
    :target_id,
    :item_id,
    :ordinal,
    :work_kind,
    :window_key,
    :window_start,
    :window_end,
    :runtime_input_expectation,
    :candidate_generation_id
  ]

  @spec hash([map()]) :: String.t()
  def hash(items) when is_list(items) do
    items =
      items
      |> Enum.map(&payload/1)
      |> Enum.sort_by(&{&1.ordinal, &1.target_id, &1.item_id})

    Plan.hash(%{items: items})
  end

  defp payload(item) do
    item
    |> Map.take(@fields)
    |> Map.update!(:window_start, &timestamp/1)
    |> Map.update!(:window_end, &timestamp/1)
  end

  defp timestamp(nil), do: nil
  defp timestamp(%DateTime{} = value), do: DateTime.to_unix(value, :microsecond)
end
