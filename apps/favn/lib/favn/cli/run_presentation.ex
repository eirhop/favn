defmodule Favn.CLI.RunPresentation do
  @moduledoc false

  @doc false
  @spec target_label(map()) :: String.t()
  def target_label(run) when is_map(run) do
    field(run, "target_label") ||
      case field(run, "target_refs") do
        [first | rest] -> Enum.join([first | rest], ",")
        _other -> "n/a"
      end
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, atom_key(key))

  defp atom_key("target_label"), do: :target_label
  defp atom_key("target_refs"), do: :target_refs
end
