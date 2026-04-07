defmodule Favn.Timezone do
  @moduledoc false

  @spec valid_identifier?(String.t()) :: boolean()
  def valid_identifier?(timezone) when is_binary(timezone) do
    timezone = String.trim(timezone)

    timezone != "" and
      not String.contains?(timezone, "..") and
      not String.starts_with?(timezone, "/") and
      Enum.any?(zoneinfo_roots(), fn root ->
        root
        |> Path.join(timezone)
        |> File.regular?()
      end)
  end

  def valid_identifier?(_other), do: false

  defp zoneinfo_roots do
    ["/usr/share/zoneinfo", "/usr/share/lib/zoneinfo", "/etc/zoneinfo"]
    |> Enum.uniq()
    |> Enum.filter(&File.dir?/1)
  end
end
