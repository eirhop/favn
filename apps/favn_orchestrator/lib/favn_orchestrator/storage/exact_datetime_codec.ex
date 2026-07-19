defmodule FavnOrchestrator.Storage.ExactDateTimeCodec do
  @moduledoc false

  @spec encode(DateTime.t()) :: map()
  def encode(%DateTime{calendar: Calendar.ISO} = datetime) do
    %{
      "value" => DateTime.to_iso8601(datetime),
      "wall" => datetime |> DateTime.to_naive() |> NaiveDateTime.to_iso8601(),
      "timezone" => datetime.time_zone,
      "zone_abbr" => datetime.zone_abbr,
      "utc_offset" => datetime.utc_offset,
      "std_offset" => datetime.std_offset
    }
  end

  @spec decode(map()) :: {:ok, DateTime.t()} | {:error, term()}
  def decode(%{
        "value" => value,
        "wall" => wall,
        "timezone" => timezone,
        "zone_abbr" => zone_abbr,
        "utc_offset" => utc_offset,
        "std_offset" => std_offset
      })
      when is_binary(value) and is_binary(wall) and is_binary(timezone) and
             is_binary(zone_abbr) and is_integer(utc_offset) and is_integer(std_offset) do
    with {:ok, naive} <- NaiveDateTime.from_iso8601(wall),
         datetime <- from_naive(naive, timezone, zone_abbr, utc_offset, std_offset),
         true <- DateTime.to_iso8601(datetime) == value do
      {:ok, datetime}
    else
      false -> {:error, {:inconsistent_datetime, value}}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, {:invalid_datetime, error}}
  end

  def decode(value), do: {:error, {:invalid_exact_datetime, value}}

  defp from_naive(naive, timezone, zone_abbr, utc_offset, std_offset) do
    %DateTime{
      year: naive.year,
      month: naive.month,
      day: naive.day,
      hour: naive.hour,
      minute: naive.minute,
      second: naive.second,
      microsecond: naive.microsecond,
      time_zone: timezone,
      zone_abbr: zone_abbr,
      utc_offset: utc_offset,
      std_offset: std_offset,
      calendar: Calendar.ISO
    }
  end
end
