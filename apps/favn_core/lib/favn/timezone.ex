defmodule Favn.Timezone do
  @moduledoc false

  @default_database Tzdata.TimeZoneDatabase

  @spec database!() :: module()
  def database! do
    database = Application.get_env(:favn_core, :time_zone_database, @default_database)
    start_default_database(database)

    if valid_database?(database) do
      database
    else
      raise ArgumentError,
            "Favn timezone database must resolve IANA timezones; " <>
              "configure :favn_core, :time_zone_database with a Calendar.TimeZoneDatabase module"
    end
  end

  @spec valid_identifier?(String.t()) :: boolean()
  def valid_identifier?(timezone) when is_binary(timezone) do
    timezone = String.trim(timezone)

    timezone != "" and
      not String.contains?(timezone, "..") and
      not String.starts_with?(timezone, "/") and
      resolves_timezone?(timezone, database!())
  end

  def valid_identifier?(_other), do: false

  defp valid_database?(database) when is_atom(database) do
    Code.ensure_loaded?(database) and resolves_timezone?("Europe/Oslo", database)
  end

  defp valid_database?(_database), do: false

  defp start_default_database(@default_database) do
    _ = Application.ensure_all_started(:tzdata)
    :ok
  end

  defp start_default_database(_database), do: :ok

  defp resolves_timezone?(timezone, database) do
    case DateTime.new(~D[2026-01-01], ~T[00:00:00], timezone, database) do
      {:ok, _datetime} -> true
      {:ambiguous, _first, _second} -> true
      {:gap, _before_gap, _after_gap} -> true
      {:error, _reason} -> false
    end
  end
end
