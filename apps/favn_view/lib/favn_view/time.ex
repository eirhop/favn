defmodule FavnView.Time do
  @moduledoc false

  alias Favn.Timezone

  @utc "Etc/UTC"

  @spec timezone(String.t() | map()) :: String.t()
  def timezone(value) when is_binary(value) and value != "", do: value
  def timezone(%{default_timezone: value}), do: timezone(value)
  def timezone(%{workspace_configuration: value}), do: timezone(value)

  @spec shift(DateTime.t(), String.t() | map()) :: DateTime.t()
  def shift(%DateTime{} = value, configuration) do
    DateTime.shift_zone!(value, timezone(configuration), Timezone.database!())
  end

  @spec to_date(DateTime.t(), String.t() | map()) :: Date.t()
  def to_date(%DateTime{} = value, configuration),
    do: value |> shift(configuration) |> DateTime.to_date()

  @spec beginning_of_day(Date.t(), String.t() | map()) :: DateTime.t()
  def beginning_of_day(%Date{} = date, configuration) do
    date
    |> DateTime.new!(~T[00:00:00], timezone(configuration), Timezone.database!())
    |> DateTime.shift_zone!(@utc, Timezone.database!())
  end

  @spec format(DateTime.t() | term(), String.t(), String.t() | map(), term()) ::
          String.t() | term()
  def format(value, format, configuration, fallback \\ "-")

  def format(%DateTime{} = value, format, configuration, _fallback) do
    value |> shift(configuration) |> Calendar.strftime(format)
  end

  def format(_value, _format, _configuration, fallback), do: fallback
end
