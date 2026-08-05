defmodule FavnView.Time do
  @moduledoc false

  alias Favn.Timezone

  @utc "Etc/UTC"

  @spec timezone() :: String.t()
  def timezone do
    case Application.get_env(:favn, :default_timezone) do
      value when is_binary(value) and value != "" -> value
      _value -> @utc
    end
  end

  @spec shift(DateTime.t()) :: DateTime.t()
  def shift(%DateTime{} = value) do
    DateTime.shift_zone!(value, timezone(), Timezone.database!())
  end

  @spec to_date(DateTime.t()) :: Date.t()
  def to_date(%DateTime{} = value), do: value |> shift() |> DateTime.to_date()

  @spec beginning_of_day(Date.t()) :: DateTime.t()
  def beginning_of_day(%Date{} = date) do
    date
    |> DateTime.new!(~T[00:00:00], timezone(), Timezone.database!())
    |> DateTime.shift_zone!(@utc, Timezone.database!())
  end

  @spec format(DateTime.t() | term(), String.t(), term()) :: String.t() | term()
  def format(value, format, fallback \\ "-")

  def format(%DateTime{} = value, format, _fallback) do
    value |> shift() |> Calendar.strftime(format)
  end

  def format(_value, _format, fallback), do: fallback
end
