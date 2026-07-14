defmodule FavnOrchestrator.Backfill.ReadModelValues do
  @moduledoc false

  @statuses [:pending, :running, :ok, :partial, :error, :cancelled, :timed_out]
  @statuses_by_name Map.new(@statuses, &{Atom.to_string(&1), &1})
  @window_kinds [:hour, :day, :month, :year]
  @window_kinds_by_name %{
    "hour" => :hour,
    "hourly" => :hour,
    "day" => :day,
    "daily" => :day,
    "month" => :month,
    "monthly" => :month,
    "year" => :year,
    "yearly" => :year
  }

  @type status :: :pending | :running | :ok | :partial | :error | :cancelled | :timed_out
  @type window_kind :: :hour | :day | :month | :year

  @spec statuses() :: [status()]
  def statuses, do: @statuses

  @spec normalize_status(term()) :: {:ok, status()} | {:error, {:invalid_status, term()}}
  def normalize_status(value) when value in @statuses, do: {:ok, value}

  def normalize_status(value) when is_binary(value) do
    case Map.fetch(@statuses_by_name, value) do
      {:ok, status} -> {:ok, status}
      :error -> {:error, {:invalid_status, value}}
    end
  end

  def normalize_status(value), do: {:error, {:invalid_status, value}}

  @spec normalize_window_kind(term()) ::
          {:ok, window_kind()} | {:error, {:invalid_window_kind, term()}}
  def normalize_window_kind(value) when value in @window_kinds, do: {:ok, value}
  def normalize_window_kind(:hourly), do: {:ok, :hour}
  def normalize_window_kind(:daily), do: {:ok, :day}
  def normalize_window_kind(:monthly), do: {:ok, :month}
  def normalize_window_kind(:yearly), do: {:ok, :year}

  def normalize_window_kind(value) when is_binary(value) do
    case Map.fetch(@window_kinds_by_name, value) do
      {:ok, window_kind} -> {:ok, window_kind}
      :error -> {:error, {:invalid_window_kind, value}}
    end
  end

  def normalize_window_kind(value), do: {:error, {:invalid_window_kind, value}}
end
