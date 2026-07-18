defmodule Favn.Log.Cursor do
  @moduledoc """
  Replay cursor for backend logs.

  All cursor scopes carry the authoritative global sequence so replay ordering
  does not depend on per-run or per-asset counters. Log sequences are an
  order-preserving encoding of the outbox publication ID and batch offset; they
  are not PostgreSQL row identity values.
  """

  @type scope :: :global | :run | :asset

  @type t :: %__MODULE__{
          scope: scope(),
          run_id: String.t() | nil,
          asset_step_id: String.t() | nil,
          global_sequence: non_neg_integer()
        }

  defstruct [:scope, :run_id, :asset_step_id, :global_sequence]

  @doc """
  Parses a cursor string.
  """
  @spec parse(String.t()) :: {:ok, t()} | {:error, :invalid_cursor}
  def parse("global:" <> sequence), do: parse_global(sequence)

  def parse("run:" <> rest) do
    with [run_id, sequence] <- String.split(rest, ":", parts: 2),
         {:ok, global_sequence} <- parse_sequence(sequence) do
      {:ok, %__MODULE__{scope: :run, run_id: run_id, global_sequence: global_sequence}}
    else
      _error -> {:error, :invalid_cursor}
    end
  end

  def parse("asset:" <> rest) do
    with [run_id, asset_step_id, sequence] <- String.split(rest, ":", parts: 3),
         {:ok, global_sequence} <- parse_sequence(sequence) do
      {:ok,
       %__MODULE__{
         scope: :asset,
         run_id: run_id,
         asset_step_id: asset_step_id,
         global_sequence: global_sequence
       }}
    else
      _error -> {:error, :invalid_cursor}
    end
  end

  def parse(_cursor), do: {:error, :invalid_cursor}

  @doc """
  Formats a cursor struct.
  """
  @spec format(t()) :: String.t()
  def format(%__MODULE__{scope: :global, global_sequence: sequence}), do: "global:#{sequence}"

  def format(%__MODULE__{scope: :run, run_id: run_id, global_sequence: sequence}),
    do: "run:#{run_id}:#{sequence}"

  def format(%__MODULE__{
        scope: :asset,
        run_id: run_id,
        asset_step_id: asset_step_id,
        global_sequence: sequence
      }),
      do: "asset:#{run_id}:#{asset_step_id}:#{sequence}"

  defp parse_global(sequence) do
    with {:ok, global_sequence} <- parse_sequence(sequence) do
      {:ok, %__MODULE__{scope: :global, global_sequence: global_sequence}}
    end
  end

  defp parse_sequence(value) do
    case Integer.parse(value) do
      {sequence, ""} when sequence >= 0 -> {:ok, sequence}
      _other -> {:error, :invalid_cursor}
    end
  end
end
