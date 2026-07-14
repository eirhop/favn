defmodule FavnOrchestrator.API.SSE.Cursor do
  @moduledoc """
  Validates and parses the bounded `Last-Event-ID` cursor contract.

  Header syntax failures are distinct from well-formed but unknown stream
  cursors so the HTTP boundary can return validation and expiry errors
  consistently.
  """

  @cursor_pattern ~r/\A[a-zA-Z0-9:_\-.]{1,128}\z/

  @doc "Parses a cursor for the global run stream."
  @spec global(String.t() | nil) :: {:ok, pos_integer() | nil} | {:error, atom()}
  def global(value) do
    with {:ok, value} <- validate(value) do
      parse_global(value)
    end
  end

  @doc "Parses a cursor belonging to `run_id`."
  @spec run(String.t() | nil, String.t()) :: {:ok, non_neg_integer()} | {:error, atom()}
  def run(value, run_id) when is_binary(run_id) do
    with {:ok, value} <- validate(value) do
      parse_run(value, run_id)
    end
  end

  defp validate(nil), do: {:ok, nil}

  defp validate(value) when is_binary(value) do
    case String.trim(value) do
      "" -> {:ok, nil}
      trimmed when byte_size(trimmed) <= 128 -> validate_characters(trimmed)
      _trimmed -> {:error, :invalid_last_event_id}
    end
  end

  defp validate(_value), do: {:error, :invalid_last_event_id}

  defp validate_characters(value) do
    if Regex.match?(@cursor_pattern, value),
      do: {:ok, value},
      else: {:error, :invalid_last_event_id}
  end

  defp parse_global(nil), do: {:ok, nil}

  defp parse_global("global:" <> sequence), do: positive_integer(sequence)
  defp parse_global(_value), do: {:error, :cursor_invalid}

  defp parse_run(nil, _run_id), do: {:ok, 0}

  defp parse_run(value, run_id) do
    case String.split(value, ":", parts: 3) do
      ["run", ^run_id, sequence] -> positive_integer(sequence)
      _other -> {:error, :cursor_invalid}
    end
  end

  defp positive_integer(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> {:ok, integer}
      _other -> {:error, :cursor_invalid}
    end
  end
end
