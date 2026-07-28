defmodule CrmDemo.Support.Landing.Manifest do
  @moduledoc """
  Completion record for one landing extraction.

  The manifest is written last, after every data part. Its presence is the only
  signal that an extraction finished, and its `files` list is the only input
  Source SQL is allowed to read. A failed extraction leaves data parts behind
  with no manifest, so no downstream asset can pick them up.
  """

  @enforce_keys [
    :dataset,
    :landing_run_id,
    :mode,
    :window_start,
    :window_end,
    :extracted_at,
    :row_count,
    :files
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          dataset: String.t(),
          landing_run_id: String.t(),
          mode: String.t(),
          window_start: DateTime.t() | nil,
          window_end: DateTime.t() | nil,
          extracted_at: DateTime.t(),
          row_count: non_neg_integer(),
          files: [String.t()]
        }

  @doc "Encodes a manifest as JSON."
  @spec encode!(t()) :: String.t()
  def encode!(%__MODULE__{} = manifest) do
    manifest
    |> Map.from_struct()
    |> Map.new(fn
      {key, %DateTime{} = at} -> {key, DateTime.to_iso8601(at)}
      {key, value} -> {key, value}
    end)
    |> Jason.encode!(pretty: true)
  end

  @doc "Decodes a manifest from JSON."
  @spec decode!(String.t()) :: t()
  def decode!(json) do
    decoded = Jason.decode!(json)

    %__MODULE__{
      dataset: decoded["dataset"],
      landing_run_id: decoded["landing_run_id"],
      mode: decoded["mode"],
      window_start: decode_datetime(decoded["window_start"]),
      window_end: decode_datetime(decoded["window_end"]),
      extracted_at: decode_datetime(decoded["extracted_at"]),
      row_count: decoded["row_count"],
      files: decoded["files"]
    }
  end

  defp decode_datetime(nil), do: nil

  defp decode_datetime(value) do
    {:ok, at, 0} = DateTime.from_iso8601(value)
    at
  end
end
