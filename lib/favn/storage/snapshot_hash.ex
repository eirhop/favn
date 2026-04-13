defmodule Favn.Storage.SnapshotHash do
  @moduledoc false

  alias Favn.Run
  alias Favn.Storage.Postgres.RunSerializer

  @spec for_run(Run.t(), keyword()) :: {:ok, String.t()} | {:error, term()}
  def for_run(%Run{} = run, opts \\ []) do
    allow_fallback_term = Keyword.get(opts, :allow_fallback_term, false)

    case snapshot_for_run(run) do
      {:ok, snapshot} -> {:ok, from_snapshot(snapshot)}
      {:error, _reason} when allow_fallback_term -> {:ok, from_term(run)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec snapshot_for_run(Run.t()) :: {:ok, map()} | {:error, term()}
  def snapshot_for_run(%Run{} = run) do
    {:ok, RunSerializer.snapshot_from_run(run)}
  rescue
    error -> {:error, {:serialization_failed, error}}
  end

  @spec from_snapshot(map()) :: String.t()
  def from_snapshot(snapshot) when is_map(snapshot) do
    snapshot
    |> JSON.encode_to_iodata!()
    |> IO.iodata_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec from_term(term()) :: String.t()
  def from_term(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
