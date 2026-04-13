defmodule Favn.Storage.RunSerializer do
  @moduledoc false

  alias Favn.Run
  alias Favn.Storage.Postgres.TermJSON

  @snapshot_version 1

  @type snapshot :: %{
          required(:snapshot_version) => pos_integer(),
          required(:payload) => map()
        }

  @spec snapshot_version() :: pos_integer()
  def snapshot_version, do: @snapshot_version

  @spec snapshot_from_run(Run.t()) :: snapshot()
  def snapshot_from_run(%Run{} = run) do
    %{
      snapshot_version: @snapshot_version,
      payload: TermJSON.encode(run)
    }
  end

  @spec run_from_snapshot(map()) :: {:ok, Run.t()} | {:error, term()}
  def run_from_snapshot(%{"snapshot_version" => @snapshot_version, "payload" => payload})
      when is_map(payload) do
    with {:ok, decoded} <- TermJSON.decode(payload),
         %Run{} = run <- decoded do
      {:ok, run}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_snapshot_payload, other}}
    end
  end

  def run_from_snapshot(%{snapshot_version: @snapshot_version, payload: payload})
      when is_map(payload) do
    with {:ok, decoded} <- TermJSON.decode(payload),
         %Run{} = run <- decoded do
      {:ok, run}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_snapshot_payload, other}}
    end
  end

  def run_from_snapshot(snapshot), do: {:error, {:unsupported_snapshot, snapshot}}
end
