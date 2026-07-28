defmodule CrmDemo.Support.Landing.Storage do
  @moduledoc """
  Immutable local file storage for landed data.

  Layout, one directory per landing run:

      .data/landing/<dataset>/<landing_run_id>/part-0001.jsonl
      .data/landing/<dataset>/<landing_run_id>/part-0002.jsonl
      .data/landing/<dataset>/<landing_run_id>/_manifest.json

  Data parts are newline-delimited JSON exactly as the source returned it. A
  retry writes a new run directory rather than touching an existing one, so
  landed data is append-only and every published relation can name the run it
  came from.

  Swap this module for object storage without changing any asset: assets only
  call `write_part!/4`, `write_manifest!/1`, and `latest_manifest!/2`.
  """

  alias CrmDemo.Support.Landing.Manifest

  @doc "Root directory for all landed data."
  @spec root() :: String.t()
  def root, do: Path.expand(Application.fetch_env!(:crm_demo, :landing_dir))

  @doc "Directory holding one landing run's parts and manifest."
  @spec run_dir(String.t(), String.t()) :: String.t()
  def run_dir(dataset, landing_run_id), do: Path.join([root(), dataset, landing_run_id])

  @doc "Writes one immutable data part and returns its absolute path."
  @spec write_part!(String.t(), String.t(), pos_integer(), [map()]) :: String.t()
  def write_part!(dataset, landing_run_id, index, rows) do
    dir = run_dir(dataset, landing_run_id)
    File.mkdir_p!(dir)

    path = Path.join(dir, "part-#{String.pad_leading(to_string(index), 4, "0")}.jsonl")
    File.write!(path, Enum.map_join(rows, "", &(Jason.encode!(&1) <> "\n")))

    path
  end

  @doc "Writes the completion manifest and returns its absolute path."
  @spec write_manifest!(Manifest.t()) :: String.t()
  def write_manifest!(%Manifest{} = manifest) do
    path = Path.join(run_dir(manifest.dataset, manifest.landing_run_id), "_manifest.json")
    File.write!(path, Manifest.encode!(manifest))

    path
  end

  @doc """
  Returns the most recent completed manifest for a dataset and window.

  `window` is `nil` for full-refresh datasets. Raises when no completed
  extraction exists, because publishing a source relation from nothing would
  hide a missing upstream run.
  """
  @spec latest_manifest!(String.t(), Favn.Window.Runtime.t() | nil) :: Manifest.t()
  def latest_manifest!(dataset, window) do
    manifests =
      [root(), dataset, "*", "_manifest.json"]
      |> Path.join()
      |> Path.wildcard()
      |> Enum.map(&(&1 |> File.read!() |> Manifest.decode!()))
      |> Enum.filter(&covers?(&1, window))

    case Enum.max_by(manifests, & &1.extracted_at, DateTime, fn -> nil end) do
      nil -> raise "no completed landing run for #{dataset} in #{inspect(window)}"
      manifest -> manifest
    end
  end

  defp covers?(_manifest, nil), do: true

  defp covers?(manifest, window) do
    manifest.window_start == window.start_at and manifest.window_end == window.end_at
  end
end
