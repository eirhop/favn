defmodule FavnOrchestrator.Backfill.CoverageEvidence do
  @moduledoc """
  Validates coverage evidence carried by successful run snapshots.

  Live projection and repair both use this module so they derive identical
  baseline identifiers and apply the same source-identity safeguards.
  """

  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState

  @required_keys [
    :source_key,
    :segment_key_hash,
    :coverage_until,
    :window_kind,
    :timezone
  ]

  @optional_keys [
    :segment_key_redacted,
    :coverage_start_at,
    :coverage_mode,
    :status,
    :metadata
  ]

  @raw_source_keys [:segment_id, :source_id, :source_secret, :token, :secret]

  @doc "Builds a coverage baseline from a successful run when evidence is present."
  @spec from_run(RunState.t()) ::
          {:ok, CoverageBaseline.t()} | :ignore | {:error, term()}
  def from_run(%RunState{status: :ok} = run) do
    with {:ok, coverage} <- coverage_metadata(run),
         :ok <- reject_raw_source_identity(coverage),
         {:ok, pipeline_module} <- pipeline_module(run) do
      attrs = normalize_attrs(coverage)
      timestamp = run.updated_at || run.inserted_at || DateTime.utc_now()

      CoverageBaseline.new(%{
        baseline_id: baseline_id(run, pipeline_module, attrs),
        pipeline_module: pipeline_module,
        source_key: Map.fetch!(attrs, :source_key),
        segment_key_hash: Map.fetch!(attrs, :segment_key_hash),
        segment_key_redacted: Map.get(attrs, :segment_key_redacted),
        window_kind: Map.fetch!(attrs, :window_kind),
        timezone: Map.fetch!(attrs, :timezone),
        coverage_start_at: Map.get(attrs, :coverage_start_at),
        coverage_until: Map.fetch!(attrs, :coverage_until),
        created_by_run_id: run.id,
        manifest_version_id: run.manifest_version_id,
        status: Map.get(attrs, :status, :ok),
        metadata: baseline_metadata(attrs),
        created_at: timestamp,
        updated_at: timestamp
      })
    end
  end

  def from_run(%RunState{}), do: :ignore

  @doc "Returns the manifest-owned pipeline module recorded by a run."
  @spec pipeline_module(RunState.t()) :: {:ok, module()} | {:error, :missing_pipeline_module}
  def pipeline_module(%RunState{} = run) do
    metadata = run.metadata || %{}
    context = field(metadata, :pipeline_context)

    [
      field(metadata, :pipeline_submit_ref),
      field(context, :module),
      field(context, :pipeline_module),
      field(run.trigger, :pipeline_module)
    ]
    |> Enum.find(&valid_module?/1)
    |> case do
      nil -> {:error, :missing_pipeline_module}
      module -> {:ok, module}
    end
  end

  defp coverage_metadata(%RunState{} = run) do
    [run.result |> field(:metadata) |> field(:coverage), field(run.metadata, :coverage)]
    |> Enum.find(&is_map/1)
    |> case do
      nil -> :ignore
      coverage -> require_keys(coverage)
    end
  end

  defp require_keys(coverage) do
    missing = Enum.filter(@required_keys, &(field(coverage, &1) in [nil, ""]))

    case missing do
      [] -> {:ok, coverage}
      keys -> {:error, {:missing_required_coverage_keys, keys}}
    end
  end

  defp normalize_attrs(coverage) do
    (@required_keys ++ @optional_keys)
    |> Enum.reduce(%{}, fn key, acc ->
      case field(coverage, key) do
        nil -> acc
        value -> Map.put(acc, key, value)
      end
    end)
  end

  defp baseline_metadata(attrs) do
    attrs
    |> Map.get(:metadata, %{})
    |> case do
      metadata when is_map(metadata) -> metadata
      _other -> %{}
    end
    |> maybe_put(:coverage_mode, Map.get(attrs, :coverage_mode))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp reject_raw_source_identity(value) do
    if raw_source_identity?(value), do: {:error, :raw_source_identity_not_allowed}, else: :ok
  end

  defp raw_source_identity?(%{__struct__: _struct}), do: false

  defp raw_source_identity?(value) when is_map(value) do
    Enum.any?(value, fn {key, nested} -> raw_source_key?(key) or raw_source_identity?(nested) end)
  end

  defp raw_source_identity?(value) when is_list(value),
    do: Enum.any?(value, &raw_source_identity?/1)

  defp raw_source_identity?(_value), do: false
  defp raw_source_key?(key) when is_atom(key), do: key in @raw_source_keys

  defp raw_source_key?(key) when is_binary(key),
    do: key in Enum.map(@raw_source_keys, &to_string/1)

  defp raw_source_key?(_key), do: false

  defp baseline_id(%RunState{} = run, pipeline_module, attrs) do
    hash_input =
      {:coverage_baseline, pipeline_module, Map.fetch!(attrs, :source_key),
       Map.fetch!(attrs, :segment_key_hash), Map.fetch!(attrs, :window_kind),
       Map.fetch!(attrs, :timezone), Map.fetch!(attrs, :coverage_until), run.manifest_version_id}

    hash = :crypto.hash(:sha256, :erlang.term_to_binary(hash_input))
    "baseline_" <> Base.encode16(hash, case: :lower)
  end

  defp valid_module?(module), do: is_atom(module) and not is_nil(module)

  defp field(value, key) when is_map(value) and is_atom(key),
    do: Map.get(value, key) || Map.get(value, Atom.to_string(key))

  defp field(_value, _key), do: nil
end
