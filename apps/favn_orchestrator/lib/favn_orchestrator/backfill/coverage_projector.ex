defmodule FavnOrchestrator.Backfill.CoverageProjector do
  @moduledoc """
  Projects successful full-load or baseline run metadata into coverage baselines.

  Coverage projection is intentionally best-effort derived state. The run
  transition remains authoritative and must not fail because optional coverage
  metadata is absent or invalid.

  A successful run can opt into projection by returning coverage metadata under
  `result.metadata.coverage` or by carrying it in run metadata. Required fields
  are `source_key`, `segment_key_hash`, `coverage_until`, `window_kind`, and
  `timezone`. Raw source identifiers and secrets are rejected.
  """

  alias FavnOrchestrator.Backfill.CoverageBaseline
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage

  @required_coverage_keys [
    :source_key,
    :segment_key_hash,
    :coverage_until,
    :window_kind,
    :timezone
  ]

  @optional_coverage_keys [
    :segment_key_redacted,
    :coverage_start_at,
    :coverage_mode,
    :status,
    :metadata
  ]

  @raw_source_keys [:segment_id, :source_id, :source_secret, :token, :secret]

  @spec project_transition(RunState.t(), atom(), map()) :: :ok | {:error, term()}
  def project_transition(%RunState{} = run_state, :run_finished, _data) do
    if run_state.status == :ok do
      project_successful_run(run_state)
    else
      :ok
    end
  end

  def project_transition(%RunState{}, _event_type, _data), do: :ok

  defp project_successful_run(%RunState{} = run_state) do
    with {:ok, coverage} <- coverage_metadata(run_state),
         :ok <- reject_raw_source_identity(coverage),
         {:ok, pipeline_module} <- pipeline_module(run_state),
         {:ok, baseline} <- coverage_baseline(run_state, pipeline_module, coverage) do
      Storage.put_coverage_baseline(baseline)
    else
      :ignore -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp coverage_baseline(%RunState{} = run_state, pipeline_module, coverage) do
    attrs = normalize_coverage_attrs(coverage)
    timestamp = run_state.updated_at || run_state.inserted_at || DateTime.utc_now()

    CoverageBaseline.new(%{
      baseline_id: baseline_id(run_state, pipeline_module, attrs),
      pipeline_module: pipeline_module,
      source_key: Map.fetch!(attrs, :source_key),
      segment_key_hash: Map.fetch!(attrs, :segment_key_hash),
      segment_key_redacted: Map.get(attrs, :segment_key_redacted),
      window_kind: Map.fetch!(attrs, :window_kind),
      timezone: Map.fetch!(attrs, :timezone),
      coverage_start_at: Map.get(attrs, :coverage_start_at),
      coverage_until: Map.fetch!(attrs, :coverage_until),
      created_by_run_id: run_state.id,
      manifest_version_id: run_state.manifest_version_id,
      status: Map.get(attrs, :status, :ok),
      metadata: baseline_metadata(attrs),
      created_at: timestamp,
      updated_at: timestamp
    })
  end

  defp normalize_coverage_attrs(coverage) do
    (@required_coverage_keys ++ @optional_coverage_keys)
    |> Enum.reduce(%{}, fn key, acc ->
      case coverage_value(coverage, key) do
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

  defp coverage_metadata(%RunState{} = run_state) do
    [
      result_coverage(run_state.result),
      metadata_coverage(run_state.metadata)
    ]
    |> Enum.find(&is_map/1)
    |> case do
      nil -> :ignore
      coverage -> require_coverage_keys(coverage)
    end
  end

  defp result_coverage(result) do
    result
    |> field(:metadata)
    |> metadata_coverage()
  end

  defp metadata_coverage(metadata), do: field(metadata, :coverage)

  defp require_coverage_keys(coverage) do
    missing = Enum.filter(@required_coverage_keys, &(coverage_value(coverage, &1) in [nil, ""]))

    case missing do
      [] -> {:ok, coverage}
      _keys -> :ignore
    end
  end

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

  defp raw_source_key?(key) when is_binary(key) do
    Enum.any?(@raw_source_keys, &(Atom.to_string(&1) == key))
  end

  defp raw_source_key?(_key), do: false

  defp pipeline_module(%RunState{metadata: metadata}) when is_map(metadata) do
    cond do
      valid_module?(field(metadata, :pipeline_submit_ref)) ->
        {:ok, field(metadata, :pipeline_submit_ref)}

      valid_module?(field(field(metadata, :pipeline_context), :module)) ->
        {:ok, field(field(metadata, :pipeline_context), :module)}

      valid_module?(field(field(metadata, :pipeline_context), :pipeline_module)) ->
        {:ok, field(field(metadata, :pipeline_context), :pipeline_module)}

      true ->
        :ignore
    end
  end

  defp pipeline_module(%RunState{}), do: :ignore

  defp valid_module?(module), do: is_atom(module) and not is_nil(module)

  defp baseline_id(%RunState{} = run_state, pipeline_module, attrs) do
    hash_input =
      {:coverage_baseline, pipeline_module, Map.fetch!(attrs, :source_key),
       Map.fetch!(attrs, :segment_key_hash), Map.fetch!(attrs, :window_kind),
       Map.fetch!(attrs, :timezone), Map.fetch!(attrs, :coverage_until),
       run_state.manifest_version_id}

    "baseline_" <>
      (hash_input
       |> :erlang.term_to_binary()
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end

  defp coverage_value(coverage, key), do: field(coverage, key)

  defp field(value, key) when is_map(value) and is_atom(key) do
    Map.get(value, key) || Map.get(value, Atom.to_string(key))
  end

  defp field(_value, _key), do: nil
end
