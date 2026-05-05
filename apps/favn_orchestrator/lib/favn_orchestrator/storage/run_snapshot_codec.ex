defmodule FavnOrchestrator.Storage.RunSnapshotCodec do
  @moduledoc false

  alias Favn.Manifest.Identity
  alias Favn.Plan
  alias Favn.Run.AssetResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunStateCodec

  @format "favn.run_snapshot.storage.v1"
  @max_manifest_atom_length 128
  @max_manifest_module_length 512

  # Favn-owned run snapshot atoms are fixed here; consumer module/name atoms come only from
  # the associated manifest record.
  @internal_atom_strings [
    "action",
    "asset",
    "asset_dependencies",
    "asset_ref",
    "asset_results",
    "attempt",
    "attempt_count",
    "attempts",
    "all",
    "anchor_ranges",
    "anchor_window",
    "backfill_range",
    "cancelled",
    "config",
    "dependencies",
    "deps",
    "downstream",
    "duration_ms",
    "error",
    "event_seq",
    "execution_id",
    "finished_at",
    "id",
    "in_flight_execution_ids",
    "inserted_at",
    "kind",
    "lineage_depth",
    "manual",
    "manifest_content_hash",
    "manifest_version_id",
    "max_attempts",
    "meta",
    "metadata",
    "name",
    "next_attempt",
    "next_retry_at",
    "nil",
    "node_key",
    "node_stages",
    "nodes",
    "none",
    "ok",
    "outputs",
    "params",
    "parent_run_id",
    "pipeline",
    "pipeline_context",
    "pipeline_dependencies",
    "pipeline_module",
    "pipeline_submit_ref",
    "pipeline_target_refs",
    "plan",
    "ref",
    "relation",
    "rerun_of_run_id",
    "resolved_refs",
    "result",
    "retry_backoff_ms",
    "retrying",
    "replay_mode",
    "replay_submit_kind",
    "exact_replay",
    "resume_from_failure",
    "root_run_id",
    "run",
    "run_finished",
    "run_kind",
    "runner_execution_id",
    "rows_written",
    "runner_metadata",
    "schedule",
    "snapshot_hash",
    "source",
    "source_run_id",
    "stage",
    "stages",
    "started_at",
    "status",
    "submit_ref",
    "submit_kind",
    "target_node_keys",
    "target_refs",
    "terminal_event_type",
    "timeout_ms",
    "topo_order",
    "trigger",
    "updated_at",
    "upstream",
    "window"
  ]

  @type run_record :: %{
          required(:run_blob) => String.t(),
          required(:manifest_version_id) => String.t()
        }

  @type manifest_record :: %{
          required(:manifest_version_id) => String.t(),
          required(:content_hash) => String.t(),
          required(:manifest_json) => String.t()
        }

  @spec encode_run(RunState.t()) :: {:ok, String.t()} | {:error, term()}
  def encode_run(%RunState{} = run_state) do
    with {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, Jason.encode!(run_to_dto(normalized))}
    end
  rescue
    error -> {:error, {:run_snapshot_encode_failed, error}}
  end

  @spec decode_run(run_record(), manifest_record() | nil) ::
          {:ok, RunState.t()} | {:error, term()}
  def decode_run(%{run_blob: payload, manifest_version_id: manifest_version_id}, manifest_record)
      when is_binary(payload) and is_binary(manifest_version_id) do
    with {:ok, manifest_record} <- validate_manifest_record(manifest_record, manifest_version_id),
         {:ok, allowed_atom_strings} <- allowed_atom_strings(manifest_record),
         {:ok, dto} <- decode_dto(payload),
         {:ok, run_state} <- dto_to_run(dto, allowed_atom_strings),
         :ok <- validate_run_manifest(run_state, manifest_version_id),
         :ok <- validate_run_content_hash(run_state, manifest_record),
         {:ok, normalized} <- RunStateCodec.normalize(run_state) do
      {:ok, normalized}
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_run_payload, other}}
    end
  end

  def decode_run(run_record, _manifest_record), do: {:error, {:invalid_run_record, run_record}}

  defp run_to_dto(%RunState{} = run) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "id" => run.id,
      "manifest_version_id" => run.manifest_version_id,
      "manifest_content_hash" => run.manifest_content_hash,
      "asset_ref" => JsonSafe.ref(run.asset_ref),
      "target_refs" => Enum.map(run.target_refs, &JsonSafe.ref/1),
      "plan" => plan_to_dto(run.plan),
      "status" => Atom.to_string(run.status),
      "event_seq" => run.event_seq,
      "snapshot_hash" => run.snapshot_hash,
      "params" => JsonSafe.data(run.params),
      "trigger" => JsonSafe.data(run.trigger),
      "metadata" => JsonSafe.data(run.metadata),
      "submit_kind" => Atom.to_string(run.submit_kind),
      "rerun_of_run_id" => run.rerun_of_run_id,
      "parent_run_id" => run.parent_run_id,
      "root_run_id" => run.root_run_id,
      "lineage_depth" => run.lineage_depth,
      "max_attempts" => run.max_attempts,
      "retry_backoff_ms" => run.retry_backoff_ms,
      "timeout_ms" => run.timeout_ms,
      "runner_execution_id" => run.runner_execution_id,
      "result" => result_to_dto(run.result),
      "error" => JsonSafe.error(run.error),
      "inserted_at" => datetime_to_dto(run.inserted_at),
      "updated_at" => datetime_to_dto(run.updated_at)
    }
  end

  defp decode_dto(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format} = dto} ->
        {:ok, dto}

      {:ok, other} ->
        {:error, {:invalid_run_snapshot_dto, other}}

      {:error, reason} ->
        {:error, {:invalid_run_snapshot_json, reason}}
    end
  end

  defp dto_to_run(%{"schema_version" => 1} = dto, allowed_atom_strings) do
    with {:ok, asset_ref} <- ref_from_dto(Map.get(dto, "asset_ref"), allowed_atom_strings),
         {:ok, target_refs} <- refs_from_dto(Map.get(dto, "target_refs"), allowed_atom_strings),
         {:ok, plan} <- plan_from_dto(Map.get(dto, "plan"), allowed_atom_strings),
         {:ok, status} <- status_from_dto(Map.get(dto, "status")),
         {:ok, submit_kind} <- submit_kind_from_dto(Map.get(dto, "submit_kind")),
         {:ok, inserted_at} <- datetime_from_dto(Map.get(dto, "inserted_at")),
         {:ok, updated_at} <- datetime_from_dto(Map.get(dto, "updated_at")) do
      {:ok,
       %RunState{
         id: Map.get(dto, "id"),
         manifest_version_id: Map.get(dto, "manifest_version_id"),
         manifest_content_hash: Map.get(dto, "manifest_content_hash"),
         asset_ref: asset_ref,
         target_refs: target_refs,
         plan: plan,
         status: status,
         event_seq: Map.get(dto, "event_seq"),
         snapshot_hash: Map.get(dto, "snapshot_hash"),
         params: json_from_dto(Map.get(dto, "params")),
         trigger: json_from_dto(Map.get(dto, "trigger")),
         metadata: metadata_from_dto(Map.get(dto, "metadata"), allowed_atom_strings),
         submit_kind: submit_kind,
         rerun_of_run_id: empty_to_nil(Map.get(dto, "rerun_of_run_id")),
         parent_run_id: empty_to_nil(Map.get(dto, "parent_run_id")),
         root_run_id: empty_to_nil(Map.get(dto, "root_run_id")),
         lineage_depth: Map.get(dto, "lineage_depth", 0),
         max_attempts: Map.get(dto, "max_attempts", 1),
         retry_backoff_ms: Map.get(dto, "retry_backoff_ms", 0),
         timeout_ms: Map.get(dto, "timeout_ms", 5000),
         runner_execution_id: empty_to_nil(Map.get(dto, "runner_execution_id")),
         result: result_from_dto(Map.get(dto, "result"), allowed_atom_strings),
         error: Map.get(dto, "error"),
         inserted_at: inserted_at,
         updated_at: updated_at
       }}
    end
  end

  defp dto_to_run(dto, _allowed_atom_strings), do: {:error, {:unsupported_run_snapshot_dto, dto}}

  defp plan_to_dto(nil), do: nil

  defp plan_to_dto(%Plan{} = plan) do
    %{
      "target_refs" => Enum.map(plan.target_refs, &JsonSafe.ref/1),
      "target_node_keys" => Enum.map(plan.target_node_keys, &node_key_to_dto/1),
      "dependencies" => Atom.to_string(plan.dependencies),
      "nodes" => Enum.map(plan.nodes, fn {_key, node} -> plan_node_to_dto(node) end),
      "topo_order" => Enum.map(plan.topo_order, &JsonSafe.ref/1),
      "stages" => Enum.map(plan.stages, fn stage -> Enum.map(stage, &JsonSafe.ref/1) end),
      "node_stages" =>
        Enum.map(plan.node_stages, fn stage -> Enum.map(stage, &node_key_to_dto/1) end)
    }
  end

  defp plan_node_to_dto(node) when is_map(node) do
    %{
      "ref" => JsonSafe.ref(Map.get(node, :ref)),
      "node_key" => node_key_to_dto(Map.get(node, :node_key)),
      "window" => JsonSafe.data(Map.get(node, :window)),
      "upstream" => Enum.map(List.wrap(Map.get(node, :upstream)), &node_key_to_dto/1),
      "downstream" => Enum.map(List.wrap(Map.get(node, :downstream)), &node_key_to_dto/1),
      "stage" => Map.get(node, :stage),
      "action" => Map.get(node, :action) |> atom_to_string()
    }
  end

  defp node_key_to_dto({ref, identity}) do
    %{"ref" => JsonSafe.ref(ref), "identity" => JsonSafe.data(identity)}
  end

  defp node_key_to_dto(_value), do: nil

  defp result_to_dto(nil), do: nil

  defp result_to_dto(%{asset_results: results} = result) when is_list(results) do
    result
    |> Map.put(:asset_results, Enum.map(results, &JsonSafe.data/1))
    |> JsonSafe.data()
  end

  defp result_to_dto(result), do: JsonSafe.data(result)

  defp plan_from_dto(nil, _allowed_atom_strings), do: {:ok, nil}

  defp plan_from_dto(%{} = dto, allowed_atom_strings) do
    with {:ok, target_refs} <- refs_from_dto(Map.get(dto, "target_refs"), allowed_atom_strings),
         {:ok, target_node_keys} <-
           node_keys_from_dto(Map.get(dto, "target_node_keys"), allowed_atom_strings),
         {:ok, dependencies} <- dependencies_from_dto(Map.get(dto, "dependencies")),
         {:ok, nodes} <- nodes_from_dto(Map.get(dto, "nodes"), allowed_atom_strings),
         {:ok, topo_order} <- refs_from_dto(Map.get(dto, "topo_order"), allowed_atom_strings),
         {:ok, stages} <- stages_from_dto(Map.get(dto, "stages"), allowed_atom_strings),
         {:ok, node_stages} <-
           node_stages_from_dto(Map.get(dto, "node_stages"), allowed_atom_strings) do
      {:ok,
       %Plan{
         target_refs: target_refs,
         target_node_keys: target_node_keys,
         dependencies: dependencies,
         nodes: nodes,
         topo_order: topo_order,
         stages: stages,
         node_stages: node_stages
       }}
    end
  end

  defp plan_from_dto(_value, _allowed_atom_strings), do: {:ok, nil}

  defp nodes_from_dto(nodes, allowed_atom_strings) when is_list(nodes) do
    Enum.reduce_while(nodes, {:ok, %{}}, fn node, {:ok, acc} ->
      with {:ok, decoded} <- node_from_dto(node, allowed_atom_strings),
           node_key when is_tuple(node_key) <- Map.get(decoded, :node_key) do
        {:cont, {:ok, Map.put(acc, node_key, decoded)}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
        _other -> {:halt, {:error, {:invalid_plan_node, node}}}
      end
    end)
  end

  defp nodes_from_dto(_nodes, _allowed_atom_strings), do: {:ok, %{}}

  defp node_from_dto(%{} = node, allowed_atom_strings) do
    with {:ok, ref} <- ref_from_dto(Map.get(node, "ref"), allowed_atom_strings),
         {:ok, node_key} <- node_key_from_dto(Map.get(node, "node_key"), allowed_atom_strings),
         {:ok, upstream} <- node_keys_from_dto(Map.get(node, "upstream"), allowed_atom_strings),
         {:ok, downstream} <-
           node_keys_from_dto(Map.get(node, "downstream"), allowed_atom_strings),
         {:ok, action} <- action_from_dto(Map.get(node, "action")) do
      {:ok,
       %{
         ref: ref,
         node_key: node_key,
         window: data_from_dto(Map.get(node, "window"), allowed_atom_strings),
         upstream: upstream,
         downstream: downstream,
         stage: Map.get(node, "stage", 0),
         action: action
       }}
    end
  end

  defp node_from_dto(node, _allowed_atom_strings), do: {:error, {:invalid_plan_node, node}}

  defp node_keys_from_dto(values, allowed_atom_strings) when is_list(values) do
    collect_values(values, &node_key_from_dto(&1, allowed_atom_strings))
  end

  defp node_keys_from_dto(_values, _allowed_atom_strings), do: {:ok, []}

  defp node_key_from_dto(nil, _allowed_atom_strings), do: {:ok, nil}

  defp node_key_from_dto(%{"ref" => ref, "identity" => identity}, allowed_atom_strings) do
    with {:ok, decoded_ref} <- ref_from_dto(ref, allowed_atom_strings) do
      {:ok, {decoded_ref, data_from_dto(identity, allowed_atom_strings)}}
    end
  end

  defp node_key_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_node_key, value}}

  defp stages_from_dto(stages, allowed_atom_strings) when is_list(stages) do
    collect_atoms(stages, &refs_from_dto(&1, allowed_atom_strings))
  end

  defp stages_from_dto(_stages, _allowed_atom_strings), do: {:ok, []}

  defp node_stages_from_dto(stages, allowed_atom_strings) when is_list(stages) do
    collect_values(stages, &node_keys_from_dto(&1, allowed_atom_strings))
  end

  defp node_stages_from_dto(_stages, _allowed_atom_strings), do: {:ok, []}

  defp refs_from_dto(values, allowed_atom_strings) when is_list(values) do
    collect_atoms(values, &ref_from_dto(&1, allowed_atom_strings))
  end

  defp refs_from_dto(_values, _allowed_atom_strings), do: {:ok, []}

  defp ref_from_dto(%{"module" => module, "name" => name}, allowed_atom_strings) do
    with {:ok, module_atom} <- atom_from_dto(module, allowed_atom_strings),
         {:ok, name_atom} <- atom_from_dto(name, allowed_atom_strings) do
      {:ok, {module_atom, name_atom}}
    end
  end

  defp ref_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_ref_dto, value}}

  defp atom_from_dto(value, allowed_atom_strings) when is_binary(value) do
    if value in allowed_atom_strings do
      {:ok, String.to_atom(value)}
    else
      {:error, {:unknown_atom, value}}
    end
  end

  defp atom_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_atom_dto, value}}

  defp status_from_dto("pending"), do: {:ok, :pending}
  defp status_from_dto("running"), do: {:ok, :running}
  defp status_from_dto("ok"), do: {:ok, :ok}
  defp status_from_dto("partial"), do: {:ok, :partial}
  defp status_from_dto("error"), do: {:ok, :error}
  defp status_from_dto("cancelled"), do: {:ok, :cancelled}
  defp status_from_dto("timed_out"), do: {:ok, :timed_out}
  defp status_from_dto(value), do: {:error, {:invalid_run_status, value}}

  defp submit_kind_from_dto("manual"), do: {:ok, :manual}
  defp submit_kind_from_dto("rerun"), do: {:ok, :rerun}
  defp submit_kind_from_dto("pipeline"), do: {:ok, :pipeline}
  defp submit_kind_from_dto("backfill_asset"), do: {:ok, :backfill_asset}
  defp submit_kind_from_dto("backfill_pipeline"), do: {:ok, :backfill_pipeline}
  defp submit_kind_from_dto(value), do: {:error, {:invalid_submit_kind, value}}

  defp dependencies_from_dto("all"), do: {:ok, :all}
  defp dependencies_from_dto("none"), do: {:ok, :none}
  defp dependencies_from_dto(nil), do: {:ok, :all}
  defp dependencies_from_dto(value), do: {:error, {:invalid_dependencies, value}}

  defp action_from_dto("run"), do: {:ok, :run}
  defp action_from_dto("observe"), do: {:ok, :observe}
  defp action_from_dto(nil), do: {:ok, :run}
  defp action_from_dto(value), do: {:error, {:invalid_plan_action, value}}

  defp result_from_dto(nil, _allowed_atom_strings), do: nil

  defp result_from_dto(%{} = result, allowed_atom_strings) do
    result = json_from_dto(result)

    result
    |> atomize_known_result(allowed_atom_strings)
  end

  defp result_from_dto(value, _allowed_atom_strings), do: json_from_dto(value)

  defp atomize_known_result(%{} = result, allowed_atom_strings) do
    result
    |> atomize_key("status", :status, &status_value_from_dto/1)
    |> atomize_key(
      "asset_results",
      :asset_results,
      &asset_results_from_dto(&1, allowed_atom_strings)
    )
    |> atomize_key("metadata", :metadata, & &1)
    |> Map.update(:status, Map.get(result, :status), &status_value_from_dto/1)
    |> Map.update(
      :asset_results,
      Map.get(result, :asset_results, []),
      &asset_results_from_dto(&1, allowed_atom_strings)
    )
    |> Map.update(:metadata, Map.get(result, :metadata, %{}), & &1)
  end

  defp atomize_known_result(value, _allowed_atom_strings), do: value

  defp asset_results_from_dto(results, allowed_atom_strings) when is_list(results),
    do: Enum.map(results, &asset_result_from_dto(&1, allowed_atom_strings))

  defp asset_results_from_dto(_results, _allowed_atom_strings), do: []

  defp asset_result_from_dto(%{} = result, allowed_atom_strings) do
    ref = result |> field(:ref) |> ref_from_dto_value(allowed_atom_strings)

    if is_tuple(ref) do
      build_asset_result(result, ref)
    else
      result
    end
  end

  defp asset_result_from_dto(result, _allowed_atom_strings), do: result

  defp build_asset_result(result, ref) do
    %AssetResult{
      ref: ref,
      stage: field(result, :stage, 0),
      status: status_value_from_dto(field(result, :status)),
      started_at: datetime_value_from_dto(field(result, :started_at)),
      finished_at: datetime_value_from_dto(field(result, :finished_at)),
      duration_ms: field(result, :duration_ms, 0),
      meta: field(result, :meta, %{}),
      error: field(result, :error),
      attempt_count: field(result, :attempt_count, 0),
      max_attempts: field(result, :max_attempts, 1),
      attempts: field(result, :attempts, []),
      next_retry_at: datetime_value_from_dto(field(result, :next_retry_at))
    }
  end

  defp data_from_dto(%{} = value, allowed_atom_strings) do
    Map.new(value, fn {key, val} ->
      {known_key(key), data_from_dto(val, allowed_atom_strings)}
    end)
  end

  defp data_from_dto(values, allowed_atom_strings) when is_list(values) do
    Enum.map(values, &data_from_dto(&1, allowed_atom_strings))
  end

  defp data_from_dto(value, _allowed_atom_strings), do: value

  defp metadata_from_dto(value, allowed_atom_strings) do
    value
    |> json_from_dto()
    |> normalize_metadata(allowed_atom_strings)
  end

  defp normalize_metadata(%{} = metadata, allowed_atom_strings) do
    metadata
    |> promote_key("pipeline_context", :pipeline_context)
    |> promote_key("pipeline_submit_ref", :pipeline_submit_ref)
    |> promote_key("pipeline_target_refs", :pipeline_target_refs)
    |> promote_key("pipeline_dependencies", :pipeline_dependencies)
    |> promote_key("asset_dependencies", :asset_dependencies)
    |> promote_key("replay_submit_kind", :replay_submit_kind)
    |> promote_key("replay_mode", :replay_mode)
    |> normalize_metadata_module(:pipeline_submit_ref, allowed_atom_strings)
    |> normalize_metadata_refs(:pipeline_target_refs, allowed_atom_strings)
    |> normalize_metadata_refs(:asset_dependencies, allowed_atom_strings)
    |> normalize_metadata_refs(:pipeline_dependencies, allowed_atom_strings)
    |> normalize_pipeline_context(allowed_atom_strings)
    |> normalize_metadata_atom(:replay_submit_kind, &submit_kind_value_from_dto/1)
    |> normalize_metadata_atom(:replay_mode, &replay_mode_from_dto/1)
  end

  defp normalize_metadata(value, _allowed_atom_strings), do: value

  defp normalize_metadata_module(metadata, key, allowed_atom_strings) when is_map(metadata) do
    case Map.fetch(metadata, key) do
      {:ok, value} ->
        Map.put(metadata, key, atom_from_dto_value(value, allowed_atom_strings))

      :error ->
        metadata
    end
  end

  defp normalize_metadata_refs(metadata, key, allowed_atom_strings) when is_map(metadata) do
    case Map.fetch(metadata, key) do
      {:ok, values} when is_list(values) ->
        Map.put(metadata, key, Enum.map(values, &ref_from_dto_value(&1, allowed_atom_strings)))

      {:ok, value} ->
        Map.put(metadata, key, value)

      :error ->
        metadata
    end
  end

  defp normalize_metadata_atom(metadata, key, fun) when is_map(metadata) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> Map.put(metadata, key, fun.(value))
      :error -> metadata
    end
  end

  defp normalize_pipeline_context(%{pipeline_context: context} = metadata, allowed_atom_strings)
       when is_map(context) do
    context =
      context
      |> atomize_known_context_keys()
      |> normalize_context_module(:module, allowed_atom_strings)
      |> normalize_context_module(:pipeline_module, allowed_atom_strings)
      |> normalize_context_refs(:resolved_refs, allowed_atom_strings)

    Map.put(metadata, :pipeline_context, context)
  end

  defp normalize_pipeline_context(metadata, _allowed_atom_strings), do: metadata

  defp atomize_known_context_keys(context) do
    known_context_keys = %{
      "id" => :id,
      "name" => :name,
      "run_kind" => :run_kind,
      "resolved_refs" => :resolved_refs,
      "deps" => :deps,
      "trigger" => :trigger,
      "schedule" => :schedule,
      "window" => :window,
      "anchor_window" => :anchor_window,
      "backfill_range" => :backfill_range,
      "anchor_ranges" => :anchor_ranges,
      "source" => :source,
      "outputs" => :outputs,
      "module" => :module,
      "pipeline_module" => :pipeline_module
    }

    Enum.reduce(known_context_keys, context, fn {string_key, atom_key}, acc ->
      promote_key(acc, string_key, atom_key)
    end)
  end

  defp normalize_context_module(context, key, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, key) do
      {:ok, value} -> Map.put(context, key, atom_from_dto_value(value, allowed_atom_strings))
      :error -> context
    end
  end

  defp normalize_context_refs(context, key, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, key) do
      {:ok, values} when is_list(values) ->
        Map.put(context, key, Enum.map(values, &ref_from_dto_value(&1, allowed_atom_strings)))

      {:ok, value} ->
        Map.put(context, key, value)

      :error ->
        context
    end
  end

  defp promote_key(map, string_key, atom_key) when is_map(map) do
    case Map.fetch(map, string_key) do
      {:ok, value} -> map |> Map.delete(string_key) |> Map.put_new(atom_key, value)
      :error -> map
    end
  end

  defp json_from_dto(%{} = value) do
    Map.new(value, fn {key, val} -> {key, json_from_dto(val)} end)
  end

  defp json_from_dto(values) when is_list(values), do: Enum.map(values, &json_from_dto/1)
  defp json_from_dto(value), do: value

  defp known_key(key) when key in @internal_atom_strings do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp known_key(key), do: key

  defp collect_values(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, item} -> {:cont, {:ok, acc ++ [item]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp atomize_key(map, string_key, atom_key, mapper) when is_map(map) do
    case Map.fetch(map, string_key) do
      {:ok, value} -> map |> Map.delete(string_key) |> Map.put(atom_key, mapper.(value))
      :error -> map
    end
  end

  defp field(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp status_value_from_dto(value) when is_atom(value), do: value

  defp status_value_from_dto(value) when is_binary(value),
    do: status_from_dto(value) |> elem_or(value)

  defp status_value_from_dto(value), do: value

  defp submit_kind_value_from_dto(value) when is_atom(value), do: value

  defp submit_kind_value_from_dto(value) when is_binary(value),
    do: submit_kind_from_dto(value) |> elem_or(value)

  defp submit_kind_value_from_dto(value), do: value

  defp replay_mode_from_dto("exact_replay"), do: :exact_replay
  defp replay_mode_from_dto("resume_from_failure"), do: :resume_from_failure
  defp replay_mode_from_dto(value), do: value

  defp ref_from_dto_value(value, allowed_atom_strings) do
    case ref_from_dto(value, allowed_atom_strings) do
      {:ok, ref} -> ref
      {:error, _reason} -> value
    end
  end

  defp atom_from_dto_value(value, allowed_atom_strings) do
    case atom_from_dto(value, allowed_atom_strings) do
      {:ok, atom} -> atom
      {:error, _reason} -> value
    end
  end

  defp elem_or({:ok, value}, _fallback), do: value
  defp elem_or({:error, _reason}, fallback), do: fallback

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp datetime_from_dto(nil), do: {:ok, nil}

  defp datetime_from_dto(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _other -> {:error, {:invalid_datetime, value}}
    end
  end

  defp datetime_from_dto(value), do: {:error, {:invalid_datetime, value}}

  defp datetime_value_from_dto(nil), do: nil

  defp datetime_value_from_dto(value) when is_binary(value),
    do: datetime_from_dto(value) |> elem_or(value)

  defp datetime_value_from_dto(value), do: value

  defp atom_to_string(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_to_string(value) when is_binary(value), do: value
  defp atom_to_string(_value), do: nil

  defp empty_to_nil(value) when value in [nil, ""], do: nil
  defp empty_to_nil(value), do: value

  defp validate_manifest_record(nil, manifest_version_id),
    do: {:error, {:missing_manifest_version, manifest_version_id}}

  defp validate_manifest_record(
         %{manifest_version_id: manifest_version_id} = record,
         manifest_version_id
       ),
       do: {:ok, record}

  defp validate_manifest_record(%{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}

  defp validate_manifest_record(record, _manifest_version_id),
    do: {:error, {:invalid_manifest_record, record}}

  defp allowed_atom_strings(manifest_record) do
    with {:ok, manifest_atoms} <- manifest_atom_strings(manifest_record) do
      {:ok, Enum.uniq(@internal_atom_strings ++ manifest_atoms)}
    end
  end

  defp manifest_atom_strings(%{content_hash: content_hash, manifest_json: manifest_json})
       when is_binary(content_hash) and is_binary(manifest_json) do
    with {:ok, decoded} <- decode_manifest_json(manifest_json),
         :ok <- validate_manifest_content_hash(decoded, content_hash),
         {:ok, atoms} <- manifest_atom_strings_from_manifest(decoded) do
      {:ok, Enum.uniq(atoms)}
    end
  end

  defp manifest_atom_strings(record), do: {:error, {:invalid_manifest_record, record}}

  defp decode_manifest_json(manifest_json) do
    case JSON.decode(manifest_json) do
      {:ok, decoded} when is_map(decoded) -> {:ok, decoded}
      {:ok, decoded} -> {:error, {:invalid_manifest_json_root, decoded}}
      {:error, reason} -> {:error, {:invalid_manifest_json, reason}}
    end
  end

  defp validate_manifest_content_hash(decoded_manifest, content_hash) do
    case Identity.hash_manifest(decoded_manifest) do
      {:ok, ^content_hash} -> :ok
      {:ok, computed} -> {:error, {:manifest_content_hash_mismatch, content_hash, computed}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp manifest_atom_strings_from_manifest(%{} = manifest) do
    with {:ok, asset_atoms} <- manifest_records_atoms(Map.get(manifest, "assets"), &asset_atoms/1),
         {:ok, pipeline_atoms} <-
           manifest_records_atoms(Map.get(manifest, "pipelines"), &pipeline_atoms/1),
         {:ok, schedule_atoms} <-
           manifest_records_atoms(Map.get(manifest, "schedules"), &schedule_atoms/1),
         {:ok, graph_atoms} <- graph_atoms(Map.get(manifest, "graph")) do
      {:ok, asset_atoms ++ pipeline_atoms ++ schedule_atoms ++ graph_atoms}
    end
  end

  defp manifest_records_atoms(records, fun) when is_list(records), do: collect_atoms(records, fun)
  defp manifest_records_atoms(_records, _fun), do: {:ok, []}

  defp asset_atoms(%{} = asset) do
    collect_atom_groups([
      module_atom(Map.get(asset, "module")),
      manifest_atom(Map.get(asset, "name")),
      ref_atoms(Map.get(asset, "ref")),
      refs_atoms(Map.get(asset, "depends_on")),
      refs_atoms(Map.get(asset, "relation_inputs"))
    ])
  end

  defp asset_atoms(_asset), do: {:ok, []}

  defp pipeline_atoms(%{} = pipeline) do
    collect_atom_groups([
      module_atom(Map.get(pipeline, "module")),
      manifest_atom(Map.get(pipeline, "name")),
      selector_atoms(Map.get(pipeline, "selectors"))
    ])
  end

  defp pipeline_atoms(_pipeline), do: {:ok, []}

  defp schedule_atoms(%{} = schedule) do
    collect_atom_groups([
      module_atom(Map.get(schedule, "module")),
      manifest_atom(Map.get(schedule, "name")),
      ref_atoms(Map.get(schedule, "pipeline")),
      ref_atoms(Map.get(schedule, "pipeline_ref"))
    ])
  end

  defp schedule_atoms(_schedule), do: {:ok, []}

  defp graph_atoms(%{} = graph) do
    collect_atom_groups([
      refs_atoms(Map.get(graph, "nodes")),
      refs_atoms(Map.get(graph, "topo_order")),
      edge_atoms(Map.get(graph, "edges"))
    ])
  end

  defp graph_atoms(_graph), do: {:ok, []}

  defp selector_atoms(selectors) when is_list(selectors) do
    collect_atoms(selectors, &selector_atom/1)
  end

  defp selector_atoms(_selectors), do: {:ok, []}

  defp selector_atom([_kind, ref]), do: ref_atoms(ref)
  defp selector_atom(%{"value" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"ref" => ref}), do: ref_atoms(ref)
  defp selector_atom(%{"module" => _module, "name" => _name} = ref), do: ref_atoms(ref)
  defp selector_atom(_selector), do: {:ok, []}

  defp edge_atoms(edges) when is_list(edges) do
    collect_atoms(edges, fn edge ->
      case edge do
        [left, right] ->
          collect_atom_groups([ref_atoms(left), ref_atoms(right)])

        %{"from" => left, "to" => right} ->
          collect_atom_groups([ref_atoms(left), ref_atoms(right)])

        _edge ->
          {:ok, []}
      end
    end)
  end

  defp edge_atoms(_edges), do: {:ok, []}

  defp refs_atoms(refs) when is_list(refs), do: collect_atoms(refs, &ref_atoms/1)
  defp refs_atoms(_refs), do: {:ok, []}

  defp ref_atoms(%{"module" => module, "name" => name}) do
    collect_atom_groups([module_atom(module), manifest_atom(name)])
  end

  defp ref_atoms([module, name]),
    do: collect_atom_groups([module_atom(module), manifest_atom(name)])

  defp ref_atoms(_ref), do: {:ok, []}

  defp collect_atoms(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, atoms} -> {:cont, {:ok, acc ++ atoms}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp collect_atom_groups(groups) do
    Enum.reduce_while(groups, {:ok, []}, fn
      {:ok, atoms}, {:ok, acc} -> {:cont, {:ok, acc ++ atoms}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
  end

  defp module_atom(nil), do: {:ok, []}

  defp module_atom(value) when is_binary(value) do
    if valid_manifest_module?(value),
      do: {:ok, [value]},
      else: {:error, {:invalid_manifest_module, value}}
  end

  defp module_atom(value), do: {:error, {:invalid_manifest_module, value}}

  defp manifest_atom(nil), do: {:ok, []}

  defp manifest_atom(value) when is_binary(value) do
    if valid_manifest_atom?(value),
      do: {:ok, [value]},
      else: {:error, {:invalid_manifest_atom, value}}
  end

  defp manifest_atom(value), do: {:error, {:invalid_manifest_atom, value}}

  defp valid_manifest_module?(value) when is_binary(value) do
    byte_size(value) <= @max_manifest_module_length and
      Regex.match?(~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/, value)
  end

  defp valid_manifest_atom?(value) when is_binary(value) do
    byte_size(value) in 1..@max_manifest_atom_length and
      Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/, value)
  end

  defp validate_run_manifest(
         %RunState{manifest_version_id: manifest_version_id},
         manifest_version_id
       ),
       do: :ok

  defp validate_run_manifest(%RunState{manifest_version_id: other}, manifest_version_id),
    do: {:error, {:run_manifest_mismatch, manifest_version_id, other}}

  defp validate_run_content_hash(%RunState{manifest_content_hash: content_hash}, %{
         content_hash: content_hash
       }),
       do: :ok

  defp validate_run_content_hash(%RunState{manifest_content_hash: run_hash}, %{
         content_hash: manifest_hash
       }),
       do: {:error, {:run_manifest_content_hash_mismatch, manifest_hash, run_hash}}
end
