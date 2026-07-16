defmodule FavnOrchestrator.Storage.RunSnapshotCodec do
  @moduledoc false

  alias Favn.Plan
  alias Favn.Retry.Policy, as: RetryPolicy
  alias Favn.Run.AssetResult
  alias Favn.Run.NodeResult
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.JsonSafe
  alias FavnOrchestrator.Storage.RunSnapshotCodec.ManifestAtoms
  alias FavnOrchestrator.Storage.RunStateCodec

  @format "favn.run_snapshot.storage.v1"
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
    "backoff",
    "cancelled",
    "blocked",
    "config",
    "dependencies",
    "deps",
    "downstream",
    "duration_ms",
    "error",
    "exponential",
    "event_seq",
    "execution_id",
    "execution_pool",
    "finished_at",
    "fixed",
    "freshness_key",
    "id",
    "in_flight_execution_ids",
    "initial_ms",
    "input_versions",
    "inserted_at",
    "kind",
    "lineage_depth",
    "manual",
    "manifest_content_hash",
    "manifest_version_id",
    "max_attempts",
    "max_ms",
    "meta",
    "metadata",
    "name",
    "next_attempt",
    "next_retry_at",
    "nil",
    "node_key",
    "node_results",
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
    "retry_policy",
    "retry_policy_source",
    "retrying",
    "retry_state",
    "reason",
    "replay_mode",
    "replay_submit_kind",
    "exact_replay",
    "resume_from_failure",
    "fresh_rerun",
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
    "sequential",
    "sequential_index",
    "operator",
    "default",
    "stage",
    "stage_index",
    "stages",
    "skipped_fresh",
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
    "window",
    "jitter"
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
      "metadata" => run_metadata_to_dto(run.metadata),
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

  defp run_metadata_to_dto(metadata) when is_map(metadata) do
    encoded = JsonSafe.data(metadata)

    case field(metadata, :pipeline_context) do
      context when is_map(context) ->
        encoded_context =
          context
          |> JsonSafe.data()
          |> Map.put("settings", settings_to_dto(field(context, :settings, %{})))
          |> Map.put("metadata", pipeline_metadata_to_dto(field(context, :metadata, %{})))

        Map.put(encoded, "pipeline_context", encoded_context)

      _other ->
        encoded
    end
  end

  defp run_metadata_to_dto(metadata), do: JsonSafe.data(metadata)

  defp settings_to_dto(settings) do
    settings
    |> Favn.Settings.normalize!()
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp pipeline_metadata_to_dto(metadata) when is_map(metadata) do
    Favn.Settings.normalize!(metadata: metadata).metadata
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
         {:ok, updated_at} <- datetime_from_dto(Map.get(dto, "updated_at")),
         {:ok, result} <- result_from_dto(Map.get(dto, "result"), allowed_atom_strings) do
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
         timeout_ms: Map.get(dto, "timeout_ms", RunState.default_timeout_ms()),
         runner_execution_id: empty_to_nil(Map.get(dto, "runner_execution_id")),
         result: result,
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
      "execution_pool" => Map.get(node, :execution_pool) |> atom_to_string(),
      "action" => Map.get(node, :action) |> atom_to_string(),
      "retry_policy" => retry_policy_to_dto(Map.get(node, :retry_policy)),
      "retry_policy_source" => Map.get(node, :retry_policy_source) |> atom_to_string()
    }
  end

  defp node_key_to_dto({ref, identity}) do
    %{"ref" => JsonSafe.ref(ref), "identity" => JsonSafe.data(identity)}
  end

  defp node_key_to_dto(_value), do: nil

  defp result_to_dto(nil), do: nil

  defp result_to_dto(%{} = result) do
    result
    |> JsonSafe.data()
    |> put_encoded_result_list(result, :asset_results, &JsonSafe.data/1)
    |> put_encoded_result_list(result, :node_results, &node_result_to_dto/1)
  end

  defp result_to_dto(result), do: JsonSafe.data(result)

  defp put_encoded_result_list(dto, result, key, mapper) when is_map(dto) do
    case Map.get(result, key) || Map.get(result, Atom.to_string(key)) do
      results when is_list(results) ->
        Map.put(dto, Atom.to_string(key), Enum.map(results, mapper))

      _other ->
        dto
    end
  end

  defp node_result_to_dto(%NodeResult{} = result) do
    %{
      "node_key" => node_key_to_dto(result.node_key),
      "ref" => JsonSafe.ref(result.ref),
      "window" => JsonSafe.data(result.window),
      "stage" => result.stage,
      "execution_pool" => atom_to_string(result.execution_pool),
      "status" => atom_to_string(result.status),
      "started_at" => datetime_to_dto(result.started_at),
      "finished_at" => datetime_to_dto(result.finished_at),
      "duration_ms" => result.duration_ms,
      "reason" => JsonSafe.data(result.reason),
      "freshness_key" => result.freshness_key,
      "input_versions" => JsonSafe.data(result.input_versions),
      "attempt_count" => result.attempt_count,
      "max_attempts" => result.max_attempts,
      "runner_execution_id" => JsonSafe.data(result.runner_execution_id),
      "asset_step_id" => result.asset_step_id,
      "meta" => JsonSafe.output_metadata(result.meta),
      "error" => JsonSafe.error(result.error),
      "attempts" => JsonSafe.data(result.attempts)
    }
  end

  defp node_result_to_dto(%{} = result) do
    result
    |> Map.put(
      :node_key,
      node_key_to_dto(Map.get(result, :node_key) || Map.get(result, "node_key"))
    )
    |> Map.put(:ref, JsonSafe.ref(Map.get(result, :ref) || Map.get(result, "ref")))
    |> JsonSafe.data()
  end

  defp node_result_to_dto(result), do: JsonSafe.data(result)

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

  defp plan_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_plan_dto, value}}

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

  defp nodes_from_dto(nodes, _allowed_atom_strings), do: {:error, {:invalid_plan_nodes, nodes}}

  defp node_from_dto(%{} = node, allowed_atom_strings) do
    with {:ok, ref} <- ref_from_dto(Map.get(node, "ref"), allowed_atom_strings),
         {:ok, node_key} <- node_key_from_dto(Map.get(node, "node_key"), allowed_atom_strings),
         {:ok, upstream} <- node_keys_from_dto(Map.get(node, "upstream"), allowed_atom_strings),
         {:ok, downstream} <-
           node_keys_from_dto(Map.get(node, "downstream"), allowed_atom_strings),
         {:ok, execution_pool} <-
           optional_atom_from_dto(Map.get(node, "execution_pool"), allowed_atom_strings),
         {:ok, action} <- action_from_dto(Map.get(node, "action")),
         {:ok, retry_policy} <- retry_policy_from_dto(Map.get(node, "retry_policy")),
         {:ok, retry_policy_source} <-
           retry_policy_source_from_dto(Map.get(node, "retry_policy_source")) do
      {:ok,
       %{
         ref: ref,
         node_key: node_key,
         window: data_from_dto(Map.get(node, "window"), allowed_atom_strings),
         upstream: upstream,
         downstream: downstream,
         stage: Map.get(node, "stage", 0),
         execution_pool: execution_pool,
         action: action,
         retry_policy: retry_policy,
         retry_policy_source: retry_policy_source
       }}
    end
  end

  defp node_from_dto(node, _allowed_atom_strings), do: {:error, {:invalid_plan_node, node}}

  defp retry_policy_to_dto(nil), do: nil

  defp retry_policy_to_dto(%RetryPolicy{} = policy) do
    %{
      "max_attempts" => policy.max_attempts,
      "backoff" => %{
        "strategy" => Atom.to_string(policy.backoff.strategy),
        "initial_ms" => policy.backoff.initial_ms,
        "max_ms" => policy.backoff.max_ms,
        "jitter" => policy.backoff.jitter
      }
    }
  end

  defp retry_policy_from_dto(nil), do: {:ok, RetryPolicy.default()}
  defp retry_policy_from_dto(dto), do: RetryPolicy.new(dto)

  defp retry_policy_source_from_dto(nil), do: {:ok, :default}

  defp retry_policy_source_from_dto(source)
       when source in ["operator", "asset", "pipeline", "default"] do
    {:ok, String.to_existing_atom(source)}
  end

  defp retry_policy_source_from_dto(source),
    do: {:error, {:invalid_retry_policy_source, source}}

  defp node_keys_from_dto(values, allowed_atom_strings) when is_list(values) do
    collect_values(values, &node_key_from_dto(&1, allowed_atom_strings))
  end

  defp node_keys_from_dto(values, _allowed_atom_strings),
    do: {:error, {:invalid_node_keys, values}}

  defp node_key_from_dto(nil, _allowed_atom_strings), do: {:ok, nil}

  defp node_key_from_dto(%{"ref" => ref, "identity" => identity}, allowed_atom_strings) do
    with {:ok, decoded_ref} <- ref_from_dto(ref, allowed_atom_strings) do
      {:ok, {decoded_ref, data_from_dto(identity, allowed_atom_strings)}}
    end
  end

  defp node_key_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_node_key, value}}

  defp stages_from_dto(stages, allowed_atom_strings) when is_list(stages) do
    collect_values(stages, &refs_from_dto(&1, allowed_atom_strings))
  end

  defp stages_from_dto(stages, _allowed_atom_strings), do: {:error, {:invalid_stages, stages}}

  defp node_stages_from_dto(stages, allowed_atom_strings) when is_list(stages) do
    collect_values(stages, &node_keys_from_dto(&1, allowed_atom_strings))
  end

  defp node_stages_from_dto(stages, _allowed_atom_strings),
    do: {:error, {:invalid_node_stages, stages}}

  defp refs_from_dto(values, allowed_atom_strings) when is_list(values) do
    collect_values(values, &ref_from_dto(&1, allowed_atom_strings))
  end

  defp refs_from_dto(values, _allowed_atom_strings), do: {:error, {:invalid_refs, values}}

  defp ref_from_dto(%{"module" => module, "name" => name}, allowed_atom_strings) do
    with {:ok, module_atom} <- atom_from_dto(module, allowed_atom_strings),
         {:ok, name_atom} <- atom_from_dto(name, allowed_atom_strings) do
      {:ok, {module_atom, name_atom}}
    end
  end

  defp ref_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_ref_dto, value}}

  defp atom_from_dto(value, allowed_atom_strings) when is_binary(value) do
    if MapSet.member?(allowed_atom_strings, value) do
      existing_atom(value)
    else
      {:error, {:unknown_atom, value}}
    end
  end

  defp atom_from_dto(value, _allowed_atom_strings), do: {:error, {:invalid_atom_dto, value}}

  defp optional_atom_from_dto(nil, _allowed_atom_strings), do: {:ok, nil}

  defp optional_atom_from_dto(value, allowed_atom_strings),
    do: atom_from_dto(value, allowed_atom_strings)

  defp existing_atom(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:atom_not_loaded, value}}
  end

  defp status_from_dto("pending"), do: {:ok, :pending}
  defp status_from_dto("running"), do: {:ok, :running}
  defp status_from_dto("retrying"), do: {:ok, :retrying}
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

  defp result_from_dto(nil, _allowed_atom_strings), do: {:ok, nil}

  defp result_from_dto(%{} = result, allowed_atom_strings) do
    with {:ok, asset_results} <-
           result_collection_from_dto(
             result,
             "asset_results",
             :asset_results,
             allowed_atom_strings,
             &asset_result_from_dto/2
           ),
         {:ok, node_results} <-
           result_collection_from_dto(
             result,
             "node_results",
             :node_results,
             allowed_atom_strings,
             &node_result_from_dto/2
           ) do
      decoded =
        result
        |> json_from_dto()
        |> atomize_key("status", :status, &status_value_from_dto/1)
        |> atomize_key("metadata", :metadata, & &1)
        |> put_result_collection("asset_results", :asset_results, asset_results)
        |> put_result_collection("node_results", :node_results, node_results)

      {:ok, decoded}
    end
  end

  defp result_from_dto(value, _allowed_atom_strings), do: {:ok, json_from_dto(value)}

  defp asset_result_from_dto(%{} = result, allowed_atom_strings) do
    with {:ok, ref} <- ref_from_dto(field(result, :ref), allowed_atom_strings),
         {:ok, status} <- result_status(field(result, :status), :asset_results),
         {:ok, stage} <- result_non_negative(field(result, :stage, 0), :asset_results, :stage),
         {:ok, started_at} <-
           result_datetime(field(result, :started_at), :asset_results, :started_at),
         {:ok, finished_at} <-
           result_datetime(field(result, :finished_at), :asset_results, :finished_at),
         {:ok, duration_ms} <-
           result_non_negative(field(result, :duration_ms, 0), :asset_results, :duration_ms),
         {:ok, meta} <- result_map(field(result, :meta, %{}), :asset_results, :meta),
         {:ok, attempt_count} <-
           result_non_negative(
             field(result, :attempt_count, 0),
             :asset_results,
             :attempt_count
           ),
         {:ok, max_attempts} <-
           result_positive(field(result, :max_attempts, 1), :asset_results, :max_attempts),
         {:ok, attempts} <-
           result_map_list(field(result, :attempts, []), :asset_results, :attempts),
         {:ok, next_retry_at} <-
           result_datetime(field(result, :next_retry_at), :asset_results, :next_retry_at),
         {:ok, asset_step_id} <-
           result_optional_string(field(result, :asset_step_id), :asset_results, :asset_step_id) do
      {:ok,
       %AssetResult{
         ref: ref,
         stage: stage,
         status: status,
         started_at: started_at,
         finished_at: finished_at,
         duration_ms: duration_ms,
         meta: meta,
         error: field(result, :error),
         attempt_count: attempt_count,
         max_attempts: max_attempts,
         attempts: attempts,
         next_retry_at: next_retry_at,
         asset_step_id: asset_step_id
       }}
    end
  end

  defp asset_result_from_dto(result, _allowed_atom_strings),
    do: {:error, {:invalid_result_entry, :asset_results, result}}

  defp node_result_from_dto(%{} = result, allowed_atom_strings) do
    with {:ok, node_key} <- required_node_key(field(result, :node_key), allowed_atom_strings),
         {:ok, ref} <- ref_from_dto(field(result, :ref), allowed_atom_strings),
         {:ok, status} <- result_status(field(result, :status), :node_results),
         {:ok, stage} <- result_non_negative(field(result, :stage, 0), :node_results, :stage),
         {:ok, execution_pool} <-
           result_execution_pool(field(result, :execution_pool), allowed_atom_strings),
         {:ok, started_at} <-
           result_datetime(field(result, :started_at), :node_results, :started_at),
         {:ok, finished_at} <-
           result_datetime(field(result, :finished_at), :node_results, :finished_at),
         {:ok, duration_ms} <-
           result_optional_non_negative(
             field(result, :duration_ms),
             :node_results,
             :duration_ms
           ),
         {:ok, window} <- result_optional_map(field(result, :window), :node_results, :window),
         {:ok, freshness_key} <-
           result_optional_string(field(result, :freshness_key), :node_results, :freshness_key),
         {:ok, input_versions} <-
           result_map_or_list(
             field(result, :input_versions, %{}),
             :node_results,
             :input_versions
           ),
         {:ok, attempt_count} <-
           result_non_negative(
             field(result, :attempt_count, 0),
             :node_results,
             :attempt_count
           ),
         {:ok, max_attempts} <-
           result_positive(field(result, :max_attempts, 1), :node_results, :max_attempts),
         {:ok, meta} <- result_map(field(result, :meta, %{}), :node_results, :meta),
         {:ok, attempts} <-
           result_map_list(field(result, :attempts, []), :node_results, :attempts),
         {:ok, asset_step_id} <-
           result_optional_string(field(result, :asset_step_id), :node_results, :asset_step_id) do
      {:ok,
       %NodeResult{
         node_key: node_key,
         ref: ref,
         window: data_from_dto(window, allowed_atom_strings),
         stage: stage,
         execution_pool: execution_pool,
         status: status,
         started_at: started_at,
         finished_at: finished_at,
         duration_ms: duration_ms,
         reason: data_from_dto(field(result, :reason), allowed_atom_strings),
         freshness_key: freshness_key,
         input_versions: data_from_dto(input_versions, allowed_atom_strings),
         attempt_count: attempt_count,
         max_attempts: max_attempts,
         runner_execution_id:
           data_from_dto(field(result, :runner_execution_id), allowed_atom_strings),
         asset_step_id: asset_step_id,
         meta: data_from_dto(meta, allowed_atom_strings),
         error: field(result, :error),
         attempts: data_from_dto(attempts, allowed_atom_strings)
       }}
    end
  end

  defp node_result_from_dto(result, _allowed_atom_strings),
    do: {:error, {:invalid_result_entry, :node_results, result}}

  defp result_collection_from_dto(
         result,
         string_key,
         atom_key,
         allowed_atom_strings,
         mapper
       ) do
    case Map.fetch(result, string_key) do
      :error ->
        {:ok, :absent}

      {:ok, values} when is_list(values) ->
        values
        |> collect_values(&mapper.(&1, allowed_atom_strings))
        |> wrap_result_entry_error(atom_key)

      {:ok, value} ->
        {:error, {:invalid_result_collection, atom_key, value}}
    end
  end

  defp wrap_result_entry_error({:ok, values}, _field), do: {:ok, values}

  defp wrap_result_entry_error(
         {:error, {:invalid_result_entry, field, value}},
         field
       ),
       do: {:error, {:invalid_result_entry, field, value}}

  defp wrap_result_entry_error({:error, reason}, field),
    do: {:error, {:invalid_result_entry, field, reason}}

  defp put_result_collection(result, _string_key, _atom_key, :absent), do: result

  defp put_result_collection(result, string_key, atom_key, values) do
    result |> Map.delete(string_key) |> Map.put(atom_key, values)
  end

  defp required_node_key(value, allowed_atom_strings) do
    case node_key_from_dto(value, allowed_atom_strings) do
      {:ok, node_key} when is_tuple(node_key) -> {:ok, node_key}
      _invalid -> invalid_result_field(:node_results, :node_key, value)
    end
  end

  defp result_status("skipped_fresh", :node_results), do: {:ok, :skipped_fresh}
  defp result_status("blocked", :node_results), do: {:ok, :blocked}

  defp result_status(value, collection) do
    allowed =
      case collection do
        :asset_results -> [:running, :retrying, :ok, :error, :cancelled, :timed_out]
        :node_results -> [:running, :retrying, :ok, :error, :cancelled, :timed_out]
      end

    case status_from_dto(value) do
      {:ok, status} ->
        if status in allowed,
          do: {:ok, status},
          else: invalid_result_field(collection, :status, value)

      _invalid ->
        invalid_result_field(collection, :status, value)
    end
  end

  defp result_datetime(value, collection, field) do
    case datetime_from_dto(value) do
      {:ok, datetime} -> {:ok, datetime}
      {:error, _reason} -> invalid_result_field(collection, field, value)
    end
  end

  defp result_non_negative(value, _collection, _field)
       when is_integer(value) and value >= 0,
       do: {:ok, value}

  defp result_non_negative(value, collection, field),
    do: invalid_result_field(collection, field, value)

  defp result_optional_non_negative(nil, _collection, _field), do: {:ok, nil}

  defp result_optional_non_negative(value, collection, field),
    do: result_non_negative(value, collection, field)

  defp result_positive(value, _collection, _field) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp result_positive(value, collection, field),
    do: invalid_result_field(collection, field, value)

  defp result_map(value, _collection, _field) when is_map(value), do: {:ok, value}
  defp result_map(value, collection, field), do: invalid_result_field(collection, field, value)

  defp result_optional_map(nil, _collection, _field), do: {:ok, nil}
  defp result_optional_map(value, collection, field), do: result_map(value, collection, field)

  defp result_map_or_list(value, _collection, _field) when is_map(value) or is_list(value),
    do: {:ok, value}

  defp result_map_or_list(value, collection, field),
    do: invalid_result_field(collection, field, value)

  defp result_map_list(value, collection, field) when is_list(value) do
    if Enum.all?(value, &is_map/1),
      do: {:ok, value},
      else: invalid_result_field(collection, field, value)
  end

  defp result_map_list(value, collection, field),
    do: invalid_result_field(collection, field, value)

  defp result_optional_string(nil, _collection, _field), do: {:ok, nil}

  defp result_optional_string(value, _collection, _field) when is_binary(value) and value != "",
    do: {:ok, value}

  defp result_optional_string(value, collection, field),
    do: invalid_result_field(collection, field, value)

  defp result_execution_pool(nil, _allowed_atom_strings), do: {:ok, nil}

  defp result_execution_pool(value, allowed_atom_strings) do
    case atom_from_dto(value, allowed_atom_strings) do
      {:ok, pool} -> {:ok, pool}
      {:error, _reason} -> invalid_result_field(:node_results, :execution_pool, value)
    end
  end

  defp invalid_result_field(collection, field, value),
    do: {:error, {:invalid_result_field, collection, field, value}}

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
    |> promote_key("in_flight_execution_ids", :in_flight_execution_ids)
    |> promote_key("replay_submit_kind", :replay_submit_kind)
    |> promote_key("replay_mode", :replay_mode)
    |> promote_key("retry_state", :retry_state)
    |> normalize_metadata_module(:pipeline_submit_ref, allowed_atom_strings)
    |> normalize_metadata_refs(:pipeline_target_refs, allowed_atom_strings)
    |> normalize_metadata_refs(:asset_dependencies, allowed_atom_strings)
    |> normalize_metadata_refs(:pipeline_dependencies, allowed_atom_strings)
    |> normalize_pipeline_context(allowed_atom_strings)
    |> normalize_metadata_atom(:replay_submit_kind, &submit_kind_value_from_dto/1)
    |> normalize_metadata_atom(:replay_mode, &replay_mode_from_dto/1)
    |> normalize_retry_state(allowed_atom_strings)
  end

  defp normalize_metadata(value, _allowed_atom_strings), do: value

  defp normalize_retry_state(%{retry_state: state} = metadata, allowed_atom_strings)
       when is_map(state) do
    kind = field(state, :kind)
    retry = field(state, :retry, %{})

    normalized_retry =
      retry
      |> Map.new(fn {key, value} -> {known_key(to_string(key)), value} end)
      |> normalize_retry_node_keys(allowed_atom_strings)
      |> normalize_retry_node_key(allowed_atom_strings)
      |> normalize_retry_ref(allowed_atom_strings)

    normalized =
      state
      |> Map.new(fn {key, value} -> {known_key(to_string(key)), value} end)
      |> Map.put(:kind, retry_kind(kind))
      |> Map.put(:retry, normalized_retry)

    Map.put(metadata, :retry_state, normalized)
  end

  defp normalize_retry_state(metadata, _allowed_atom_strings), do: metadata

  defp normalize_retry_node_keys(%{node_keys: values} = retry, allowed_atom_strings)
       when is_list(values) do
    Map.put(retry, :node_keys, Enum.map(values, &metadata_node_key(&1, allowed_atom_strings)))
  end

  defp normalize_retry_node_keys(retry, _allowed_atom_strings), do: retry

  defp normalize_retry_node_key(%{node_key: value} = retry, allowed_atom_strings),
    do: Map.put(retry, :node_key, metadata_node_key(value, allowed_atom_strings))

  defp normalize_retry_node_key(retry, _allowed_atom_strings), do: retry

  defp normalize_retry_ref(%{asset_ref: value} = retry, allowed_atom_strings),
    do: Map.put(retry, :asset_ref, ref_from_dto_value(value, allowed_atom_strings))

  defp normalize_retry_ref(retry, _allowed_atom_strings), do: retry

  defp metadata_node_key([ref, identity], allowed_atom_strings),
    do:
      {ref_from_dto_value(ref, allowed_atom_strings),
       data_from_dto(identity, allowed_atom_strings)}

  defp metadata_node_key(value, _allowed_atom_strings), do: value

  defp retry_kind("sequential"), do: :sequential
  defp retry_kind("pipeline"), do: :pipeline
  defp retry_kind(kind), do: kind

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
      |> normalize_context_atom(:name, allowed_atom_strings)
      |> normalize_context_atom(:dependencies, allowed_atom_strings)
      |> normalize_context_atom(:execution_pool, allowed_atom_strings)
      |> normalize_context_atom(:source, allowed_atom_strings)
      |> normalize_context_atoms(:outputs, allowed_atom_strings)
      |> normalize_context_settings(allowed_atom_strings)
      |> normalize_context_refs(:resolved_refs, allowed_atom_strings)
      |> normalize_context_schedule(allowed_atom_strings)

    Map.put(metadata, :pipeline_context, context)
  end

  defp normalize_pipeline_context(metadata, _allowed_atom_strings), do: metadata

  defp atomize_known_context_keys(context) do
    known_context_keys = %{
      "module" => :module,
      "name" => :name,
      "resolved_refs" => :resolved_refs,
      "dependencies" => :dependencies,
      "settings" => :settings,
      "metadata" => :metadata,
      "trigger" => :trigger,
      "schedule" => :schedule,
      "window" => :window,
      "max_concurrency" => :max_concurrency,
      "execution_pool" => :execution_pool,
      "anchor_window" => :anchor_window,
      "backfill_range" => :backfill_range,
      "anchor_ranges" => :anchor_ranges,
      "source" => :source,
      "outputs" => :outputs
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

  defp normalize_context_atom(context, key, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, key) do
      {:ok, nil} -> context
      {:ok, value} -> Map.put(context, key, atom_from_dto_value(value, allowed_atom_strings))
      :error -> context
    end
  end

  defp normalize_context_atoms(context, key, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, key) do
      {:ok, values} when is_list(values) ->
        Map.put(context, key, Enum.map(values, &atom_from_dto_value(&1, allowed_atom_strings)))

      _other ->
        context
    end
  end

  defp normalize_context_settings(context, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, :settings) do
      {:ok, settings} when is_map(settings) ->
        normalized =
          Map.new(settings, fn {key, value} ->
            {atom_from_dto_value(key, allowed_atom_strings), value}
          end)

        Map.put(context, :settings, normalized)

      _other ->
        context
    end
  end

  defp normalize_context_schedule(context, allowed_atom_strings) when is_map(context) do
    case Map.fetch(context, :schedule) do
      {:ok, nil} ->
        context

      {:ok, schedule} when is_map(schedule) ->
        normalized = %Favn.Manifest.Schedule{
          module: schedule |> field(:module) |> atom_from_dto_value(allowed_atom_strings),
          name: schedule |> field(:name) |> atom_from_dto_value(allowed_atom_strings),
          ref: schedule |> field(:ref) |> ref_from_dto_value(allowed_atom_strings),
          kind: schedule |> field(:kind, "cron") |> schedule_kind_from_dto(),
          cron: field(schedule, :cron),
          timezone: field(schedule, :timezone),
          missed: schedule |> field(:missed, "skip") |> schedule_missed_from_dto(),
          overlap: schedule |> field(:overlap, "forbid") |> schedule_overlap_from_dto(),
          active: field(schedule, :active, true),
          origin: schedule |> field(:origin, "named") |> schedule_origin_from_dto()
        }

        Map.put(context, :schedule, normalized)

      _other ->
        context
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
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, item} -> {:cont, {:ok, [item | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, reason} -> {:error, reason}
    end
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
  defp replay_mode_from_dto("fresh_rerun"), do: :fresh_rerun
  defp replay_mode_from_dto(value), do: value

  defp schedule_kind_from_dto(value) when value in [:cron, "cron"], do: :cron
  defp schedule_kind_from_dto(value), do: value

  defp schedule_missed_from_dto(value) when value in [:skip, "skip"], do: :skip
  defp schedule_missed_from_dto(value) when value in [:one, "one"], do: :one
  defp schedule_missed_from_dto(value) when value in [:all, "all"], do: :all
  defp schedule_missed_from_dto(value), do: value

  defp schedule_overlap_from_dto(value) when value in [:forbid, "forbid"], do: :forbid
  defp schedule_overlap_from_dto(value) when value in [:allow, "allow"], do: :allow

  defp schedule_overlap_from_dto(value) when value in [:queue_one, "queue_one"],
    do: :queue_one

  defp schedule_overlap_from_dto(value), do: value

  defp schedule_origin_from_dto(value) when value in [:inline, "inline"], do: :inline
  defp schedule_origin_from_dto(value) when value in [:named, "named"], do: :named
  defp schedule_origin_from_dto(value), do: value

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

  defp atom_to_string(nil), do: nil
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
    with {:ok, manifest_atoms} <- ManifestAtoms.extract(manifest_record) do
      {:ok, Enum.reduce(@internal_atom_strings, manifest_atoms, &MapSet.put(&2, &1))}
    end
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
