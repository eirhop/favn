defmodule Favn.Contracts.RunnerTask.PersistenceData do
  @moduledoc false

  alias Favn.Manifest
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias Favn.Manifest.ExecutionPackage

  # Closed task contract registry. Persisted names never select arbitrary modules.
  @structs [
    Date,
    Time,
    NaiveDateTime,
    DateTime,
    Decimal,
    Favn.Contracts.RunnerWork,
    Favn.Contracts.RunnerResult,
    Favn.Contracts.RunnerAssetResult,
    Favn.Contracts.RunnerError,
    Favn.Resource.Ref,
    Favn.Contracts.ResourceOutcome,
    Favn.Contracts.TargetGenerationPin,
    Favn.Contracts.RelationInspectionRequest,
    Favn.Contracts.RelationInspectionResult,
    Favn.Contracts.GenerationCapabilitiesRequest,
    Favn.Contracts.GenerationCapabilitiesResult,
    Favn.Contracts.GenerationMarkerReadRequest,
    Favn.Contracts.GenerationMarkerReadResult,
    Favn.Contracts.GenerationMarkerInitializationRequest,
    Favn.Contracts.GenerationMarkerInitializationResult,
    Favn.Contracts.GenerationActivationRequest,
    Favn.Contracts.GenerationActivationResult,
    Favn.Contracts.GenerationReconciliationRequest,
    Favn.Contracts.GenerationReconciliationResult,
    Favn.Contracts.GenerationDiscardRequest,
    Favn.Contracts.GenerationDiscardResult,
    Favn.Contracts.GenerationMarker,
    Version,
    Favn.Manifest.ExecutionPackage,
    Favn.Manifest.SQLExecution,
    Favn.Manifest.TargetDescriptor,
    Favn.RelationRef,
    Favn.Plan.NodeIdentity,
    Favn.Run.PipelineContext,
    Favn.Window.Runtime,
    Favn.Window.Anchor,
    Favn.Window.Selection,
    Favn.Window.Spec,
    Favn.RuntimeInput.Pin,
    Favn.RuntimeInputResolver.Ref,
    Favn.Asset.RelationInput,
    Favn.SQL.Relation,
    Favn.SQL.Column,
    Favn.SQL.ContractValidation,
    Favn.SQL.GroupReplacementResult,
    Favn.SQL.Check,
    Favn.SQL.CheckResult,
    Favn.SQL.Contract,
    Favn.SQL.Definition,
    Favn.SQL.Definition.Param,
    Favn.SQL.Contract.Column,
    Favn.SQL.Contract.Param,
    Favn.SQL.Contract.Grain,
    Favn.SQL.Contract.UniqueKey,
    Favn.SQL.Contract.RowCount,
    Favn.SQL.Contract.Lineage,
    Favn.SQL.Contract.Fragment,
    Favn.SQL.Template,
    Favn.SQL.Template.Position,
    Favn.SQL.Template.Span,
    Favn.SQL.Template.Requirements,
    Favn.SQL.Template.Fragment,
    Favn.SQL.Template.Text,
    Favn.SQL.Template.Placeholder,
    Favn.SQL.Template.DefinitionRef,
    Favn.SQL.Template.Call,
    Favn.SQL.Template.AssetRef,
    Favn.SQL.Template.Relation,
    Favn.SQL.Template.RuntimeRelation
  ]
  @atoms [
    :retry_policy_source,
    :sequential,
    :physical_relation,
    :evidence_generation_id,
    :pipeline_active_stage_outcome,
    :retrying,
    :retry,
    :admission_deadline_ms,
    :pipeline_execution_policy,
    :max_concurrency,
    :invalid_asset_input,
    :not_sql_asset,
    :invalid_sql_asset_definition,
    :missing_runtime_input,
    :missing_query_param,
    :unresolved_asset_ref,
    :invalid_relation,
    :cross_connection_asset_ref,
    :defsql_expansion_failed,
    :binding_failure,
    :unresolved_runtime_relation,
    :invalid_check_result,
    :check_failed,
    :contract_violation,
    :materialization_planning_failed,
    :runtime_inputs_missing_module,
    :runtime_inputs_missing_callback,
    :runtime_inputs_raised,
    :runtime_inputs_invalid_result,
    :runtime_inputs_timeout,
    :runtime_inputs_cancelled,
    :runtime_inputs_failed,
    :runtime_inputs_param_collision,
    :runtime_inputs_payload_too_large,
    :unsupported_materialization,
    :render,
    :worker_crash,
    :native_cancel_unknown,
    :runner_task_cancellation,
    :runner_task_execution,
    :tuple,
    :list,
    :number,
    :term,
    Calendar.ISO,
    :sql_asset_succeeded,
    :sql_resource_reached,
    :other,
    :redacted,
    :invalid_config,
    :authentication_error,
    :connection_error,
    :execution_error,
    :unsupported_capability,
    :introspection_mismatch,
    :missing_relation,
    :admission_timeout,
    :pool_timeout,
    :operation_timeout,
    :backend_execution_failed,
    :classification,
    :asset_retryable?,
    :resource_failure?,
    :resource_succeeded?,
    :resource_failure_category,
    :resource_safe_to_repeat?,
    :connect,
    :bootstrap,
    :materialize,
    :preview,
    :explain,
    :unknown_commit_state,
    :unknown_outcome_timeout,
    :transaction_stage,
    :rollback,
    :unknown_outcome?,
    :sql_error_type,
    :stacktrace_depth,
    :replaced,
    :delete_only,
    :empty_scope_no_op,
    :before_check_skipped,
    :bootstrap_created,
    :bootstrap_empty,
    :unavailable,
    :check_results,
    :quality_status,
    :rows_affected,
    :rows_written,
    :rows_inserted,
    :command,
    :group_replacement,
    :contract_validation,
    :written,
    :no_op,
    :warning,
    :passed,
    :warned,
    :condition_skipped,
    :materialization_skipped,
    :not_run,
    :errored,
    :condition_false,
    :target_missing,
    :check_halted,
    :transaction_failed,
    :materialization_failed,
    :transaction_outcome,
    :committed,
    :rolled_back,
    :input_identity,
    :input_metadata,
    :execution_id,
    :retention,
    :truncated,
    :original_bytes,
    :omitted,
    :asset_meta,
    :asset_attempts,
    :runner_metadata,
    :retention_truncated,
    :native_type,
    :nullability_observed?,
    :maximum,
    :json,
    :uuid,
    :missing,
    :unexpected,
    :order,
    :nullability,
    :column_limit,
    :column,
    :expected,
    :observed,
    :backfill,
    :start_at_us,
    :end_at_us,
    :month,
    :year,
    :runtime_input_mode,
    :fresh,
    :pinned,
    :inherit,
    :refresh_policy,
    :mode,
    :refs,
    :include_upstream?,
    :auto,
    :force_assets,
    :asset_dependencies,
    :pipeline_dependencies,
    :dependencies,
    :upstream,
    :downstream,
    :rebuild,
    :rebuild_operation_id,
    :rebuild_action_id,
    :rebuild_item_id,
    :rebuild_evaluated_at,
    :runtime_input_lineage,
    :input_versions,
    :input_generations,
    :workspace_id,
    :deployment_id,
    :manual,
    :operator,
    :schedule,
    :rerun_of_run_id,
    :source_run_id,
    :parent_run_id,
    :root_run_id,
    :replay_mode,
    :exact_replay,
    :resume_from_failure,
    :fresh_rerun,
    :replay_submit_kind,
    :refresh,
    :force,
    :config,
    :kind,
    :backfill_range,
    :combine_windows,
    :anchor_ranges,
    :anchor_window,
    :window_selection,
    :pipeline_identity_ref,
    :pipeline_module,
    :pipeline_submit_ref,
    :pipeline_target_refs,
    :retry_state,
    :next_retry_at,
    :next_attempt,
    :retry_backoff_ms,
    :sequential_index,
    :stage_index,
    :active_runner_task_ids,
    :transactional_ddl,
    :isolated_candidates,
    :physical_inspection,
    :atomic_swap,
    :marker_reconciliation,
    :idempotent_discard,
    :snapshots,
    :max_identifier_bytes,
    :runner_task_mode,
    :runtime_input_resolution,
    :runtime,
    :window_start,
    :window_end,
    :map,
    :table,
    :view,
    :materialized_view,
    :temporary,
    :contract_nullability,
    :reliable,
    :unreliable,
    :relation_failed,
    :columns_failed,
    :row_count_failed,
    :sample_failed,
    :table_metadata_failed,
    :limit,
    :sample,
    :table_metadata,
    :supported,
    :already_absent,
    :candidate_active,
    :previous_active,
    :candidate,
    :retired,
    :success,
    :failure,
    :connection,
    :storage,
    :source,
    :runner_error,
    :exit,
    :throw,
    :preflight,
    :boundary,
    :ok,
    :error,
    :cancelled,
    :timed_out,
    :unknown,
    :safe_failure,
    :succeeded,
    :failed,
    :terminal,
    :safe_to_retry,
    :reconcile_before_retry,
    :unknown_do_not_retry,
    :default,
    :normal_materialization,
    :rebuild_candidate,
    :append,
    :replace,
    :delete_insert,
    :merge,
    :replace_groups,
    :query,
    :expression,
    :relation,
    :target,
    :replacement_scope,
    :runtime_inputs,
    :query_params,
    :resolved,
    :deferred,
    :plain_relation,
    :direct_asset_ref,
    :before_materialize,
    :after_materialize,
    :target_exists,
    :fail,
    :warn,
    :skip_materialization,
    :authored,
    :contract,
    :asset,
    :external,
    :identity,
    :transformation,
    :aggregation,
    :string,
    :integer,
    :float,
    :boolean,
    :date,
    :datetime,
    :decimal,
    :binary,
    :daily,
    :day,
    :hour,
    :minute,
    :second,
    :always,
    :all,
    :none,
    :combined,
    :materialized,
    :not_materialized,
    :outcome_unknown,
    :activated,
    :discarded,
    :initialized,
    :already_initialized,
    :unchanged,
    :not_found,
    :unsupported,
    :rows,
    :columns,
    :data,
    :count,
    :message,
    :code,
    :reason,
    :retryable?,
    :runner_task_id,
    :node_key,
    :window,
    :execution_pool,
    :run_attempt,
    :duration_ms,
    :row_count,
    :adapter,
    :name,
    :type,
    :nullable,
    :precision,
    :scale
  ]
  @max_depth 64
  @max_nodes 100_000

  @spec encode(term(), pos_integer()) :: {:ok, map()} | {:error, atom()}
  def encode(value, limit) do
    with true <- byte_size(:erlang.term_to_binary(value, [:deterministic])) <= limit,
         {data, _remaining} <- pack(value, @max_depth, @max_nodes),
         true <- byte_size(Serializer.encode_canonical!(data)) <= 4 * limit do
      {:ok, %{"format" => "task-data-v1", "data" => data}}
    else
      _invalid -> {:error, :invalid_runner_task_data}
    end
  rescue
    _error -> {:error, :invalid_runner_task_data}
  catch
    :invalid_task_data -> {:error, :invalid_runner_task_data}
  end

  @spec decode(map(), pos_integer(), Version.t() | nil, [atom()], [ExecutionPackage.t()]) ::
          {:ok, term()} | {:error, atom()}
  def decode(envelope, limit, version \\ nil, owner_atoms \\ [], packages \\ []) do
    with %{"format" => "task-data-v1", "data" => data} when map_size(envelope) == 2 <- envelope,
         {_nodes, _bytes} <- check_encoded(data, 2 * @max_depth + 2, 4 * @max_nodes, 4 * limit),
         true <- byte_size(Serializer.encode_canonical!(data)) <= 4 * limit,
         atoms <- atom_dictionary(version, owner_atoms, packages),
         {value, _remaining} <- unpack(data, @max_depth, @max_nodes, atoms, version),
         true <- byte_size(:erlang.term_to_binary(value, [:deterministic])) <= limit do
      {:ok, value}
    else
      _invalid -> {:error, :invalid_runner_task_data}
    end
  rescue
    _error -> {:error, :invalid_runner_task_data}
  catch
    :invalid_task_data -> {:error, :invalid_runner_task_data}
  end

  defp check_encoded(_value, depth, nodes, bytes) when depth < 0 or nodes <= 0 or bytes < 0,
    do: throw(:invalid_task_data)

  defp check_encoded(value, _depth, nodes, bytes) when is_binary(value) do
    if byte_size(value) > bytes, do: throw(:invalid_task_data)
    {nodes - 1, bytes - byte_size(value)}
  end

  defp check_encoded(value, depth, nodes, bytes) when is_list(value) do
    Enum.reduce(value, {nodes - 1, bytes}, fn item, {left, available} ->
      check_encoded(item, depth - 1, left, available)
    end)
  end

  defp check_encoded(value, _depth, nodes, bytes)
       when is_number(value) or is_boolean(value) or is_nil(value),
       do: {nodes - 1, bytes - 1}

  defp check_encoded(_value, _depth, _nodes, _bytes), do: throw(:invalid_task_data)

  defp pack(_value, depth, remaining) when depth < 0 or remaining <= 0,
    do: throw(:invalid_task_data)

  defp pack(value, _depth, remaining) when is_number(value) or is_boolean(value) or is_nil(value),
    do: {value, remaining - 1}

  defp pack(value, _depth, remaining) when is_binary(value),
    do: {["binary", Base.encode64(value)], remaining - 1}

  defp pack(value, _depth, remaining) when is_atom(value),
    do: {["atom", Atom.to_string(value)], remaining - 1}

  defp pack(%Manifest{} = value, _depth, remaining),
    do: {["manifest", fingerprint(value)], remaining - 1}

  defp pack(%MapSet{} = value, depth, remaining),
    do: pack_sequence("set", Enum.sort(MapSet.to_list(value)), depth, remaining)

  defp pack(%Favn.Contracts.RunnerError{} = value, depth, remaining) do
    fields =
      value
      |> Map.from_struct()
      |> Map.update!(:type, &error_label/1)
      |> Map.update!(:phase, &error_label/1)

    {fields, left} = pack(fields, depth - 1, remaining - 1)
    {["struct", Atom.to_string(Favn.Contracts.RunnerError), fields], left}
  end

  defp pack(%module{} = value, depth, remaining) when module in @structs do
    {fields, left} = pack(Map.from_struct(value), depth - 1, remaining - 1)
    {["struct", Atom.to_string(module), fields], left}
  end

  defp pack(value, depth, remaining) when is_tuple(value),
    do: pack_sequence("tuple", Tuple.to_list(value), depth, remaining)

  defp pack(value, depth, remaining) when is_list(value),
    do: pack_sequence("list", value, depth, remaining)

  defp pack(value, depth, remaining) when is_map(value) and not is_struct(value) do
    {pairs, left} =
      Enum.map_reduce(value, remaining - 1, fn {key, item}, budget ->
        {key, budget} = pack(key, depth - 1, budget)
        {item, budget} = pack(item, depth - 1, budget)
        {[key, item], budget}
      end)

    {["map", Enum.sort(pairs)], left}
  end

  defp pack(_value, _depth, _remaining), do: throw(:invalid_task_data)

  defp error_label(value) when value in @atoms or is_nil(value), do: value
  defp error_label(value) when is_atom(value), do: value |> Atom.to_string() |> error_label()
  defp error_label(value) when is_binary(value) and byte_size(value) in 1..255, do: value
  defp error_label(_value), do: throw(:invalid_task_data)

  defp pack_sequence(tag, items, depth, remaining) do
    {items, left} = Enum.map_reduce(items, remaining - 1, &pack(&1, depth - 1, &2))
    {[tag, items], left}
  end

  defp unpack(_value, depth, remaining, _atoms, _version) when depth < 0 or remaining <= 0,
    do: throw(:invalid_task_data)

  defp unpack(value, _depth, remaining, _atoms, _version)
       when is_number(value) or is_boolean(value) or is_nil(value), do: {value, remaining - 1}

  defp unpack(["binary", value], _depth, remaining, _atoms, _version) when is_binary(value),
    do: {Base.decode64!(value), remaining - 1}

  defp unpack(["atom", value], _depth, remaining, atoms, _version) when is_binary(value),
    do: {Map.fetch!(atoms, value), remaining - 1}

  defp unpack(["manifest", hash], _depth, remaining, _atoms, %Version{
         manifest: %Manifest{} = manifest
       }) do
    if fingerprint(manifest) != hash, do: throw(:invalid_task_data)
    {manifest, remaining - 1}
  end

  defp unpack(["struct", name, data], depth, remaining, atoms, version) when is_binary(name) do
    module = Enum.find(@structs, &(Atom.to_string(&1) == name)) || throw(:invalid_task_data)
    {fields, left} = unpack(data, depth - 1, remaining - 1, atoms, version)
    expected = module |> struct() |> Map.from_struct() |> Map.keys() |> Enum.sort()

    if not is_map(fields) or Enum.sort(Map.keys(fields)) != expected,
      do: throw(:invalid_task_data)

    value = struct!(module, fields)
    validate_datetime!(value)
    {value, left}
  end

  defp unpack(["map", pairs], depth, remaining, atoms, version) when is_list(pairs) do
    Enum.reduce(pairs, {%{}, remaining - 1}, fn [key, item], {map, budget} ->
      {key, budget} = unpack(key, depth - 1, budget, atoms, version)
      {item, budget} = unpack(item, depth - 1, budget, atoms, version)
      if Map.has_key?(map, key), do: throw(:invalid_task_data)
      {Map.put(map, key, item), budget}
    end)
  end

  defp unpack([tag, items], depth, remaining, atoms, version)
       when tag in ["list", "tuple", "set"] and is_list(items) do
    {items, left} =
      Enum.map_reduce(items, remaining - 1, &unpack(&1, depth - 1, &2, atoms, version))

    value =
      case tag do
        "list" -> items
        "tuple" -> List.to_tuple(items)
        "set" -> MapSet.new(items)
      end

    if tag == "set" and MapSet.size(value) != length(items), do: throw(:invalid_task_data)
    {value, left}
  end

  defp unpack(_value, _depth, _remaining, _atoms, _version), do: throw(:invalid_task_data)

  defp atom_dictionary(version, owner_atoms, packages) do
    fields = Enum.flat_map(@structs, &Map.keys(struct(&1)))
    fixed = Map.new(@atoms ++ fields ++ owner_atoms, &{Atom.to_string(&1), &1})
    fixed = manifest_atoms(version, fixed)

    case packages do
      [] ->
        fixed

      [%ExecutionPackage{} = package] ->
        %Version{} = version
        asset = Enum.find(version.manifest.assets, &(&1.ref == package.asset_ref))
        {:ok, ^package} = ExecutionPackage.verify_for_asset(package, asset)
        manifest_atoms(package, fixed)

      _invalid ->
        throw(:invalid_task_data)
    end
  end

  # Only separately validated retained artifacts supply open-world atoms.
  defp manifest_atoms(value, atoms) when is_atom(value),
    do: Map.put(atoms, Atom.to_string(value), value)

  defp manifest_atoms(value, atoms) when is_map(value),
    do: Enum.reduce(Map.to_list(value), atoms, &manifest_atoms/2)

  defp manifest_atoms(value, atoms) when is_tuple(value),
    do: manifest_atoms(Tuple.to_list(value), atoms)

  defp manifest_atoms(value, atoms) when is_list(value),
    do: Enum.reduce(value, atoms, &manifest_atoms/2)

  defp manifest_atoms(_value, atoms), do: atoms

  defp fingerprint(manifest),
    do:
      :crypto.hash(:sha256, :erlang.term_to_binary(manifest, [:deterministic]))
      |> Base.encode16(case: :lower)

  defp validate_datetime!(%Date{calendar: Calendar.ISO} = value) do
    {:ok, ^value} = Date.new(value.year, value.month, value.day)
    :ok
  end

  defp validate_datetime!(%Time{calendar: Calendar.ISO} = value) do
    {:ok, ^value} = Time.new(value.hour, value.minute, value.second, value.microsecond)
    :ok
  end

  defp validate_datetime!(%NaiveDateTime{calendar: Calendar.ISO} = value) do
    {:ok, ^value} =
      NaiveDateTime.new(
        value.year,
        value.month,
        value.day,
        value.hour,
        value.minute,
        value.second,
        value.microsecond
      )

    :ok
  end

  defp validate_datetime!(%DateTime{calendar: Calendar.ISO} = value) do
    validate_datetime!(DateTime.to_naive(value))

    unless is_binary(value.time_zone) and is_binary(value.zone_abbr) and
             is_integer(value.utc_offset) and abs(value.utc_offset) < 86_400 and
             is_integer(value.std_offset) and abs(value.std_offset) < 86_400,
           do: throw(:invalid_task_data)

    :ok
  end

  defp validate_datetime!(%module{}) when module in [Date, Time, NaiveDateTime, DateTime],
    do: throw(:invalid_task_data)

  defp validate_datetime!(%Decimal{sign: sign, coef: coef, exp: exp})
       when sign in [-1, 1] and is_integer(coef) and coef >= 0 and is_integer(exp), do: :ok

  defp validate_datetime!(%Decimal{}), do: throw(:invalid_task_data)
  defp validate_datetime!(_value), do: :ok
end
