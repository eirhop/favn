defmodule Favn.SQLAsset.Runtime do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.RuntimeInput.Pin
  alias Favn.Run.Context
  alias Favn.SQL.Client, as: SQLClient
  alias Favn.SQL.CancelToken
  alias Favn.SQL.Error, as: SQLError
  alias Favn.SQL.{Check, CheckResult, Contract, ContractValidation, Result, Session}

  alias Favn.SQL.{Explain, IncrementalWindow, MaterializationResult, Params, Preview, Render}

  alias Favn.SQLAsset.{
    CheckResultNormalizer,
    CheckedMaterialization,
    Compiler,
    Definition,
    Error,
    Renderer
  }

  alias Favn.Window.Runtime
  alias FavnRunner.RuntimeInputResolver
  alias FavnRunner.RuntimeInputResolver.Resolution, as: RuntimeInputResolution
  alias FavnRunner.SQL.MaterializationPlanner

  @runner_registry FavnRunner.ConnectionRegistry

  @type opts :: [
          params: map(),
          runtime: map(),
          context: Context.t(),
          timeout_ms: pos_integer()
        ]

  @spec run_manifest(Asset.t(), Version.t(), RunnerWork.t(), Context.t()) ::
          {:ok, map()} | {:error, Error.t()} | {:error, Error.t(), map()}
  def run_manifest(
        %Asset{} = asset,
        %Version{} = version,
        %RunnerWork{} = work,
        %Context{} = context
      ) do
    with {:ok, %Definition{} = definition, %Context{} = final_context, final_opts} <-
           prepare_manifest_execution(asset, version, work, context),
         {:ok, %Render{} = rendered, %CheckedMaterialization{} = materialization, resolution} <-
           execute_finalized_definition(definition, final_context, final_opts) do
      output =
        definition
        |> runtime_output(rendered, materialization, resolution)
        |> Map.put(:manifest_version_id, version.manifest_version_id)
        |> Map.put(:manifest_content_hash, version.content_hash)

      {:ok, output}
    else
      {:error, %Error{} = error, meta} -> {:error, error, meta}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  @doc false
  @spec prepare_manifest_execution(Asset.t(), Version.t(), RunnerWork.t(), Context.t()) ::
          {:ok, Definition.t(), Context.t(), keyword()} | {:error, Error.t()}
  def prepare_manifest_execution(
        %Asset{} = asset,
        %Version{} = version,
        %RunnerWork{} = work,
        %Context{} = context
      ) do
    opts = context |> run_opts() |> Keyword.merge(runner_runtime_opts(work))

    with {:ok, %Definition{} = definition} <- manifest_definition(asset, version),
         {:ok, final_context, final_opts} <-
           finalize_execution_window(definition, context, opts) do
      {:ok, definition, final_context, final_opts}
    end
  end

  @spec render(map(), opts()) :: {:ok, Render.t()} | {:error, Error.t()}
  def render(asset, opts \\ [])

  def render(%{type: :sql, module: module}, opts) when is_atom(module) and is_list(opts) do
    with {:ok, _definition, %Render{} = rendered} <- render_sql_asset(module, opts) do
      {:ok, rendered}
    end
  end

  def render(%{} = asset, _opts) do
    asset_ref = Map.get(asset, :ref)

    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :render,
       asset_ref: asset_ref,
       message: "asset #{inspect(asset_ref)} is not a SQL asset"
     }}
  end

  @spec preview(map(), opts()) :: {:ok, Preview.t()} | {:error, Error.t()}
  def preview(%{} = asset, opts \\ []) when is_list(opts) do
    limit = Keyword.get(opts, :limit, 100)

    with :ok <- validate_limit(limit),
         {:ok, %Definition{} = definition, %Render{} = rendered} <- render_sql_asset(asset, opts),
         statement <- preview_statement(rendered.sql, limit),
         {:ok, result} <- query_render(definition, rendered, statement, :preview, opts) do
      {:ok, %Preview{render: rendered, limit: limit, statement: statement, result: result}}
    end
  end

  @spec explain(map(), opts()) :: {:ok, Explain.t()} | {:error, Error.t()}
  def explain(%{} = asset, opts \\ []) when is_list(opts) do
    analyze? = Keyword.get(opts, :analyze?, false)

    with {:ok, %Definition{} = definition, %Render{} = rendered} <- render_sql_asset(asset, opts),
         statement <- explain_statement(rendered.sql, analyze?),
         {:ok, result} <- query_render(definition, rendered, statement, :explain, opts) do
      {:ok, %Explain{render: rendered, statement: statement, analyze?: analyze?, result: result}}
    end
  end

  @spec materialize(map(), opts()) :: {:ok, MaterializationResult.t()} | {:error, Error.t()}
  def materialize(%{type: :sql, module: module}, opts) when is_atom(module) and is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module) do
      materialize_definition(definition, opts)
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  def materialize(%{} = asset, _opts) do
    asset_ref = Map.get(asset, :ref)

    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :materialize,
       asset_ref: asset_ref,
       message: "asset #{inspect(asset_ref)} is not a SQL asset"
     }}
  end

  @spec run(module(), Context.t()) :: {:ok, map()} | {:error, Error.t()}
  def run(module, %Context{} = ctx) when is_atom(module) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         opts <- run_opts(ctx),
         {:ok, %Render{} = rendered, %CheckedMaterialization{} = output, resolution} <-
           execute_definition(definition, ctx, opts) do
      {:ok, runtime_output(definition, rendered, output, resolution)}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, %Error{} = error, _meta} -> {:error, error}
    end
  end

  defp run_opts(%Context{} = ctx) do
    runtime =
      case ctx.window do
        nil -> %{}
        window -> %{window: window}
      end

    [params: ctx.params || %{}, runtime: runtime, context: ctx]
  end

  defp materialize_definition(%Definition{runtime_inputs: nil} = definition, opts) do
    with {:ok, %Render{} = rendered} <- render_for_materialize(definition, opts),
         {:ok, %CheckedMaterialization{} = output} <-
           materialize_render(definition, rendered, opts) do
      {:ok, materialization_result(rendered, output)}
    else
      {:error, %Error{} = error, _meta} -> {:error, error}
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp materialize_definition(%Definition{} = definition, opts) do
    case Keyword.get(opts, :context) do
      %Context{} = context ->
        context_opts =
          context
          |> run_opts()
          |> Keyword.merge(Keyword.drop(opts, [:params, :runtime, :context]))

        with {:ok, %Render{} = rendered, %CheckedMaterialization{} = output, _resolution} <-
               execute_definition(definition, context, context_opts) do
          {:ok, materialization_result(rendered, output)}
        else
          {:error, %Error{} = error, _meta} -> {:error, error}
          {:error, %Error{} = error} -> {:error, error}
        end

      _other ->
        {:error,
         %Error{
           type: :runtime_inputs_invalid_result,
           phase: :runtime_inputs,
           asset_ref: definition.asset.ref,
           message:
             "materializing an asset with @runtime_inputs requires context: %Favn.Run.Context{}"
         }}
    end
  end

  defp materialization_result(rendered, output) do
    %MaterializationResult{
      render: rendered,
      write_plan: output.write_plan,
      result: output.result,
      check_results: output.check_results,
      write_outcome: output.write_outcome,
      reason: output.reason
    }
  end

  defp execute_definition(%Definition{} = definition, %Context{} = context, opts) do
    with {:ok, final_context, final_opts} <- finalize_execution_window(definition, context, opts) do
      execute_finalized_definition(definition, final_context, final_opts)
    end
  end

  defp execute_finalized_definition(
         %Definition{} = definition,
         %Context{} = final_context,
         final_opts
       ) do
    case resolve_runtime_inputs(definition, final_context, final_opts) do
      {:ok, resolution, resolved_opts} ->
        result =
          with {:ok, %Render{} = rendered} <- Renderer.render(definition, resolved_opts),
               {:ok, %CheckedMaterialization{} = output} <-
                 materialize_render(definition, rendered, resolved_opts) do
            {:ok, rendered, output, resolution}
          end

        redact_resolution_result(result, resolution)

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp finalize_execution_window(
         %Definition{materialization: {:incremental, _opts}} = definition,
         %Context{} = context,
         opts
       ) do
    case context.window do
      %Runtime{} = runtime_window ->
        with {:ok, %IncrementalWindow{} = effective_window} <-
               IncrementalWindow.resolve(runtime_window, definition.asset.window_spec),
             {:ok, %Runtime{} = effective_runtime} <-
               IncrementalWindow.to_runtime(effective_window) do
          final_context = %Context{context | window: effective_runtime}

          {:ok, final_context,
           opts
           |> Keyword.put(:context, final_context)
           |> put_runtime_window(effective_runtime)}
        else
          {:error, reason} ->
            {:error,
             %Error{
               type: :materialization_planning_failed,
               phase: :materialize,
               asset_ref: definition.asset.ref,
               message: "failed to resolve the effective incremental window",
               details: %{materialization: definition.materialization},
               cause: reason
             }}
        end

      _other ->
        {:error,
         %Error{
           type: :materialization_planning_failed,
           phase: :materialize,
           asset_ref: definition.asset.ref,
           message: "incremental materialization requires runtime.window",
           details: %{materialization: definition.materialization}
         }}
    end
  end

  defp finalize_execution_window(%Definition{}, %Context{} = context, opts),
    do: {:ok, context, Keyword.put(opts, :context, context)}

  defp resolve_runtime_inputs(%Definition{runtime_inputs: nil}, _context, opts),
    do: {:ok, nil, opts}

  defp resolve_runtime_inputs(%Definition{runtime_inputs: resolver}, context, opts) do
    case pinned_or_resolved_runtime_inputs(resolver, context, opts) do
      {:ok, %RuntimeInputResolution{} = resolution} ->
        {:ok, resolution, Keyword.put(opts, :params, resolution.params)}

      {:error, %Error{} = error} ->
        {:error, %Error{error | asset_ref: context.current_ref}}
    end
  end

  defp pinned_or_resolved_runtime_inputs(resolver, _context, opts) do
    case Keyword.get(opts, :runtime_input_pin) do
      %Pin{resolver: resolver_module} = pin when resolver_module == resolver.module ->
        {:ok,
         %RuntimeInputResolution{
           resolver: pin.resolver,
           params: pin.params,
           identity: pin.input_identity,
           metadata: pin.metadata,
           sensitive_params: pin.sensitive_params,
           sensitive_values: sensitive_values(pin.params, pin.sensitive_params),
           duration_ms: 0
         }}

      %Pin{} ->
        {:error,
         %Error{
           type: :runtime_inputs_invalid_result,
           phase: :runtime_inputs,
           message: "runtime-input pin resolver does not match the manifest resolver"
         }}

      nil ->
        resolve_unpinned_runtime_inputs(resolver, opts)
    end
  end

  defp resolve_unpinned_runtime_inputs(resolver, opts) do
    context = Keyword.fetch!(opts, :context)

    if Keyword.get(opts, :require_runtime_input_pin, false) do
      {:error,
       %Error{
         type: :runtime_inputs_invalid_result,
         phase: :runtime_inputs,
         asset_ref: context.current_ref,
         message: "manifest work with @runtime_inputs requires a persisted runtime-input pin"
       }}
    else
      RuntimeInputResolver.resolve(resolver, context, context.params || %{}, opts)
    end
  end

  defp sensitive_values(params, names) do
    Enum.flat_map(names, fn name ->
      string_name = to_string(name)

      [Map.get(params, name), Map.get(params, string_name)]
      |> Enum.reject(&is_nil/1)
    end)
  end

  defp redact_resolution_result(result, nil), do: result

  defp redact_resolution_result(
         {:ok, %Render{} = rendered, %CheckedMaterialization{} = output, resolution},
         %RuntimeInputResolution{} = runtime_input_resolution
       ) do
    safe_output = %CheckedMaterialization{
      output
      | write_plan:
          RuntimeInputResolver.redact_write_plan(
            output.write_plan,
            runtime_input_resolution
          ),
        check_results: RuntimeInputResolver.redact(output.check_results, runtime_input_resolution)
    }

    {:ok, RuntimeInputResolver.redact_render(rendered, runtime_input_resolution), safe_output,
     resolution}
  end

  defp redact_resolution_result(result, %RuntimeInputResolution{} = resolution),
    do: RuntimeInputResolver.redact(result, resolution)

  defp render_sql_asset(%{type: :sql, module: module}, opts)
       when is_atom(module) and is_list(opts),
       do: render_sql_asset(module, opts)

  defp render_sql_asset(%{} = asset, _opts) do
    asset_ref = Map.get(asset, :ref)

    {:error,
     %Error{
       type: :not_sql_asset,
       phase: :render,
       asset_ref: asset_ref,
       message: "asset #{inspect(asset_ref)} is not a SQL asset"
     }}
  end

  defp render_sql_asset(module, opts) when is_atom(module) and is_list(opts) do
    with {:ok, %Definition{} = definition} <- fetch_definition(module),
         {:ok, %Render{} = rendered} <- Renderer.render(definition, opts) do
      {:ok, definition, rendered}
    end
  end

  defp fetch_definition(module) do
    case Compiler.fetch_definition(module) do
      {:ok, %Definition{} = definition} ->
        {:ok, definition}

      {:error, reason} ->
        {:error,
         %Error{
           type: :invalid_sql_asset_definition,
           phase: :render,
           message: "failed to load SQL asset definition for #{inspect(module)}",
           details: %{module: module, reason: reason}
         }}
    end
  end

  defp validate_limit(limit) when is_integer(limit) and limit > 0, do: :ok

  defp validate_limit(limit) do
    {:error,
     %Error{
       type: :binding_failure,
       phase: :preview,
       message: "preview limit must be a positive integer",
       details: %{limit: limit}
     }}
  end

  defp preview_statement(sql, limit) do
    "SELECT * FROM (#{trim_sql(sql)}) AS favn_preview LIMIT #{limit}"
  end

  defp explain_statement(sql, true), do: "EXPLAIN ANALYZE #{trim_sql(sql)}"
  defp explain_statement(sql, false), do: "EXPLAIN #{trim_sql(sql)}"

  defp query_render(%Definition{} = definition, %Render{} = rendered, statement, phase, opts) do
    with_session(
      rendered.connection,
      opts,
      session_required_catalogs(definition, rendered),
      session_required_resources(definition),
      fn session ->
        SQLClient.query(
          session,
          statement,
          Keyword.merge(sql_operation_opts(opts),
            params: adapter_params(rendered.params),
            read_only?: true
          )
        )
      end
    )
    |> map_sql_result_error(rendered.asset_ref, phase)
  end

  defp materialize_render(
         %Definition{checks: [_check | _rest]} = definition,
         rendered,
         opts
       ) do
    checked_materialize(definition, rendered, opts)
  end

  defp materialize_render(%Definition{contract: %Contract{}} = definition, rendered, opts),
    do: checked_materialize(definition, rendered, opts)

  defp materialize_render(%Definition{} = definition, %Render{} = rendered, opts) do
    with_session(
      rendered.connection,
      opts,
      session_required_catalogs(definition, rendered),
      session_required_resources(definition),
      fn session ->
        with {:ok, write_plan} <- MaterializationPlanner.build(session, definition, rendered),
             {:ok, result} <-
               SQLClient.materialize(
                 session,
                 write_plan,
                 Keyword.merge(sql_operation_opts(opts), params: adapter_params(rendered.params))
               ) do
          {:ok,
           %CheckedMaterialization{
             write_plan: write_plan,
             result: result,
             check_results: [],
             write_outcome: :written
           }}
        end
      end
    )
    |> map_sql_result_error(rendered.asset_ref, :materialize)
  end

  defp checked_materialize(%Definition{} = definition, %Render{} = rendered, opts) do
    with_session(
      rendered.connection,
      opts,
      session_required_catalogs(definition, rendered),
      session_required_resources(definition),
      fn session ->
        with :ok <- ensure_checked_materialization_supported(session, definition, rendered) do
          SQLClient.transaction(
            session,
            fn tx_session -> checked_transaction(tx_session, definition, rendered, opts) end,
            Keyword.put(sql_operation_opts(opts), :preserve_body_result_on_commit_error?, true)
          )
        end
      end
    )
    |> map_checked_materialization_result(definition, rendered)
  end

  defp ensure_checked_materialization_supported(
         %Session{capabilities: %{transactions: :supported}} = session,
         %Definition{materialization: materialization},
         %Render{} = rendered
       )
       when materialization != :view do
    if function_exported?(session.adapter, :materialize_in_transaction, 3) do
      :ok
    else
      checked_capability_error(rendered, session, :transactional_materialization)
    end
  end

  defp ensure_checked_materialization_supported(%Session{} = session, _definition, rendered) do
    capability =
      if rendered.materialization == :view, do: :table_materialization, else: :transactions

    checked_capability_error(rendered, session, capability)
  end

  defp checked_capability_error(%Render{} = rendered, %Session{} = session, capability) do
    {:error,
     %Error{
       type: :unsupported_materialization,
       phase: :materialize,
       asset_ref: rendered.asset_ref,
       message: "checked SQL assets require transactional table materialization",
       details: %{connection: session.resolved.name, missing_capability: capability}
     }}
  end

  defp checked_transaction(
         %Session{} = session,
         %Definition{} = definition,
         %Render{} = rendered,
         opts
       ) do
    stage = candidate_stage(definition)

    with {:ok, target_exists?} <- checked_target(session, rendered),
         :ok <- create_candidate_stage(session, stage, rendered, opts),
         {:ok, contract_validation} <-
           validate_candidate_contract(session, definition, stage, rendered) do
      runtime_relations = runtime_relations(rendered, stage)

      result =
        run_checked_body(
          session,
          definition,
          rendered,
          opts,
          runtime_relations,
          target_exists?
        )
        |> put_contract_validation(contract_validation)

      finalize_candidate_stage(session, stage, result, opts, definition, rendered)
    else
      {:error, reason} ->
        {:error, checked_transaction_error(reason, definition, rendered, [])}
    end
  end

  defp put_contract_validation({:ok, %CheckedMaterialization{} = output}, validation),
    do: {:ok, %CheckedMaterialization{output | contract_validation: validation}}

  defp put_contract_validation(result, _validation), do: result

  defp run_checked_body(
         session,
         definition,
         rendered,
         opts,
         runtime_relations,
         target_exists?
       ) do
    case run_check_phase(
           session,
           definition,
           :before_materialize,
           opts,
           runtime_relations,
           target_exists?,
           []
         ) do
      {:ok, before_results} ->
        run_checked_write(
          session,
          definition,
          rendered,
          opts,
          runtime_relations,
          before_results
        )

      {:skip, before_results, check_name} ->
        results = complete_check_results(definition, before_results, :materialization_skipped)

        {:ok,
         %CheckedMaterialization{
           result: no_op_result(),
           check_results: results,
           write_outcome: :no_op,
           reason: check_name
         }}

      {:error, reason, before_results} ->
        results = complete_check_results(definition, before_results, :check_halted)
        {:error, checked_transaction_error(reason, definition, rendered, results)}
    end
  end

  defp run_checked_write(
         session,
         definition,
         rendered,
         opts,
         runtime_relations,
         before_results
       ) do
    staged_render = staged_materialization_render(rendered, runtime_relations)

    with {:ok, write_plan} <- MaterializationPlanner.build(session, definition, staged_render),
         {:ok, result} <-
           SQLClient.materialize_in_transaction(
             session,
             write_plan,
             Keyword.merge(sql_operation_opts(opts),
               params: adapter_params(staged_render.params)
             )
           ) do
      case run_check_phase(
             session,
             definition,
             :after_materialize,
             opts,
             runtime_relations,
             true,
             before_results
           ) do
        {:ok, check_results} ->
          {:ok,
           %CheckedMaterialization{
             write_plan: write_plan,
             result: result,
             check_results: order_check_results(definition, check_results),
             write_outcome: :written
           }}

        {:skip, check_results, _check_name} ->
          results = complete_check_results(definition, check_results, :check_halted)

          {:error,
           checked_transaction_error(
             invalid_after_skip_error(rendered),
             definition,
             rendered,
             results
           )}

        {:error, reason, check_results} ->
          results = complete_check_results(definition, check_results, :check_halted)
          {:error, checked_transaction_error(reason, definition, rendered, results)}
      end
    else
      {:error, reason} ->
        results = complete_check_results(definition, before_results, :materialization_failed)
        {:error, checked_transaction_error(reason, definition, rendered, results)}
    end
  end

  defp run_check_phase(
         session,
         %Definition{} = definition,
         phase,
         opts,
         runtime_relations,
         target_exists?,
         initial_results
       ) do
    definition.checks
    |> Enum.filter(&(&1.at == phase))
    |> Enum.reduce_while({:ok, initial_results}, fn check, {:ok, results} ->
      case run_check(session, definition, check, opts, runtime_relations, target_exists?) do
        {:continue, check_result} ->
          {:cont, {:ok, results ++ [check_result]}}

        {:skip, check_result} ->
          {:halt, {:skip, results ++ [check_result], check.name}}

        {:error, reason, check_result} ->
          {:halt, {:error, reason, results ++ [check_result]}}
      end
    end)
  end

  defp run_check(
         _session,
         _definition,
         %Check{when: :target_exists} = check,
         _opts,
         _relations,
         false
       ) do
    result =
      check_result(check, :condition_skipped,
        duration_ms: 0,
        reason: :target_missing
      )

    {:continue, result}
  end

  defp run_check(session, definition, %Check{} = check, opts, runtime_relations, _target_exists?) do
    started_at = System.monotonic_time(:millisecond)

    result =
      with {:ok, %Render{} = check_render} <-
             Renderer.render_check(
               definition,
               check,
               Keyword.put(opts, :runtime_relations, runtime_relations)
             ),
           {:ok, %Result{} = query_result} <-
             SQLClient.query(
               session,
               check_statement(check_render.sql),
               Keyword.merge(sql_operation_opts(opts),
                 params: adapter_params(check_render.params),
                 read_only?: true
               )
             ),
           {:ok, passed?, metrics} <-
             CheckResultNormalizer.normalize(query_result, check, definition.asset.ref) do
        duration_ms = max(System.monotonic_time(:millisecond) - started_at, 0)
        check_outcome(check, passed?, metrics, duration_ms, definition.asset.ref)
      else
        {:error, reason} ->
          duration_ms = max(System.monotonic_time(:millisecond) - started_at, 0)
          reason = normalize_check_execution_error(reason, check, definition.asset.ref)

          error_result =
            check_result(check, :errored,
              duration_ms: duration_ms,
              reason: check_error_reason(reason)
            )

          {:error, reason, error_result}
      end

    result
  end

  defp check_outcome(check, true, metrics, duration_ms, _asset_ref) do
    {:continue, check_result(check, :passed, metrics: metrics, duration_ms: duration_ms)}
  end

  defp check_outcome(%Check{on_violation: :warn} = check, false, metrics, duration_ms, _asset_ref) do
    {:continue,
     check_result(check, :warned,
       metrics: metrics,
       duration_ms: duration_ms,
       reason: :condition_false
     )}
  end

  defp check_outcome(
         %Check{on_violation: :skip_materialization} = check,
         false,
         metrics,
         duration_ms,
         _asset_ref
       ) do
    {:skip,
     check_result(check, :materialization_skipped,
       metrics: metrics,
       duration_ms: duration_ms,
       reason: :condition_false
     )}
  end

  defp check_outcome(%Check{on_violation: :fail} = check, false, metrics, duration_ms, asset_ref) do
    result =
      check_result(check, :failed,
        metrics: metrics,
        duration_ms: duration_ms,
        reason: :condition_false
      )

    error = %Error{
      type: :check_failed,
      phase: check.at,
      asset_ref: asset_ref,
      message: check.message || "SQL check #{inspect(check.name)} failed",
      details: %{check: check.name, metrics: metrics}
    }

    {:error, error, result}
  end

  defp check_result(%Check{} = check, outcome, opts) do
    CheckResult.new(%{
      name: check.name,
      phase: check.at,
      outcome: outcome,
      origin: check.origin,
      claim_id: check.claim_id,
      message: check.message,
      metrics: Keyword.get(opts, :metrics, %{}),
      duration_ms: Keyword.get(opts, :duration_ms),
      reason: Keyword.get(opts, :reason)
    })
  end

  defp complete_check_results(%Definition{} = definition, results, reason) do
    by_name = Map.new(results, &{&1.name, &1})

    Enum.map(definition.checks, fn check ->
      Map.get_lazy(by_name, check.name, fn ->
        check_result(check, :not_run, reason: reason)
      end)
    end)
  end

  defp order_check_results(%Definition{} = definition, results) do
    by_name = Map.new(results, &{&1.name, &1})
    Enum.map(definition.checks, &Map.fetch!(by_name, &1.name))
  end

  defp checked_target(%Session{} = session, %Render{} = rendered) do
    case SQLClient.relation(session, target_ref(rendered)) do
      {:ok, nil} ->
        {:ok, false}

      {:ok, %{type: :view}} ->
        {:error,
         %Error{
           type: :unsupported_materialization,
           phase: :materialize,
           asset_ref: rendered.asset_ref,
           message: "checked materialization cannot replace an existing view",
           details: %{target: target_ref(rendered), existing_type: :view}
         }}

      {:ok, _relation} ->
        {:ok, true}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp candidate_stage(%Definition{} = definition) do
    if match?(%Contract{}, definition.contract) or Enum.any?(definition.checks, & &1.uses_query?) do
      "favn_check_candidate_#{System.unique_integer([:positive, :monotonic])}"
    end
  end

  defp validate_candidate_contract(_session, %Definition{contract: nil}, _stage, _rendered),
    do: {:ok, nil}

  defp validate_candidate_contract(
         %Session{} = session,
         %Definition{contract: %Contract{} = contract},
         stage,
         %Render{} = rendered
       )
       when is_binary(stage) do
    if function_exported?(session.adapter, :columns, 3) do
      case SQLClient.columns(session, RelationRef.new!(name: stage)) do
        {:ok, columns} when is_list(columns) ->
          validation = ContractValidation.compare(contract, columns)

          if validation.status == :passed do
            {:ok, validation}
          else
            {:error, contract_violation_error(rendered, validation)}
          end

        {:error, reason} ->
          {:error, contract_inspection_error(rendered, session, reason)}
      end
    else
      {:error, contract_inspection_error(rendered, session, :columns_not_supported)}
    end
  end

  defp contract_violation_error(%Render{} = rendered, %ContractValidation{} = validation) do
    %Error{
      type: :contract_violation,
      phase: :before_materialize,
      asset_ref: rendered.asset_ref,
      message: "candidate schema does not satisfy the SQL output contract",
      details: %{contract_validation: validation}
    }
  end

  defp contract_inspection_error(%Render{} = rendered, %Session{} = session, reason) do
    %Error{
      type: :contract_violation,
      phase: :before_materialize,
      asset_ref: rendered.asset_ref,
      message: "candidate schema could not be inspected for the SQL output contract",
      details: %{
        missing_capability: :candidate_columns,
        adapter: session.adapter,
        reason: inspect(reason)
      }
    }
  end

  defp create_candidate_stage(_session, nil, _rendered, _opts), do: :ok

  defp create_candidate_stage(session, stage, %Render{} = rendered, opts) do
    SQLClient.execute(
      session,
      ["CREATE TEMP TABLE ", quote_identifier(stage), " AS ", trim_sql(rendered.sql)],
      Keyword.merge(sql_operation_opts(opts), params: adapter_params(rendered.params))
    )
    |> case do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp finalize_candidate_stage(_session, nil, result, _opts, _definition, _rendered),
    do: result

  defp finalize_candidate_stage(
         session,
         stage,
         {:ok, output},
         opts,
         definition,
         rendered
       ) do
    case SQLClient.execute(
           session,
           ["DROP TABLE IF EXISTS ", quote_identifier(stage)],
           sql_operation_opts(opts)
         ) do
      {:ok, _result} ->
        {:ok, output}

      {:error, reason} ->
        {:error, checked_transaction_error(reason, definition, rendered, output.check_results)}
    end
  end

  defp finalize_candidate_stage(
         _session,
         _stage,
         {:error, _reason} = error,
         _opts,
         _definition,
         _rendered
       ),
       do: error

  defp runtime_relations(%Render{} = rendered, stage) do
    %{
      target: qualified_relation(rendered.relation)
    }
    |> maybe_put_candidate_relation(stage)
  end

  defp maybe_put_candidate_relation(relations, nil), do: relations

  defp maybe_put_candidate_relation(relations, stage),
    do: Map.put(relations, :query, quote_identifier(stage))

  defp staged_materialization_render(%Render{} = rendered, %{query: staged_query}) do
    %Render{
      rendered
      | sql: "SELECT * FROM #{staged_query}",
        params: %Params{format: :positional, bindings: []}
    }
  end

  defp staged_materialization_render(%Render{} = rendered, _runtime_relations), do: rendered

  defp qualified_relation(%RelationRef{} = relation) do
    [relation.catalog, relation.schema, relation.name]
    |> Enum.reject(&is_nil/1)
    |> Enum.map_join(".", &quote_identifier/1)
  end

  defp quote_identifier(value) do
    escaped = value |> to_string() |> String.replace("\"", "\"\"")
    "\"#{escaped}\""
  end

  defp target_ref(%Render{} = rendered) do
    RelationRef.new!(%{
      catalog: rendered.relation.catalog,
      schema: rendered.relation.schema,
      name: rendered.relation.name
    })
  end

  defp check_statement(sql), do: "SELECT * FROM (#{trim_sql(sql)}) AS favn_check LIMIT 2"

  defp no_op_result do
    %Result{
      kind: :materialize,
      command: "CHECKED MATERIALIZATION SKIPPED",
      rows_affected: 0,
      metadata: %{write_outcome: :no_op}
    }
  end

  defp invalid_after_skip_error(%Render{} = rendered) do
    %Error{
      type: :invalid_sql_asset_definition,
      phase: :after_materialize,
      asset_ref: rendered.asset_ref,
      message: "after-materialize checks cannot skip materialization"
    }
  end

  defp checked_transaction_error(%SQLError{} = error, _definition, _rendered, results) do
    details =
      Map.merge(error.details || %{}, %{check_results: results, checked_materialization?: true})

    %SQLError{error | details: details, retryable?: false}
  end

  defp checked_transaction_error(reason, _definition, %Render{} = rendered, results) do
    {message, type, phase, details} = checked_error_fields(reason)

    %SQLError{
      type: :execution_error,
      message: message,
      retryable?: false,
      connection: rendered.connection,
      operation: :transaction,
      details:
        Map.merge(details, %{
          checked_materialization?: true,
          check_results: results,
          sql_asset_error_type: type,
          sql_asset_error_phase: phase
        }),
      cause: reason
    }
  end

  defp checked_error_fields(%Error{} = error),
    do: {error.message, error.type, error.phase, error.details || %{}}

  defp checked_error_fields(reason),
    do:
      {"checked SQL materialization failed", :backend_execution_failed, :materialize,
       %{reason: inspect(reason)}}

  defp map_checked_materialization_result(
         {:ok, %CheckedMaterialization{} = output},
         %Definition{} = definition,
         _rendered
       ) do
    emit_check_telemetry(definition, output.check_results, :committed, output.write_outcome)
    {:ok, output}
  end

  defp map_checked_materialization_result(
         {:error, %SQLError{} = error},
         %Definition{} = definition,
         %Render{} = rendered
       ) do
    {transaction_outcome, write_outcome} = failed_transaction_outcomes(error)

    check_results =
      error
      |> checked_error_results()
      |> case do
        [] -> complete_check_results(definition, [], failed_check_reason(transaction_outcome))
        results -> results
      end

    safe_error = error |> strip_internal_transaction_details() |> SQLError.redact()

    sql_asset_error = %Error{
      type: Map.get(error.details || %{}, :sql_asset_error_type, :backend_execution_failed),
      phase: Map.get(error.details || %{}, :sql_asset_error_phase, :materialize),
      asset_ref: rendered.asset_ref,
      message: safe_error.message || "checked SQL materialization failed",
      details: sql_error_details(safe_error, :materialize),
      cause: safe_error
    }

    emit_check_telemetry(definition, check_results, transaction_outcome, write_outcome)

    meta =
      rendered
      |> failed_check_metadata(check_results, transaction_outcome, write_outcome)
      |> maybe_put_contract_validation(find_contract_validation(error))

    {:error, sql_asset_error, meta}
  end

  defp map_checked_materialization_result(
         {:error, %Error{} = error},
         %Definition{} = definition,
         %Render{} = rendered
       ) do
    results = complete_check_results(definition, [], :transaction_not_started)
    emit_check_telemetry(definition, results, :not_started, :not_started)

    meta =
      rendered
      |> failed_check_metadata(results, :not_started, :not_started)
      |> maybe_put_contract_validation(find_contract_validation(error))

    {:error, error, meta}
  end

  defp map_checked_materialization_result(
         {:error, reason},
         %Definition{} = definition,
         %Render{} = rendered
       ) do
    results = complete_check_results(definition, [], :transaction_failed)
    emit_check_telemetry(definition, results, :unknown, :unknown)

    {:error,
     %Error{
       type: :backend_execution_failed,
       phase: :materialize,
       asset_ref: rendered.asset_ref,
       message: "checked SQL materialization failed",
       cause: reason
     }, failed_check_metadata(rendered, results, :unknown, :unknown)}
  end

  defp checked_error_results(%SQLError{details: details, cause: cause}) do
    details = details || %{}

    case {Map.get(details, :check_results), Map.get(details, :transaction_body_result)} do
      {results, _body_result} when is_list(results) and results != [] ->
        results

      {_results, %CheckedMaterialization{check_results: results}} when results != [] ->
        results

      _other ->
        checked_error_results(cause)
    end
  end

  defp checked_error_results(%Error{cause: cause}), do: checked_error_results(cause)
  defp checked_error_results(_reason), do: []

  defp find_contract_validation(%ContractValidation{} = validation), do: validation

  defp find_contract_validation(%SQLError{details: details, cause: cause}) do
    find_contract_validation(details || %{}) || find_contract_validation(cause)
  end

  defp find_contract_validation(%Error{details: details, cause: cause}) do
    find_contract_validation(details || %{}) || find_contract_validation(cause)
  end

  defp find_contract_validation(%_{}), do: nil

  defp find_contract_validation(value) when is_map(value) do
    Map.get(value, :contract_validation) || Map.get(value, "contract_validation") ||
      Enum.find_value(value, fn {_key, child} -> find_contract_validation(child) end)
  end

  defp find_contract_validation(value) when is_list(value),
    do: Enum.find_value(value, &find_contract_validation/1)

  defp find_contract_validation(_value), do: nil

  defp strip_internal_transaction_details(%SQLError{} = error) do
    %SQLError{
      error
      | details: strip_internal_transaction_details(error.details || %{}),
        cause: strip_internal_transaction_details(error.cause)
    }
  end

  defp strip_internal_transaction_details(%Error{} = error) do
    %Error{
      error
      | details: strip_internal_transaction_details(error.details || %{}),
        cause: strip_internal_transaction_details(error.cause)
    }
  end

  defp strip_internal_transaction_details(%_{} = value), do: value

  defp strip_internal_transaction_details(value) when is_map(value) do
    value
    |> Map.drop([:transaction_body_result, "transaction_body_result"])
    |> Map.new(fn {key, child} -> {key, strip_internal_transaction_details(child)} end)
  end

  defp strip_internal_transaction_details(value) when is_list(value),
    do: Enum.map(value, &strip_internal_transaction_details/1)

  defp strip_internal_transaction_details(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&strip_internal_transaction_details/1)
    |> List.to_tuple()
  end

  defp strip_internal_transaction_details(value), do: value

  defp failed_transaction_outcomes(%SQLError{} = error) do
    cond do
      transaction_not_started?(error) -> {:not_started, :not_started}
      unknown_transaction_outcome?(error) -> {:unknown, :unknown}
      true -> {:rolled_back, :rolled_back}
    end
  end

  defp failed_check_reason(:not_started), do: :transaction_not_started
  defp failed_check_reason(_transaction_outcome), do: :transaction_failed

  defp transaction_not_started?(%SQLError{details: details, cause: cause}) do
    transaction_not_started?(details || %{}) or transaction_not_started?(cause)
  end

  defp transaction_not_started?(%Error{details: details, cause: cause}) do
    transaction_not_started?(details || %{}) or transaction_not_started?(cause)
  end

  defp transaction_not_started?(%_{}), do: false

  defp transaction_not_started?(value) when is_map(value) do
    Map.get(value, :transaction_stage) == :begin or
      Map.get(value, "transaction_stage") == "begin" or
      Enum.any?(value, fn {_key, child} -> transaction_not_started?(child) end)
  end

  defp transaction_not_started?(value) when is_list(value),
    do: Enum.any?(value, &transaction_not_started?/1)

  defp transaction_not_started?(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.any?(&transaction_not_started?/1)
  end

  defp transaction_not_started?(_value), do: false

  defp unknown_transaction_outcome?(%SQLError{details: details, cause: cause}) do
    unknown_transaction_outcome?(details || %{}) or unknown_transaction_outcome?(cause)
  end

  defp unknown_transaction_outcome?(%Error{details: details, cause: cause}) do
    unknown_transaction_outcome?(details || %{}) or unknown_transaction_outcome?(cause)
  end

  defp unknown_transaction_outcome?(%_{}), do: false

  defp unknown_transaction_outcome?(value) when is_map(value) do
    classification = Map.get(value, :classification) || Map.get(value, "classification")

    Map.get(value, :transaction_stage) == :rollback or
      Map.get(value, "transaction_stage") == "rollback" or
      Map.get(value, :unknown_outcome?) == true or
      Map.get(value, "unknown_outcome?") == true or
      classification in [
        :unknown_commit_state,
        :unknown_outcome_timeout,
        "unknown_commit_state",
        "unknown_outcome_timeout"
      ] or
      Enum.any?(value, fn {_key, child} -> unknown_transaction_outcome?(child) end)
  end

  defp unknown_transaction_outcome?(value) when is_list(value),
    do: Enum.any?(value, &unknown_transaction_outcome?/1)

  defp unknown_transaction_outcome?(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.any?(&unknown_transaction_outcome?/1)
  end

  defp unknown_transaction_outcome?(_value), do: false

  defp failed_check_metadata(
         %Render{} = rendered,
         check_results,
         transaction_outcome,
         write_outcome
       ) do
    %{
      connection: rendered.connection,
      check_results: check_results,
      quality_status: :failed,
      transaction_outcome: transaction_outcome,
      write_outcome: write_outcome
    }
  end

  defp check_error_reason(%Error{type: type}), do: type

  defp normalize_check_execution_error(%Error{} = error, _check, _asset_ref), do: error

  defp normalize_check_execution_error(%SQLError{} = error, %Check{} = check, asset_ref) do
    %Error{
      type: :backend_execution_failed,
      phase: check.at,
      asset_ref: asset_ref,
      message: "SQL check #{inspect(check.name)} could not be executed",
      details: %{check: check.name, sql_error_type: error.type},
      cause: error
    }
  end

  defp normalize_check_execution_error(reason, %Check{} = check, asset_ref) do
    %Error{
      type: :backend_execution_failed,
      phase: check.at,
      asset_ref: asset_ref,
      message: "SQL check #{inspect(check.name)} could not be executed",
      details: %{check: check.name},
      cause: reason
    }
  end

  defp emit_check_telemetry(
         %Definition{} = definition,
         check_results,
         transaction_outcome,
         write_outcome
       ) do
    checks = Map.new(definition.checks, &{&1.name, &1})

    Enum.each(check_results, fn %CheckResult{} = result ->
      check = Map.fetch!(checks, result.name)

      :telemetry.execute(
        [:favn, :sql_asset, :check],
        %{duration_ms: result.duration_ms || 0},
        %{
          check: check.name,
          phase: check.at,
          outcome: result.outcome,
          on_violation: check.on_violation,
          origin: check.origin,
          claim_id: check.claim_id,
          transaction_outcome: transaction_outcome,
          write_outcome: write_outcome
        }
      )
    end)

    :ok
  end

  defp render_for_materialize(
         %Definition{materialization: {:incremental, _opts}} = definition,
         opts
       ) do
    with {:ok, %Render{} = initial_render} <- Renderer.render(definition, opts),
         {:ok, %Runtime{} = runtime_window} <- runtime_window(initial_render, definition),
         {:ok, %IncrementalWindow{} = effective_window} <-
           IncrementalWindow.resolve(runtime_window, definition.asset.window_spec),
         {:ok, %Runtime{} = effective_runtime} <- IncrementalWindow.to_runtime(effective_window),
         {:ok, %Render{} = widened_render} <-
           Renderer.render(definition, put_runtime_window(opts, effective_runtime)) do
      {:ok, widened_render}
    end
  end

  defp render_for_materialize(%Definition{} = definition, opts),
    do: Renderer.render(definition, opts)

  defp runtime_window(%Render{} = render, %Definition{} = definition) do
    case render.runtime do
      %Runtime{} = window ->
        {:ok, window}

      _ ->
        {:error,
         %Error{
           type: :materialization_planning_failed,
           phase: :materialize,
           asset_ref: render.asset_ref,
           message: "incremental materialization requires runtime.window",
           details: %{materialization: definition.materialization}
         }}
    end
  end

  defp put_runtime_window(opts, %Runtime{} = runtime_window) do
    runtime_map =
      opts
      |> Keyword.get(:runtime, %{})
      |> case do
        map when is_map(map) -> map
        _ -> %{}
      end

    Keyword.put(opts, :runtime, Map.put(runtime_map, :window, runtime_window))
  end

  defp with_session(connection, opts, required_catalogs, required_resources, fun)
       when is_function(fun, 1) do
    timeout_opts =
      case Keyword.get(opts, :timeout_ms) do
        timeout when is_integer(timeout) and timeout > 0 -> [timeout_ms: timeout]
        _ -> []
      end

    connect_opts =
      timeout_opts
      |> Keyword.put(:registry_name, @runner_registry)
      |> maybe_put_required_catalogs(required_catalogs)
      |> maybe_put_required_resources(required_resources)

    with {:ok, session} <-
           SQLClient.connect(connection, connect_opts) do
      try do
        fun.(session)
      after
        _ = SQLClient.disconnect(session)
      end
    end
  rescue
    error -> {:error, error}
  end

  defp sql_operation_opts(opts) do
    Keyword.take(opts, [:timeout_ms, :deadline, :cancel_token])
  end

  defp runner_runtime_opts(%RunnerWork{metadata: metadata} = work) when is_map(metadata) do
    deadline_at = Map.get(metadata, :deadline_at) || Map.get(metadata, "deadline_at")

    []
    |> Keyword.put(:require_runtime_input_pin, true)
    |> maybe_put_runtime_input_pin(work.runtime_input_pin)
    |> maybe_put_timeout(deadline_at)
    |> Keyword.put(
      :cancel_token,
      CancelToken.new(
        operation_id: Map.get(metadata, :dispatch_id) || Map.get(metadata, "dispatch_id"),
        deadline_at: deadline_at
      )
    )
  end

  defp runner_runtime_opts(%RunnerWork{}), do: []

  defp maybe_put_runtime_input_pin(opts, %Pin{} = pin),
    do: Keyword.put(opts, :runtime_input_pin, pin)

  defp maybe_put_runtime_input_pin(opts, _pin), do: opts

  defp maybe_put_timeout(opts, %DateTime{} = deadline_at) do
    remaining_ms = max(DateTime.diff(deadline_at, DateTime.utc_now(), :millisecond), 1)
    Keyword.put(opts, :timeout_ms, remaining_ms)
  end

  defp maybe_put_timeout(opts, _deadline_at), do: opts

  defp session_required_catalogs(%Definition{} = definition, %Render{} = rendered) do
    # Catalog scope comes from manifest-declared relation ownership and resolved
    # Favn asset references, not by parsing arbitrary SQL text.
    catalogs =
      [rendered.relation | definition_relation_inputs(definition)]
      |> Enum.concat(resolved_relations(rendered.resolved_asset_refs))
      |> Enum.flat_map(&relation_catalog/1)
      |> Enum.uniq()

    catalogs
  end

  defp definition_relation_inputs(%Definition{relation_inputs: inputs}) when is_list(inputs) do
    Enum.map(inputs, fn input ->
      Map.get(input, :relation_ref) || Map.get(input, "relation_ref")
    end)
  end

  defp definition_relation_inputs(%Definition{}), do: []

  defp resolved_relations(refs) when is_list(refs) do
    Enum.map(refs, fn ref -> Map.get(ref, :relation) || Map.get(ref, "relation") end)
  end

  defp resolved_relations(_refs), do: []

  defp relation_catalog(%RelationRef{catalog: catalog}) when is_binary(catalog) and catalog != "",
    do: [catalog]

  defp relation_catalog(%RelationRef{catalog: catalog})
       when is_atom(catalog) and not is_nil(catalog),
       do: [Atom.to_string(catalog)]

  defp relation_catalog(%{} = relation) do
    case Map.get(relation, :catalog) || Map.get(relation, "catalog") do
      catalog when is_binary(catalog) and catalog != "" -> [catalog]
      catalog when is_atom(catalog) and not is_nil(catalog) -> [Atom.to_string(catalog)]
      _catalog -> []
    end
  end

  defp relation_catalog(_relation), do: []

  defp maybe_put_required_catalogs(opts, catalogs),
    do: Keyword.put(opts, :required_catalogs, catalogs)

  defp session_required_resources(%Definition{session_requirements: requirements}) do
    requirements
    |> Favn.SQL.SessionRequirements.validate!()
    |> Map.fetch!(:resources)
  end

  defp maybe_put_required_resources(opts, []), do: opts

  defp maybe_put_required_resources(opts, resources),
    do: Keyword.put(opts, :required_resources, resources)

  defp map_sql_result_error({:ok, result}, _asset_ref, _phase), do: {:ok, result}

  defp map_sql_result_error({:ok, write_plan, result}, _asset_ref, _phase),
    do: {:ok, write_plan, result}

  defp map_sql_result_error({:error, %SQLError{} = error}, asset_ref, phase) do
    safe_error = SQLError.redact(error)

    {:error,
     %Error{
       type: :backend_execution_failed,
       phase: phase,
       asset_ref: asset_ref,
       message: safe_error.message || "SQL execution failed",
       details: sql_error_details(safe_error, phase),
       cause: safe_error
     }}
  end

  defp map_sql_result_error({:error, %Error{} = error}, _asset_ref, _phase), do: {:error, error}

  defp map_sql_result_error({:error, reason}, asset_ref, phase) do
    {:error,
     %Error{
       type: :backend_execution_failed,
       phase: phase,
       asset_ref: asset_ref,
       message: "SQL execution failed",
       cause: reason
     }}
  end

  defp trim_sql(sql) when is_binary(sql) do
    sql
    |> String.trim()
    |> String.trim_trailing(";")
    |> String.trim()
  end

  defp adapter_params(%Params{} = params), do: Params.to_adapter_params(params)

  defp sql_error_details(%SQLError{} = error, phase) do
    classification = Map.get(error.details || %{}, :classification)

    %{
      connection: error.connection,
      operation: error.operation,
      retryable?: error.retryable? == true,
      classification: classification,
      asset_retryable?: sql_asset_retryable?(phase, error, classification)
    }
  end

  defp sql_asset_retryable?(phase, %SQLError{} = error, classification) do
    cond do
      classification in [:unknown_commit_state, :unknown_outcome_timeout] ->
        false

      phase in [:preview, :explain] ->
        error.retryable? == true

      error.operation in [:connect, :bootstrap] ->
        error.retryable? != false

      true ->
        false
    end
  end

  defp manifest_definition(
         %Asset{type: :sql, sql_execution: %SQLExecution{} = payload} = asset,
         %Version{} = version
       ) do
    asset_stub = %{
      module: asset.module,
      name: asset.name,
      entrypoint: :asset,
      ref: asset.ref,
      arity: 1,
      type: :sql,
      file: "manifest",
      line: 1,
      config: asset.config || %{},
      window_spec: asset.window,
      relation: asset.relation,
      materialization: asset.materialization,
      relation_inputs: asset.relation_inputs || [],
      session_requirements: asset.session_requirements
    }

    {:ok,
     %Definition{
       module: asset.module,
       asset: asset_stub,
       sql: payload.sql,
       template: payload.template,
       materialization: asset.materialization,
       relation_inputs: asset.relation_inputs || [],
       sql_definitions: payload.sql_definitions,
       checks: payload.checks,
       contract: payload.contract,
       runtime_inputs: payload.runtime_inputs,
       session_requirements: asset.session_requirements,
       raw_asset: %{
         manifest_relation_by_module: relation_map(version),
         deferred_resolution: :manifest_only
       }
     }}
  end

  defp manifest_definition(%Asset{} = asset, _version) do
    {:error,
     %Error{
       type: :invalid_sql_asset_definition,
       phase: :runtime,
       asset_ref: asset.ref,
       message: "manifest SQL execution payload is missing",
       details: %{asset_ref: asset.ref}
     }}
  end

  defp relation_map(%Version{manifest: %{assets: assets}}) when is_list(assets) do
    assets
    |> Enum.reduce(%{}, fn
      %Asset{module: module, relation: %RelationRef{} = relation}, acc
      when is_atom(module) ->
        Map.put(acc, module, relation)

      _, acc ->
        acc
    end)
  end

  defp relation_map(_), do: %{}

  defp runtime_output(
         %Definition{},
         %Render{} = rendered,
         %CheckedMaterialization{} = materialization,
         resolution
       ) do
    quality_status =
      if Enum.any?(
           materialization.check_results,
           &(&1.outcome in [:warned, :materialization_skipped])
         ),
         do: :warning,
         else: :passed

    output = %{
      materialized: rendered.relation,
      connection: rendered.connection,
      rows_affected: materialization.result.rows_affected,
      command: materialization.result.command,
      check_results: materialization.check_results,
      quality_status: quality_status,
      write_outcome: materialization.write_outcome,
      reason: materialization.reason
    }

    output = maybe_put_contract_validation(output, materialization.contract_validation)

    output
    |> maybe_put_no_op_details(materialization)
    |> maybe_put_runtime_inputs(resolution)
  end

  defp maybe_put_contract_validation(output, nil), do: output

  defp maybe_put_contract_validation(output, %ContractValidation{} = validation),
    do: Map.put(output, :contract_validation, validation)

  defp maybe_put_runtime_inputs(output, nil), do: output

  defp maybe_put_runtime_inputs(output, %RuntimeInputResolution{} = resolution),
    do: Map.put(output, :runtime_inputs, RuntimeInputResolver.lineage(resolution))

  defp maybe_put_no_op_details(
         output,
         %CheckedMaterialization{write_outcome: :no_op, reason: reason, check_results: results}
       ) do
    case Enum.find(results, &(&1.name == reason)) do
      %CheckResult{} = result ->
        output
        |> Map.put(:message, result.message)
        |> Map.put(:metrics, result.metrics)

      nil ->
        output
    end
  end

  defp maybe_put_no_op_details(output, _materialization), do: output
end
