defmodule FavnOrchestrator.Operator.Commands do
  @moduledoc """
  Executes authenticated operator run and backfill commands.

  The module rehydrates authorization context, validates operator intent,
  resolves manifest-owned targets, and only then calls the owning run or
  backfill runtime. Thin browser, API, and CLI callers share these semantics
  through the public `FavnOrchestrator` facade.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Backfills
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Manifests
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunManager
  alias FavnOrchestrator.RunSubmission.AssetOptions

  @type run_id :: String.t()
  @type run_target ::
          %{required(:type) => :asset | :pipeline | String.t(), required(:id) => String.t()}
          | %{
              required(:type) => :pipeline | String.t(),
              required(:module) => module()
            }
          | %{required(String.t()) => term()}

  @doc "Submits an authenticated operator asset run."
  @spec submit_asset_run(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          AssetRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_run(%WorkspaceContext{} = context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    submit_run(context, manifest_version_id, %{type: :asset, id: target_id}, input)
  end

  @doc "Submits an authenticated operator asset or pipeline run."
  @spec submit_run(
          WorkspaceContext.t(),
          String.t(),
          run_target(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_run(%WorkspaceContext{} = context, manifest_version_id, target, input)
      when is_binary(manifest_version_id) and is_map(target) do
    submit_run(context, manifest_version_id, target, input, [])
  end

  @doc false
  @spec submit_run(
          WorkspaceContext.t(),
          String.t(),
          run_target(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil,
          keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_run(%WorkspaceContext{} = context, manifest_version_id, target, input, opts)
      when is_binary(manifest_version_id) and is_map(target) and is_list(opts) do
    with :ok <- authorize_operator(context),
         :ok <- validate_command_opts(opts),
         {:ok, descriptor} <- target_descriptor(target),
         {:ok, request} <- normalize_run_request(descriptor, input),
         {:ok, version} <- active_target_release(context, manifest_version_id, descriptor),
         {:ok, resolved_target} <- resolve_run_target(version, descriptor) do
      submit_resolved_run(context, version, resolved_target, request, opts)
    end
  end

  @doc "Submits an authenticated operator pipeline run."
  @spec submit_pipeline_run(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run(%WorkspaceContext{} = context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    submit_run(context, manifest_version_id, %{type: :pipeline, id: target_id}, input)
  end

  @doc "Submits an authenticated operator asset backfill."
  @spec submit_asset_backfill(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          AssetBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_backfill(%WorkspaceContext{} = context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    with :ok <- authorize_operator(context),
         {:ok, request} <- AssetBackfillRequest.from_input(input),
         {:ok, version} <- ManifestStore.get_manifest(context, manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
         {:ok, opts} <- asset_backfill_options(asset.ref, request),
         {:ok, backfill} <-
           Backfills.submit_asset(context, manifest_version_id, target_id, request.range, opts) do
      {:ok, backfill.root_run_id}
    end
  end

  @doc "Submits an authenticated operator pipeline backfill."
  @spec submit_pipeline_backfill(
          WorkspaceContext.t(),
          String.t(),
          String.t(),
          PipelineBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill(
        %WorkspaceContext{} = context,
        manifest_version_id,
        target_id,
        input
      )
      when is_binary(manifest_version_id) and is_binary(target_id) do
    with :ok <- authorize_operator(context),
         {:ok, request} <- PipelineBackfillRequest.from_input(input),
         {:ok, version} <- ManifestStore.get_manifest(context, manifest_version_id),
         {:ok, _pipeline} <- ManifestTarget.resolve_pipeline(version, target_id),
         {:ok, opts} <- pipeline_backfill_options(request),
         true <- is_nil(request.coverage_baseline_id),
         {:ok, backfill} <-
           Backfills.submit_pipeline(context, manifest_version_id, target_id, request.range, opts) do
      {:ok, backfill.root_run_id}
    else
      false -> {:error, {:unsupported_backfill_option, :coverage_baseline_id}}
      {:error, _reason} = error -> error
    end
  end

  defp target_descriptor(target) do
    type = field(target, :type)
    id = field(target, :id)

    cond do
      type in [:asset, "asset"] and is_binary(id) ->
        {:ok, {:asset, id}}

      type in [:pipeline, "pipeline"] and is_binary(id) ->
        {:ok, {:pipeline, id}}

      true ->
        {:error, :invalid_target}
    end
  end

  defp normalize_run_request({:asset, _id}, input), do: AssetRunRequest.from_input(input)

  defp normalize_run_request({:pipeline, _target}, input),
    do: PipelineRunRequest.from_input(input)

  defp resolve_run_target(%Version{} = version, {:asset, target_id}) do
    with {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      {:ok, {:asset, asset}}
    end
  end

  defp resolve_run_target(%Version{} = version, {:pipeline, target_id}) do
    with {:ok, pipeline} <- ManifestTarget.resolve_pipeline(version, target_id) do
      {:ok, {:pipeline, {pipeline.module, pipeline.name}}}
    end
  end

  defp submit_resolved_run(
         context,
         %Version{} = version,
         {:asset, asset},
         %AssetRunRequest{} = request,
         command_opts
       ) do
    with {:ok, opts} <- AssetOptions.from_operator_request(asset, request) do
      submit_asset_run(
        context,
        asset.ref,
        command_run_opts(opts, version, command_opts)
      )
    end
  end

  defp submit_resolved_run(
         context,
         %Version{} = version,
         {:pipeline, {module, name} = pipeline_ref},
         %PipelineRunRequest{} = request,
         command_opts
       )
       when is_atom(module) and is_atom(name) do
    with {:ok, opts} <- pipeline_run_options(request) do
      submit_pipeline_run(
        context,
        pipeline_ref,
        command_run_opts(opts, version, command_opts)
      )
    end
  end

  defp authorize_operator(%WorkspaceContext{} = context) do
    if Enum.any?(
         context.roles,
         &(&1 in [:customer_operator, :workspace_admin, :platform_operator])
       ),
       do: :ok,
       else: {:error, :forbidden}
  end

  defp active_target_release(context, manifest_version_id, {target_kind, target_id})
       when target_kind in [:asset, :pipeline] do
    Manifests.get_active_target_release(context, manifest_version_id, target_kind, target_id)
  end

  defp submit_asset_run(%WorkspaceContext{} = context, asset_ref, opts),
    do: RunManager.submit_asset_run(context, asset_ref, opts)

  defp submit_pipeline_run(%WorkspaceContext{} = context, {module, name} = pipeline_ref, opts)
       when is_atom(module) and is_atom(name),
       do: RunManager.submit_pipeline_ref_run(context, pipeline_ref, opts)

  defp command_run_opts(opts, version, command_opts) do
    opts
    |> Keyword.put(:manifest_version_id, version.manifest_version_id)
    |> maybe_put(:run_id, Keyword.get(command_opts, :run_id))
    |> maybe_put(:_idempotency, Keyword.get(command_opts, :idempotency))
  end

  defp validate_command_opts(opts) do
    allowed = [:run_id, :idempotency]
    run_id = Keyword.get(opts, :run_id)
    idempotency = Keyword.get(opts, :idempotency)

    cond do
      not Keyword.keyword?(opts) ->
        {:error, :invalid_command_options}

      Keyword.keys(opts) -- allowed != [] ->
        {:error, :invalid_command_options}

      not is_nil(run_id) and not (is_binary(run_id) and run_id != "") ->
        {:error, :invalid_run_id}

      not is_nil(idempotency) and not match?(%CommandIdempotency{}, idempotency) ->
        {:error, :invalid_idempotency_context}

      true ->
        :ok
    end
  end

  defp asset_backfill_options(asset_ref, %AssetBackfillRequest{} = request) do
    with {:ok, refresh} <-
           asset_backfill_refresh(request.refresh_mode, asset_ref, request.dependency_mode),
         {:ok, opts} <- put_metadata([], request.metadata) do
      {:ok,
       opts
       |> Keyword.put(:range_request, request.range)
       |> Keyword.put(:dependencies, request.dependency_mode)
       |> maybe_put(:refresh, refresh)
       |> maybe_put(:retry_policy, request.retry_policy)
       |> maybe_put(:timeout_ms, request.timeout_ms)}
    end
  end

  defp pipeline_run_options(%PipelineRunRequest{} = request) do
    with {:ok, opts} <- put_metadata([], request.metadata) do
      {:ok,
       opts
       |> maybe_put(:window_request, request.window)
       |> maybe_put(:refresh, pipeline_refresh(request.refresh_mode))
       |> maybe_put(:retry_policy, request.retry_policy)
       |> maybe_put(:timeout_ms, request.timeout_ms)}
    end
  end

  defp pipeline_backfill_options(%PipelineBackfillRequest{} = request) do
    with {:ok, opts} <- put_metadata([], request.metadata) do
      {:ok,
       opts
       |> Keyword.put(:range_request, request.range)
       |> maybe_put(:coverage_baseline_id, request.coverage_baseline_id)
       |> maybe_put(:refresh, pipeline_refresh(request.refresh_mode))
       |> maybe_put(:retry_policy, request.retry_policy)
       |> maybe_put(:timeout_ms, request.timeout_ms)}
    end
  end

  defp asset_backfill_refresh(:auto, _asset_ref, _dependencies), do: {:ok, nil}

  defp asset_backfill_refresh(refresh_mode, asset_ref, dependencies),
    do: AssetOptions.operator_refresh(refresh_mode, asset_ref, dependencies)

  defp pipeline_refresh(:auto), do: nil
  defp pipeline_refresh(:missing), do: :missing
  defp pipeline_refresh(:force_all), do: :force

  defp put_metadata(opts, nil), do: {:ok, opts}

  defp put_metadata(opts, metadata) when is_map(metadata),
    do: {:ok, Keyword.put(opts, :metadata, metadata)}

  defp put_metadata(_opts, _metadata), do: {:error, :invalid_run_metadata}

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp field(value, key) when is_map(value) do
    case Map.fetch(value, key) do
      {:ok, field_value} -> field_value
      :error -> Map.get(value, Atom.to_string(key))
    end
  end
end
