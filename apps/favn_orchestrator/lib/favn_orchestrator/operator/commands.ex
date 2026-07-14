defmodule FavnOrchestrator.Operator.Commands do
  @moduledoc """
  Executes authenticated operator run and backfill commands.

  The module rehydrates authorization context, validates operator intent,
  resolves manifest-owned targets, and only then calls the owning run or
  backfill runtime. Thin browser, API, and CLI callers share these semantics
  through the public `FavnOrchestrator` facade.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.BackfillManager
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.ManifestTarget
  alias FavnOrchestrator.Operator.Authorization
  alias FavnOrchestrator.OperatorCommands.AssetBackfillRequest
  alias FavnOrchestrator.OperatorCommands.AssetRunRequest
  alias FavnOrchestrator.OperatorCommands.PipelineBackfillRequest
  alias FavnOrchestrator.OperatorCommands.PipelineRunRequest
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
          Authorization.context(),
          String.t(),
          String.t(),
          AssetRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_run(context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    submit_run(context, manifest_version_id, %{type: :asset, id: target_id}, input)
  end

  @doc "Submits an authenticated operator asset or pipeline run."
  @spec submit_run(
          Authorization.context(),
          String.t(),
          run_target(),
          AssetRunRequest.t() | PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_run(context, manifest_version_id, target, input)
      when is_binary(manifest_version_id) and is_map(target) do
    with {:ok, _actor} <- Authorization.authorize(context, :operator),
         {:ok, descriptor} <- target_descriptor(target),
         {:ok, request} <- normalize_run_request(descriptor, input),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, resolved_target} <- resolve_run_target(version, descriptor) do
      submit_resolved_run(version, resolved_target, request)
    end
  end

  def submit_run(_context, _manifest_version_id, _target, _input),
    do: {:error, :invalid_target}

  @doc "Submits an authenticated operator pipeline run."
  @spec submit_pipeline_run(
          Authorization.context(),
          String.t(),
          String.t(),
          PipelineRunRequest.t() | map() | keyword() | nil
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_run(context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    submit_run(context, manifest_version_id, %{type: :pipeline, id: target_id}, input)
  end

  @doc "Submits an authenticated operator asset backfill."
  @spec submit_asset_backfill(
          Authorization.context(),
          String.t(),
          String.t(),
          AssetBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_asset_backfill(context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    with {:ok, _actor} <- Authorization.authorize(context, :operator),
         {:ok, request} <- AssetBackfillRequest.from_input(input),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id),
         {:ok, opts} <- asset_backfill_options(asset.ref, request) do
      BackfillManager.submit_asset_backfill(
        asset.ref,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  @doc "Submits an authenticated operator pipeline backfill."
  @spec submit_pipeline_backfill(
          Authorization.context(),
          String.t(),
          String.t(),
          PipelineBackfillRequest.t() | map() | keyword()
        ) :: {:ok, run_id()} | {:error, term()}
  def submit_pipeline_backfill(context, manifest_version_id, target_id, input)
      when is_binary(manifest_version_id) and is_binary(target_id) do
    with {:ok, _actor} <- Authorization.authorize(context, :operator),
         {:ok, request} <- PipelineBackfillRequest.from_input(input),
         {:ok, version} <- ManifestStore.get_manifest(manifest_version_id),
         {:ok, pipeline_module} <- ManifestTarget.resolve_pipeline_module(version, target_id),
         {:ok, opts} <- pipeline_backfill_options(request) do
      BackfillManager.submit_pipeline_backfill(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, manifest_version_id)
      )
    end
  end

  defp target_descriptor(target) do
    type = field(target, :type)
    id = field(target, :id)
    pipeline_module = field(target, :module)

    cond do
      type in [:asset, "asset"] and is_binary(id) ->
        {:ok, {:asset, id}}

      type in [:pipeline, "pipeline"] and is_binary(id) ->
        {:ok, {:pipeline, id}}

      type in [:pipeline, "pipeline"] and is_atom(pipeline_module) ->
        {:ok, {:pipeline_module, pipeline_module}}

      true ->
        {:error, :invalid_target}
    end
  end

  defp normalize_run_request({:asset, _id}, input), do: AssetRunRequest.from_input(input)

  defp normalize_run_request({kind, _target}, input)
       when kind in [:pipeline, :pipeline_module],
       do: PipelineRunRequest.from_input(input)

  defp resolve_run_target(%Version{} = version, {:asset, target_id}) do
    with {:ok, asset} <- ManifestTarget.resolve_asset(version, target_id) do
      {:ok, {:asset, asset}}
    end
  end

  defp resolve_run_target(%Version{} = version, {:pipeline, target_id}) do
    with {:ok, pipeline_module} <- ManifestTarget.resolve_pipeline_module(version, target_id) do
      {:ok, {:pipeline, pipeline_module}}
    end
  end

  defp resolve_run_target(%Version{} = version, {:pipeline_module, pipeline_module}) do
    if Enum.any?(List.wrap(version.manifest.pipelines), &(&1.module == pipeline_module)) do
      {:ok, {:pipeline, pipeline_module}}
    else
      {:error, :invalid_pipeline_target}
    end
  end

  defp submit_resolved_run(%Version{} = version, {:asset, asset}, %AssetRunRequest{} = request) do
    with {:ok, opts} <- AssetOptions.from_operator_request(asset, request) do
      RunManager.submit_asset_run(
        asset.ref,
        Keyword.put(opts, :manifest_version_id, version.manifest_version_id)
      )
    end
  end

  defp submit_resolved_run(
         %Version{} = version,
         {:pipeline, pipeline_module},
         %PipelineRunRequest{} = request
       ) do
    with {:ok, opts} <- pipeline_run_options(request) do
      RunManager.submit_pipeline_module_run(
        pipeline_module,
        Keyword.put(opts, :manifest_version_id, version.manifest_version_id)
      )
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
       |> maybe_put(:max_attempts, request.max_attempts)
       |> maybe_put(:retry_backoff_ms, request.retry_backoff_ms)
       |> maybe_put(:timeout_ms, request.timeout_ms)}
    end
  end

  defp pipeline_run_options(%PipelineRunRequest{} = request) do
    with {:ok, opts} <- put_metadata([], request.metadata) do
      {:ok,
       opts
       |> maybe_put(:window_request, request.window)
       |> maybe_put(:refresh, pipeline_refresh(request.refresh_mode))
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
       |> maybe_put(:max_attempts, request.max_attempts)
       |> maybe_put(:retry_backoff_ms, request.retry_backoff_ms)
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
