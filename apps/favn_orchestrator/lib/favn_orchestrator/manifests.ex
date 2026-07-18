defmodule FavnOrchestrator.Manifests do
  @moduledoc """
  Context-explicit manifest publication, workspace deployment, and catalogue use cases.

  Global releases are written under platform authority. Customer-visible reads
  are always derived from the workspace's active immutable deployment catalog.
  """

  alias Favn.Manifest.Version
  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Operator.Catalogue.Targets
  alias FavnOrchestrator.Persistence.DeploymentPlanner
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Results.RuntimeState
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @type details :: %{required(:manifest) => map(), required(:targets) => map()}

  @doc "Publishes one immutable platform-global manifest release."
  @spec publish(PlatformContext.t(), Version.t()) ::
          {:ok, :published | :already_published, Version.t()} | {:error, term()}
  def publish(%PlatformContext{} = context, %Version{} = version) do
    ManifestStore.publish_manifest(context, version)
  end

  @doc "Creates and activates one exact workspace deployment."
  @spec deploy(PlatformContext.t(), WorkspaceContext.t(), String.t(), map(), keyword()) ::
          {:ok, FavnOrchestrator.Persistence.Results.RuntimeState.t()} | {:error, term()}
  def deploy(
        %PlatformContext{} = platform_context,
        %WorkspaceContext{} = context,
        manifest_version_id,
        selection,
        opts \\ []
      )
      when is_binary(manifest_version_id) and is_map(selection) and is_list(opts) do
    with true <- platform_deployer?(platform_context),
         {:ok, version} <- ManifestStore.get_manifest(platform_context, manifest_version_id),
         {:ok, planner} <- deployment_selection(version, selection) do
      ManifestStore.deploy_manifest(
        platform_context,
        context,
        manifest_version_id,
        planner,
        Keyword.put_new(opts, :configuration, %{})
      )
    else
      false -> {:error, :platform_operator_required}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns the active release and exact customer-visible targets for one workspace."
  @spec active(WorkspaceContext.t()) :: {:ok, details()} | {:error, term()}
  def active(%WorkspaceContext{} = context) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true) do
      {:ok, details(runtime, grants)}
    end
  end

  @doc "Returns a release only when it is the workspace's active deployment release."
  @spec get_active_release(WorkspaceContext.t(), String.t()) ::
          {:ok, details()} | {:error, term()}
  def get_active_release(%WorkspaceContext{} = context, manifest_version_id)
      when is_binary(manifest_version_id) do
    case active(context) do
      {:ok, %{manifest: %{manifest_version_id: ^manifest_version_id}} = details} -> {:ok, details}
      {:ok, _other} -> {:error, :manifest_not_active_in_workspace}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns the active release only when it grants the exact customer-visible target."
  @spec get_active_target_release(
          WorkspaceContext.t(),
          String.t(),
          :asset | :pipeline,
          String.t()
        ) :: {:ok, Version.t()} | {:error, term()}
  def get_active_target_release(context, manifest_version_id, target_kind, target_id)
      when is_struct(context, WorkspaceContext) and is_binary(manifest_version_id) and
             target_kind in [:asset, :pipeline] and is_binary(target_id) do
    with {:ok, {runtime, grants}} <-
           ManifestStore.get_active_deployment(context, customer_visible_only: true),
         true <- runtime.manifest_version_id == manifest_version_id,
         true <-
           Enum.any?(grants, &(&1.target_kind == target_kind and &1.target_id == target_id)),
         {:ok, version} <- ManifestStore.get_manifest(context, manifest_version_id) do
      {:ok, version}
    else
      false -> {:error, :manifest_or_target_not_active_in_workspace}
      {:error, _reason} = error -> error
    end
  end

  @doc "Returns stable manifest summary data without another storage query."
  @spec summary(Version.t() | RuntimeState.t()) :: map()
  def summary(%Version{} = version) do
    %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      asset_count: length(List.wrap(version.manifest.assets)),
      pipeline_count: length(List.wrap(version.manifest.pipelines)),
      schedule_count: length(List.wrap(version.manifest.schedules))
    }
  end

  def summary(%RuntimeState{} = runtime) do
    %{
      manifest_version_id: runtime.manifest_version_id,
      content_hash: runtime.manifest_content_hash,
      schema_version: runtime.schema_version,
      runner_contract_version: runtime.runner_contract_version,
      asset_count: runtime.asset_count,
      pipeline_count: runtime.pipeline_count,
      schedule_count: runtime.schedule_count
    }
  end

  defp details(%RuntimeState{} = runtime, grants) do
    assets = target_descriptors(grants, :asset)
    pipelines = target_descriptors(grants, :pipeline)

    %{
      manifest: summary(runtime),
      targets: %{
        manifest_version_id: runtime.manifest_version_id,
        assets: assets,
        pipelines: pipelines
      }
    }
  end

  defp target_descriptors(grants, target_kind) do
    grants
    |> Enum.filter(&(&1.target_kind == target_kind))
    |> Enum.map(fn grant ->
      grant.descriptor
      |> Targets.restore_descriptor()
      |> Map.put(:target_id, grant.target_id)
    end)
    |> Enum.sort_by(& &1.target_id)
  end

  defp deployment_selection(version, selection) do
    with {:ok, common_assets} <-
           selected_refs(version.manifest.assets, selection, :common_assets, :asset),
         {:ok, common_pipelines} <-
           selected_refs(version.manifest.pipelines, selection, :common_pipelines, :pipeline),
         {:ok, workspace_assets} <-
           selected_refs(version.manifest.assets, selection, :workspace_assets, :asset),
         {:ok, workspace_pipelines} <-
           selected_refs(version.manifest.pipelines, selection, :workspace_pipelines, :pipeline) do
      {:ok,
       %DeploymentPlanner{
         common_assets: common_assets,
         common_pipelines: common_pipelines,
         workspace_assets: workspace_assets,
         workspace_pipelines: workspace_pipelines
       }}
    end
  end

  defp selected_refs(items, selection, key, kind) do
    value = Map.get(selection, key) || Map.get(selection, Atom.to_string(key)) || []
    available = Map.new(items, &{target_id(&1, kind), target_ref(&1, kind)})

    case value do
      "all" ->
        {:ok, available |> Map.values() |> Enum.sort()}

      ids when is_list(ids) ->
        Enum.reduce_while(ids, {:ok, []}, fn id, {:ok, refs} ->
          case Map.fetch(available, id) do
            {:ok, ref} -> {:cont, {:ok, [ref | refs]}}
            :error -> {:halt, {:error, {:deployment_target_not_found, kind, id}}}
          end
        end)
        |> then(fn
          {:ok, refs} -> {:ok, refs |> Enum.uniq() |> Enum.sort()}
          error -> error
        end)

      _invalid ->
        {:error, {:invalid_deployment_selection, key}}
    end
  end

  defp target_id(asset, :asset), do: TargetIdentity.for_asset(asset.ref)

  defp target_id(pipeline, :pipeline),
    do: TargetIdentity.for_pipeline({pipeline.module, pipeline.name})

  defp target_ref(asset, :asset), do: asset.ref
  defp target_ref(pipeline, :pipeline), do: {pipeline.module, pipeline.name}

  defp platform_deployer?(%PlatformContext{} = context) do
    PlatformContext.valid?(context) and
      Enum.any?(context.roles, &(&1 in [:platform_operator, :platform_admin]))
  end
end
