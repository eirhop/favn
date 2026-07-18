defmodule FavnStoragePostgres.OperatorReads.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.OperatorReadStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.FreshnessIdentity
  alias FavnOrchestrator.Persistence.Queries.GetExecutionGroup
  alias FavnOrchestrator.Persistence.Queries.GetAssetDetailState
  alias FavnOrchestrator.Persistence.Queries.GetFreshnessMany
  alias FavnOrchestrator.Persistence.Queries.GetTargetStatuses
  alias FavnOrchestrator.Persistence.Queries.PageExecutionGroups
  alias FavnOrchestrator.Persistence.Queries.PageGroupRuns
  alias FavnOrchestrator.Persistence.Queries.PageGroupWindows
  alias FavnOrchestrator.Persistence.Queries.PageManifests
  alias FavnOrchestrator.Persistence.Queries.PageTargetRuns
  alias FavnOrchestrator.Persistence.Results.BackfillWindow, as: BackfillWindowResult
  alias FavnOrchestrator.Persistence.Results.AssetDetailState, as: AssetDetailStateResult
  alias FavnOrchestrator.Persistence.Results.AssetWindowState, as: AssetWindowResult
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.ExecutionGroup
  alias FavnOrchestrator.Persistence.Results.ExecutionGroupOverview, as: GroupOverviewResult
  alias FavnOrchestrator.Persistence.Results.FreshnessState, as: FreshnessResult
  alias FavnOrchestrator.Persistence.Results.ManifestSummary
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.Results.TargetStatus, as: TargetStatusResult
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.Backfill
  alias FavnStoragePostgres.Schemas.BackfillWindow
  alias FavnStoragePostgres.Schemas.AssetFreshnessState
  alias FavnStoragePostgres.Schemas.AssetWindowState
  alias FavnStoragePostgres.Schemas.ExecutionGroupOverview
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunTarget
  alias FavnStoragePostgres.Schemas.TargetStatus
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment

  @max_batch 500
  @group_statuses [:pending, :running, :succeeded, :failed]
  @run_summary_fields [
    :workspace_id,
    :run_id,
    :root_execution_group_id,
    :parent_run_id,
    :rerun_of_run_id,
    :deployment_id,
    :manifest_version_id,
    :status,
    :submit_kind,
    :trigger_type,
    :event_sequence,
    :submitted_event_id,
    :latest_event_id,
    :inserted_at,
    :updated_at,
    :terminal_at
  ]

  @impl true
  def page_manifests(%PageManifests{} = page) do
    with :ok <- validate_manifest_page(page) do
      query =
        ManifestVersion
        |> after_manifest(page.after)
        |> order_by([manifest], desc: manifest.inserted_at, desc: manifest.manifest_version_id)
        |> limit(^(page.limit + 1))
        |> select([manifest], %ManifestVersion{
          manifest_version_id: manifest.manifest_version_id,
          content_hash: manifest.content_hash,
          schema_version: manifest.schema_version,
          runner_contract_version: manifest.runner_contract_version,
          inserted_at: manifest.inserted_at
        })

      rows = Repo.all(query)
      {:ok, cursor_page(rows, page.limit, &manifest_result/1, &manifest_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_execution_groups(%PageExecutionGroups{} = page) do
    with :ok <- validate_group_page(page) do
      query =
        ExecutionGroupOverview
        |> group_scope(page.scope)
        |> group_status(page.status)
        |> after_group(page.after)
        |> order_by(
          [group],
          desc: group.latest_event_id,
          asc: group.workspace_id,
          asc: group.root_run_id
        )
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)
      {:ok, cursor_page(rows, page.limit, &group_result/1, &group_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_execution_group(%GetExecutionGroup{} = query) do
    with :ok <- validate_get_group(query),
         %ExecutionGroupOverview{} = overview <-
           Repo.get_by(ExecutionGroupOverview,
             workspace_id: query.workspace_context.workspace_id,
             root_run_id: query.root_run_id
           ),
         {:ok, runs} <-
           page_group_runs(%PageGroupRuns{
             workspace_context: query.workspace_context,
             root_run_id: query.root_run_id,
             limit: query.detail_limit
           }),
         {:ok, windows} <-
           page_group_windows(%PageGroupWindows{
             workspace_context: query.workspace_context,
             root_run_id: query.root_run_id,
             limit: query.detail_limit
           }),
         {:ok, failures} <-
           failure_page(query.workspace_context, query.root_run_id, query.detail_limit) do
      {:ok,
       %ExecutionGroup{
         overview: group_result(overview),
         runs: runs,
         windows: windows,
         failures: failures
       }}
    else
      nil -> {:error, Error.new(:not_found, "execution group not found")}
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_group_runs(%PageGroupRuns{} = page) do
    with :ok <- validate_group_runs(page) do
      query =
        Run
        |> where(
          [run],
          run.workspace_id == ^page.workspace_context.workspace_id and
            run.root_execution_group_id == ^page.root_run_id
        )
        |> after_group_run(page.after)
        |> order_by([run], desc: run.submitted_event_id, desc: run.run_id)
        |> select([run], struct(run, ^@run_summary_fields))
        |> limit(^(page.limit + 1))

      rows = Repo.all(query)
      run_page(rows, page.limit)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_group_windows(%PageGroupWindows{} = page) do
    with :ok <- validate_group_windows(page) do
      workspace_id = page.workspace_context.workspace_id

      query =
        from(window in BackfillWindow,
          join: backfill in Backfill,
          on:
            backfill.workspace_id == window.workspace_id and
              backfill.backfill_id == window.backfill_id,
          where:
            backfill.workspace_id == ^workspace_id and
              backfill.root_run_id == ^page.root_run_id,
          order_by: [desc: window.window_start, desc: window.window_id],
          limit: ^(page.limit + 1),
          select: window
        )
        |> after_group_window(page.after)

      rows = Repo.all(query)
      {:ok, cursor_page(rows, page.limit, &window_result/1, &window_cursor/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_target_statuses(%GetTargetStatuses{} = query) do
    with :ok <- validate_target_statuses(query) do
      workspace_id = query.workspace_context.workspace_id
      target_kind = Atom.to_string(query.target_kind)

      rows =
        from(status in TargetStatus,
          join: deployment in WorkspaceDeployment,
          on:
            deployment.workspace_id == status.workspace_id and
              deployment.deployment_id == status.deployment_id,
          where:
            status.workspace_id == ^workspace_id and
              deployment.manifest_version_id == ^query.manifest_version_id and
              status.target_kind == ^target_kind and status.target_id in ^query.target_ids,
          select: status
        )
        |> Repo.all()
        |> Map.new(&{&1.target_id, &1})

      {:ok,
       query.target_ids
       |> Enum.map(&Map.get(rows, &1))
       |> Enum.reject(&is_nil/1)
       |> Enum.map(&target_status_result/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_target_runs(%PageTargetRuns{} = page) do
    with :ok <- validate_target_runs(page) do
      workspace_id = page.workspace_context.workspace_id
      target_kind = Atom.to_string(page.target_kind)

      query =
        from(target in RunTarget,
          join: run in Run,
          on: run.workspace_id == target.workspace_id and run.run_id == target.run_id,
          where:
            target.workspace_id == ^workspace_id and
              target.deployment_id == ^page.deployment_id and
              target.target_kind == ^target_kind and target.target_id == ^page.target_id,
          order_by: [desc: target.submitted_event_id, desc: target.run_id],
          limit: ^(page.limit + 1),
          select: struct(run, ^@run_summary_fields)
        )
        |> after_target_run(page.after)

      rows = Repo.all(query)
      run_page(rows, page.limit)
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_freshness_many(%GetFreshnessMany{} = query) do
    with :ok <- validate_freshness(query) do
      {deployment_ids, target_ids, freshness_keys} = freshness_arrays(query.identities)

      %{rows: rows} =
        SQL.query!(
          Repo,
          """
          WITH requested AS (
            SELECT *
            FROM unnest($2::text[], $3::text[], $4::text[])
              AS item(deployment_id, target_id, freshness_key)
          )
          SELECT state.workspace_id, state.deployment_id, state.target_id,
                 state.freshness_key, state.latest_attempt_materialization_id,
                 state.latest_success_materialization_id, state.status, state.payload,
                 state.source_publication_id, state.updated_at
          FROM requested
          JOIN favn_control.asset_freshness_states state
            ON state.workspace_id = $1
           AND state.deployment_id = requested.deployment_id
           AND state.target_id = requested.target_id
           AND state.freshness_key = requested.freshness_key
          """,
          [
            query.workspace_context.workspace_id,
            deployment_ids,
            target_ids,
            freshness_keys
          ]
        )

      indexed = Map.new(rows, fn row -> {freshness_identity(row), freshness_result(row)} end)

      {:ok,
       query.identities
       |> Enum.map(&Map.get(indexed, {&1.deployment_id, &1.target_id, &1.freshness_key}))
       |> Enum.reject(&is_nil/1)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_asset_detail_state(%GetAssetDetailState{} = query) do
    with :ok <- validate_asset_detail_state(query) do
      workspace_id = query.workspace_context.workspace_id

      freshness_states =
        from(state in AssetFreshnessState,
          where:
            state.workspace_id == ^workspace_id and
              state.deployment_id == ^query.deployment_id and
              state.target_id == ^query.target_id,
          order_by: [desc: state.updated_at, desc: state.freshness_key],
          limit: ^query.limit
        )
        |> Repo.all()
        |> Enum.map(&freshness_result/1)

      window_states =
        from(state in AssetWindowState,
          where:
            state.workspace_id == ^workspace_id and
              state.manifest_version_id == ^query.manifest_version_id and
              state.target_id == ^query.target_id,
          order_by: [desc: state.window_start, desc: state.window_key],
          limit: ^query.limit
        )
        |> Repo.all()
        |> Enum.map(&asset_window_result/1)

      {:ok,
       %AssetDetailStateResult{
         freshness_states: freshness_states,
         window_states: window_states
       }}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp failure_page(context, root_run_id, limit) do
    rows =
      from(run in Run,
        where:
          run.workspace_id == ^context.workspace_id and
            run.root_execution_group_id == ^root_run_id and
            run.status in ["error", "cancelled", "timed_out"],
        order_by: [desc: run.latest_event_id, desc: run.run_id],
        limit: ^(limit + 1),
        select: struct(run, ^@run_summary_fields)
      )
      |> Repo.all()

    run_page(rows, limit)
  end

  defp run_page(rows, limit) do
    page_rows = Enum.take(rows, limit)
    has_more? = length(rows) > limit

    {:ok,
     %CursorPage{
       items: Enum.map(page_rows, &run_result/1),
       limit: limit,
       has_more?: has_more?,
       next_cursor: if(has_more? and page_rows != [], do: run_cursor(List.last(page_rows)))
     }}
  end

  defp cursor_page(rows, limit, mapper, cursor) do
    page_rows = Enum.take(rows, limit)
    items = Enum.map(page_rows, mapper)
    has_more? = length(rows) > limit

    %CursorPage{
      items: items,
      limit: limit,
      has_more?: has_more?,
      next_cursor: if(has_more? and page_rows != [], do: cursor.(List.last(page_rows)))
    }
  end

  defp manifest_result(manifest) do
    %ManifestSummary{
      manifest_version_id: manifest.manifest_version_id,
      content_hash: Base.encode16(manifest.content_hash, case: :lower),
      schema_version: manifest.schema_version,
      runner_contract_version: manifest.runner_contract_version,
      inserted_at: manifest.inserted_at
    }
  end

  defp group_result(group) do
    %GroupOverviewResult{
      workspace_id: group.workspace_id,
      root_run_id: group.root_run_id,
      status: String.to_existing_atom(group.status),
      run_count: group.run_count,
      pending_count: group.pending_count,
      running_count: group.running_count,
      succeeded_count: group.succeeded_count,
      failed_count: group.failed_count,
      latest_event_id: group.latest_event_id,
      source_publication_id: group.source_publication_id,
      updated_at: group.updated_at
    }
  end

  defp run_result(row) do
    %RunSummary{
      workspace_id: row.workspace_id,
      run_id: row.run_id,
      root_run_id: row.root_execution_group_id,
      parent_run_id: row.parent_run_id,
      deployment_id: row.deployment_id,
      manifest_version_id: row.manifest_version_id,
      status: String.to_existing_atom(row.status),
      submit_kind: String.to_existing_atom(row.submit_kind),
      trigger_type: String.to_existing_atom(row.trigger_type),
      submitted_event_id: row.submitted_event_id,
      latest_event_id: row.latest_event_id,
      event_sequence: row.event_sequence,
      inserted_at: row.inserted_at,
      updated_at: row.updated_at,
      terminal_at: row.terminal_at,
      rerun_of_run_id: row.rerun_of_run_id
    }
  end

  defp window_result(window) do
    %BackfillWindowResult{
      workspace_id: window.workspace_id,
      backfill_id: window.backfill_id,
      window_id: window.window_id,
      window_key: window.window_key,
      window_start: window.window_start,
      window_end: window.window_end,
      status: String.to_existing_atom(window.status),
      claim_owner: window.claim_owner,
      fencing_token: window.fencing_token,
      claim_expires_at: window.claim_expires_at,
      run_id: window.run_id,
      attempt_count: window.attempt_count,
      last_error: window.last_error,
      payload: window.payload,
      version: window.version
    }
  end

  defp target_status_result(status) do
    %TargetStatusResult{
      workspace_id: status.workspace_id,
      deployment_id: status.deployment_id,
      target_kind: String.to_existing_atom(status.target_kind),
      target_id: status.target_id,
      status: String.to_existing_atom(status.status),
      run_id: status.run_id,
      event_id: status.event_id,
      source_publication_id: status.source_publication_id,
      updated_at: status.updated_at
    }
  end

  defp freshness_result([
         workspace_id,
         deployment_id,
         target_id,
         freshness_key,
         latest_attempt,
         latest_success,
         status,
         payload,
         publication_id,
         updated_at
       ]) do
    %FreshnessResult{
      workspace_id: workspace_id,
      deployment_id: deployment_id,
      target_id: target_id,
      freshness_key: freshness_key,
      latest_attempt_materialization_id: latest_attempt,
      latest_success_materialization_id: latest_success,
      status: String.to_existing_atom(status),
      payload: payload,
      source_publication_id: publication_id,
      updated_at: updated_at
    }
  end

  defp freshness_result(state) do
    %FreshnessResult{
      workspace_id: state.workspace_id,
      deployment_id: state.deployment_id,
      target_id: state.target_id,
      freshness_key: state.freshness_key,
      latest_attempt_materialization_id: state.latest_attempt_materialization_id,
      latest_success_materialization_id: state.latest_success_materialization_id,
      status: String.to_existing_atom(state.status),
      payload: state.payload,
      source_publication_id: state.source_publication_id,
      updated_at: state.updated_at
    }
  end

  defp asset_window_result(state) do
    %AssetWindowResult{
      workspace_id: state.workspace_id,
      manifest_version_id: state.manifest_version_id,
      target_id: state.target_id,
      window_key: state.window_key,
      window_start: state.window_start,
      window_end: state.window_end,
      status: String.to_existing_atom(state.status),
      run_id: state.run_id,
      materialization_id: state.materialization_id,
      payload: state.payload,
      source_publication_id: state.source_publication_id,
      updated_at: state.updated_at
    }
  end

  defp freshness_identity([_workspace, deployment, target, key | _rest]),
    do: {deployment, target, key}

  defp freshness_arrays(identities) do
    Enum.reduce(identities, {[], [], []}, fn identity, {deployments, targets, keys} ->
      {
        [identity.deployment_id | deployments],
        [identity.target_id | targets],
        [identity.freshness_key | keys]
      }
    end)
    |> then(fn {deployments, targets, keys} ->
      {Enum.reverse(deployments), Enum.reverse(targets), Enum.reverse(keys)}
    end)
  end

  defp manifest_cursor(row),
    do: %{inserted_at: row.inserted_at, manifest_version_id: row.manifest_version_id}

  defp group_cursor(row),
    do: %{
      latest_event_id: row.latest_event_id,
      workspace_id: row.workspace_id,
      root_run_id: row.root_run_id
    }

  defp run_cursor(row),
    do: %{submitted_event_id: row.submitted_event_id, run_id: row.run_id}

  defp window_cursor(row),
    do: %{window_start: row.window_start, window_id: row.window_id}

  defp after_manifest(query, nil), do: query

  defp after_manifest(query, %{inserted_at: inserted_at, manifest_version_id: id}) do
    where(
      query,
      [manifest],
      manifest.inserted_at < ^inserted_at or
        (manifest.inserted_at == ^inserted_at and manifest.manifest_version_id < ^id)
    )
  end

  defp group_scope(query, %WorkspaceContext{workspace_id: workspace_id}),
    do: where(query, [group], group.workspace_id == ^workspace_id)

  defp group_scope(query, %PlatformContext{}), do: query

  defp group_status(query, nil), do: query

  defp group_status(query, status),
    do: where(query, [group], group.status == ^Atom.to_string(status))

  defp after_group(query, nil), do: query

  defp after_group(query, cursor) do
    where(
      query,
      [group],
      group.latest_event_id < ^cursor.latest_event_id or
        (group.latest_event_id == ^cursor.latest_event_id and
           (group.workspace_id > ^cursor.workspace_id or
              (group.workspace_id == ^cursor.workspace_id and
                 group.root_run_id > ^cursor.root_run_id)))
    )
  end

  defp after_group_run(query, nil), do: query

  defp after_group_run(query, cursor) do
    where(
      query,
      [run],
      run.submitted_event_id < ^cursor.submitted_event_id or
        (run.submitted_event_id == ^cursor.submitted_event_id and run.run_id < ^cursor.run_id)
    )
  end

  defp after_group_window(query, nil), do: query

  defp after_group_window(query, cursor) do
    where(
      query,
      [window, _backfill],
      window.window_start < ^cursor.window_start or
        (window.window_start == ^cursor.window_start and window.window_id < ^cursor.window_id)
    )
  end

  defp after_target_run(query, nil), do: query

  defp after_target_run(query, cursor) do
    where(
      query,
      [target, _run],
      target.submitted_event_id < ^cursor.submitted_event_id or
        (target.submitted_event_id == ^cursor.submitted_event_id and
           target.run_id < ^cursor.run_id)
    )
  end

  defp validate_manifest_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{inserted_at: %DateTime{}, manifest_version_id: id} when is_binary(id),
          page.after
        )

    if PlatformContext.valid?(page.platform_context) and cursor? and valid_limit?(page.limit),
      do: :ok,
      else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_page(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{latest_event_id: event_id, workspace_id: workspace_id, root_run_id: run_id}
          when is_integer(event_id) and is_binary(workspace_id) and is_binary(run_id),
          page.after
        )

    if WorkspaceContext.valid?(page.scope) or PlatformContext.valid?(page.scope) do
      if (is_nil(page.status) or page.status in @group_statuses) and cursor? and
           valid_limit?(page.limit),
         do: :ok,
         else: {:error, ErrorMapper.map(:invalid)}
    else
      {:error, ErrorMapper.map(:invalid)}
    end
  end

  defp validate_get_group(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.root_run_id) and
         valid_bound?(query.detail_limit, 1, 200),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_runs(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{submitted_event_id: event_id, run_id: run_id}
          when is_integer(event_id) and is_binary(run_id),
          page.after
        )

    if workspace_context?(page.workspace_context) and valid_id?(page.root_run_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_group_windows(page) do
    cursor? =
      is_nil(page.after) or
        match?(%{window_start: %DateTime{}, window_id: id} when is_binary(id), page.after)

    if workspace_context?(page.workspace_context) and valid_id?(page.root_run_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_target_statuses(query) do
    ids = query.target_ids

    if workspace_context?(query.workspace_context) and valid_id?(query.manifest_version_id) and
         query.target_kind in [:asset, :pipeline] and valid_id_list?(ids),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_target_runs(page) do
    cursor? =
      is_nil(page.after) or
        match?(
          %{submitted_event_id: event_id, run_id: run_id}
          when is_integer(event_id) and is_binary(run_id),
          page.after
        )

    if workspace_context?(page.workspace_context) and valid_id?(page.deployment_id) and
         page.target_kind in [:asset, :pipeline] and valid_id?(page.target_id) and cursor? and
         valid_limit?(page.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_freshness(query) do
    identities = query.identities

    if workspace_context?(query.workspace_context) and is_list(identities) and identities != [] and
         length(identities) <= @max_batch and Enum.all?(identities, &freshness_identity?/1) and
         length(identities) == length(Enum.uniq_by(identities, &Map.from_struct/1)),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp validate_asset_detail_state(query) do
    if workspace_context?(query.workspace_context) and valid_id?(query.deployment_id) and
         valid_id?(query.manifest_version_id) and valid_id?(query.target_id) and
         valid_limit?(query.limit),
       do: :ok,
       else: {:error, ErrorMapper.map(:invalid)}
  end

  defp freshness_identity?(%FreshnessIdentity{} = identity),
    do:
      valid_id?(identity.deployment_id) and valid_id?(identity.target_id) and
        valid_id?(identity.freshness_key)

  defp freshness_identity?(_other), do: false

  defp valid_id_list?(ids),
    do:
      is_list(ids) and ids != [] and length(ids) <= @max_batch and Enum.all?(ids, &valid_id?/1) and
        length(ids) == length(Enum.uniq(ids))

  defp workspace_context?(context), do: WorkspaceContext.valid?(context)

  defp valid_limit?(limit), do: valid_bound?(limit, 1, @max_batch)
  defp valid_bound?(value, min, max), do: is_integer(value) and value >= min and value <= max
  defp valid_id?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255
end
