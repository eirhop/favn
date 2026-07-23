defmodule FavnStoragePostgres.OperatorReads.CoverageTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias FavnOrchestrator.Persistence.Queries.CountSuccessfulAssetWindows
  alias FavnOrchestrator.Persistence.Queries.GetSuccessfulAssetWindowKeys
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.OperatorReads.Store
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.AssetWindowState
  alias FavnStoragePostgres.StorageV2.Migrations

  setup_all do
    url =
      System.get_env("FAVN_DATABASE_URL") ||
        raise "FAVN_DATABASE_URL is required for PostgreSQL storage tests"

    {:ok, options} = Config.repo_options(url: url, ssl_mode: :disable, pool: Sandbox)
    start_supervised!({Repo, options})
    :ok = Migrations.migrate!(Repo)
    Sandbox.mode(Repo, :manual)
  end

  setup do
    :ok = Sandbox.checkout(Repo)

    suffix = System.unique_integer([:positive]) |> Integer.to_string()
    workspace_id = "coverage-workspace-" <> suffix
    other_workspace_id = "coverage-other-workspace-" <> suffix

    {:ok, context} = WorkspaceContext.new(workspace_id, "operator", [:customer_reader])

    {:ok, other_context} =
      WorkspaceContext.new(other_workspace_id, "operator", [:customer_reader])

    {:ok, context: context, other_context: other_context}
  end

  test "counts and fetches only successful evidence in the pinned generation", fixture do
    first = ~U[2026-07-01 00:00:00.000000Z]
    second = ~U[2026-07-02 00:00:00.000000Z]

    insert_state(fixture.context.workspace_id, "generation-a", "asset-a", "window-a", first)

    insert_state(
      fixture.context.workspace_id,
      "generation-a",
      "asset-a",
      "window-b",
      second,
      "failed"
    )

    insert_state(fixture.context.workspace_id, "generation-b", "asset-a", "window-c", second)
    insert_state(fixture.context.workspace_id, "generation-a", "asset-b", "window-d", second)

    insert_state(
      fixture.other_context.workspace_id,
      "generation-a",
      "asset-a",
      "window-e",
      second
    )

    assert {:ok, 1} =
             Store.count_successful_asset_windows(%CountSuccessfulAssetWindows{
               workspace_context: fixture.context,
               evidence_generation_id: "generation-a",
               target_id: "asset-a",
               first_window_start: first,
               last_window_start: second
             })

    assert {:ok, ["window-a"]} =
             Store.get_successful_asset_window_keys(%GetSuccessfulAssetWindowKeys{
               workspace_context: fixture.context,
               evidence_generation_id: "generation-a",
               target_id: "asset-a",
               window_keys: ["window-a", "window-b", "window-c", "window-e"]
             })

    assert {:ok, 0} =
             Store.count_successful_asset_windows(%CountSuccessfulAssetWindows{
               workspace_context: fixture.context,
               evidence_generation_id: "generation-a",
               target_id: "asset-a",
               first_window_start: DateTime.add(second, 86_400, :second),
               last_window_start: DateTime.add(second, 172_800, :second)
             })
  end

  defp insert_state(
         workspace_id,
         generation_id,
         target_id,
         window_key,
         window_start,
         status \\ "succeeded"
       ) do
    now = DateTime.utc_now()

    Repo.insert!(%AssetWindowState{
      workspace_id: workspace_id,
      evidence_generation_id: generation_id,
      manifest_version_id: "manifest-coverage",
      target_id: target_id,
      window_key: window_key,
      window_start: window_start,
      window_end: DateTime.add(window_start, 86_400, :second),
      status: status,
      payload: %{},
      source_publication_id: System.unique_integer([:positive]),
      updated_at: now
    })
  end
end
