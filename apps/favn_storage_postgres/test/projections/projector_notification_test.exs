defmodule FavnStoragePostgres.Projections.ProjectorNotificationTest do
  use ExUnit.Case, async: true

  alias FavnStoragePostgres.Projections.Projector

  test "collapses a run scope to its strongest change" do
    base = %{workspace_id: "workspace", run_id: "run", root_run_id: "root"}

    scopes =
      Projector.strongest_notification_scopes([
        Map.put(base, :change, "header"),
        Map.put(base, :change, "steps"),
        Map.put(base, :change, "membership"),
        %{workspace_id: "workspace", run_id: "sibling", root_run_id: "root", change: "windows"}
      ])

    assert Enum.sort_by(scopes, & &1.run_id) == [
             %{
               workspace_id: "workspace",
               run_id: "run",
               root_run_id: "root",
               change: "membership"
             },
             %{
               workspace_id: "workspace",
               run_id: "sibling",
               root_run_id: "root",
               change: "windows"
             }
           ]
  end
end
