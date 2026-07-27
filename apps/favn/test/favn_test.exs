defmodule FavnPublicTest do
  use ExUnit.Case
  doctest Favn

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_public_operator_#{System.unique_integer([:positive])}"
      )

    %{root_dir: root_dir}
  end

  test "public operator functions resolve the local runtime without starting it", %{
    root_dir: root_dir
  } do
    assert {:error, :not_running} =
             Favn.run(MyApp.Pipelines.Daily,
               root_dir: root_dir,
               dependencies: :none,
               refresh: :force_selected
             )

    assert {:error, :not_running} = Favn.list_runs(root_dir: root_dir, status: :error)
    assert {:error, :not_running} = Favn.get_run("run-1", root_dir: root_dir)
    assert {:error, :not_running} = Favn.run_events("run-1", root_dir: root_dir, limit: 20)
    assert {:error, :not_running} = Favn.cancel_run("run-1", root_dir: root_dir)
    assert {:error, :not_running} = Favn.diagnostics(root_dir: root_dir)
  end
end
