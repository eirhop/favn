defmodule Favn.Dev.RunTest do
  use ExUnit.Case, async: true

  alias Favn.Dev
  alias Favn.Dev.Run
  alias Favn.Dev.State

  setup do
    root_dir = Path.join(System.tmp_dir!(), "favn_dev_run_test_#{System.unique_integer([:positive])}")
    File.mkdir_p!(root_dir)

    on_exit(fn -> File.rm_rf(root_dir) end)

    %{root_dir: root_dir}
  end

  test "run_pipeline/2 fails when stack is not running", %{root_dir: root_dir} do
    assert {:error, :stack_not_running} = Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir)
  end

  test "resolve_pipeline_target/2 finds active manifest pipeline by module label" do
    active_manifest = %{
      "targets" => %{
        "pipelines" => [
          %{
            "target_id" => "pipeline:Elixir.MyApp.Pipeline",
            "label" => "MyApp.Pipeline"
          }
        ]
      }
    }

    assert {:ok, %{"target_id" => "pipeline:Elixir.MyApp.Pipeline"}} =
             Run.resolve_pipeline_target(active_manifest, MyApp.Pipeline)
  end

  test "resolve_pipeline_target/2 reports available pipelines on miss" do
    active_manifest = %{
      "targets" => %{
        "pipelines" => [%{"target_id" => "pipeline:Elixir.Other", "label" => "Other"}]
      }
    }

    assert {:error, {:pipeline_not_found, "Missing.Pipeline", ["Other"]}} =
             Run.resolve_pipeline_target(active_manifest, "Missing.Pipeline")
  end

  test "run_pipeline/2 reports missing local credentials", %{root_dir: root_dir} do
    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    assert :ok =
             State.write_runtime(
               %{
                 "services" => %{
                   "web" => %{"pid" => pid},
                   "orchestrator" => %{"pid" => pid},
                   "runner" => %{"pid" => pid}
                 }
               },
               root_dir: root_dir
             )

    assert :ok = State.write_secrets(%{}, root_dir: root_dir)

    assert {:error, :missing_local_operator_credentials} =
             Dev.run_pipeline(MyApp.Pipeline, root_dir: root_dir)
  end
end
