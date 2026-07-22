defmodule Favn.Local.ComposeExecutionAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{ComposeLifecycle, Docker, Install, OrchestratorClient, Reset, State}

  @moduletag :integration
  @moduletag :acceptance
  @moduletag timeout: 1_200_000

  @pipeline_id "pipeline:Elixir.FavnIssue262Sample.Pipelines.ProductionSmoke:production_smoke"

  setup do
    candidate =
      System.get_env("FAVN_CONTROL_PLANE_CANDIDATE") ||
        raise "FAVN_CONTROL_PLANE_CANDIDATE must name the repository-built candidate image"

    {:ok, image} = Docker.inspect_image(candidate)
    root_dir = Favn.Local.CanonicalSampleProject.create!("favn_local_compose_execution")
    run_mix!(root_dir, ["deps.get"])

    opts = [
      root_dir: root_dir,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      candidate_control_plane: %{"reference" => candidate, "image_id" => image.id},
      web_port: free_port(),
      orchestrator_port: free_port(),
      progress_fun: fn _message -> :ok end,
      ready_timeout_ms: 180_000,
      docker_build_timeout_ms: 1_200_000,
      compose_command_timeout_ms: 600_000,
      runner_build_fun: &build_consumer_runner/1,
      env_file_loaded: %{
        "FAVN_CANONICAL_DUCKDB_PATH" => "/tmp/favn/canonical-acceptance.duckdb",
        "FAVN_CANONICAL_SOURCE_NAME" => "compose-acceptance",
        "FAVN_CANONICAL_SOURCE_TOKEN" => "compose-acceptance-source-token",
        "FAVN_CANONICAL_MISSING_SECRET" => "compose-acceptance-present-secret"
      },
      foreground: false
    ]

    assert {:ok, :installed} = Install.run(opts)

    on_exit(fn ->
      _ = ComposeLifecycle.stop(opts)
      _ = Reset.run(Keyword.put(opts, :yes, true))
      File.rm_rf(root_dir)
    end)

    %{opts: opts, root_dir: root_dir}
  end

  test "canonical customer runner executes Elixir and SQL assets", context do
    assert {:ok, started} = ComposeLifecycle.start(context.opts)
    assert {:ok, secrets} = State.read_secrets(root_dir: context.root_dir)

    assert {:ok, session} =
             OrchestratorClient.password_login(
               started.orchestrator_url,
               secrets["service_token"],
               "local-dev",
               "admin",
               secrets["bootstrap_password"]
             )

    assert {:ok, %{"manifest" => manifest}} =
             OrchestratorClient.bootstrap_active_manifest(
               started.orchestrator_url,
               secrets["service_token"],
               session
             )

    payload = %{
      target: %{type: "pipeline", id: @pipeline_id},
      manifest_selection: %{
        mode: "version",
        manifest_version_id: manifest["manifest_version_id"]
      }
    }

    assert {:ok, run} =
             OrchestratorClient.submit_run(
               started.orchestrator_url,
               secrets["service_token"],
               session,
               payload
             )

    terminal =
      await_terminal_run!(started.orchestrator_url, secrets, session, run["id"], context.opts)

    assert terminal["status"] == "ok", compose_logs(context.opts)

    outcomes =
      terminal["asset_results"]
      |> Enum.map(&{&1["asset_ref"], &1["status"]})
      |> Map.new()

    assert outcomes["Elixir.FavnIssue262Sample.Assets.SourceCheck:asset"] == "ok"
    assert outcomes["Elixir.FavnIssue262Sample.Lakehouse.Raw.Sales.Orders:asset"] == "ok"

    assert outcomes[
             "Elixir.FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary:asset"
           ] == "ok"

    summary =
      Enum.find(terminal["asset_results"], fn result ->
        result["asset_ref"] ==
          "Elixir.FavnIssue262Sample.Lakehouse.Mart.Sales.OrderSummary:asset"
      end)

    assert get_in(summary, ["output_metadata", "materialized", "schema"]) == "mart"
    assert get_in(summary, ["output_metadata", "materialized", "name"]) == "order_summary"
  end

  defp build_consumer_runner(opts) do
    root_dir = Favn.Dev.Paths.root_dir(opts) |> Path.expand()

    expression = """
    opts = [
      root_dir: #{inspect(root_dir)},
      skip_compile: true,
      allow_non_prod_build: true,
      allow_unpinned_favn: true
    ]

    case Favn.Dev.build_runner(opts) do
      {:ok, _result} -> :ok
      {:error, reason} -> raise "consumer runner build failed: \#{inspect(reason)}"
    end
    """

    case System.cmd(
           System.find_executable("mix") || "mix",
           ["run", "--no-start", "-e", expression],
           cd: root_dir,
           env: [{"MIX_ENV", "test"}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:runner_release_build_failed, status, bounded(output)}}
    end
  end

  defp await_terminal_run!(base_url, secrets, session, run_id, opts, attempts \\ 240)

  defp await_terminal_run!(base_url, secrets, session, run_id, opts, attempts)
       when attempts > 0 do
    case OrchestratorClient.get_run(base_url, secrets["service_token"], session, run_id) do
      {:ok, %{"status" => status} = run}
      when status in ["ok", "error", "cancelled", "timed_out"] ->
        run

      {:ok, _run} ->
        Process.sleep(250)
        await_terminal_run!(base_url, secrets, session, run_id, opts, attempts - 1)

      {:error, reason} ->
        flunk("run lookup failed: #{inspect(reason)}\n#{compose_logs(opts)}")
    end
  end

  defp await_terminal_run!(_base_url, _secrets, _session, run_id, opts, 0),
    do: flunk("run #{run_id} did not finish\n#{compose_logs(opts)}")

  defp compose_logs(opts) do
    ExUnit.CaptureIO.capture_io(fn ->
      _ = ComposeLifecycle.logs(Keyword.put(opts, :tail, 300))
    end)
  end

  defp run_mix!(root_dir, args) do
    case System.cmd(System.find_executable("mix") || "mix", args,
           cd: root_dir,
           env: [{"MIX_ENV", "test"}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> flunk("mix #{Enum.join(args, " ")} failed (#{status}):\n#{output}")
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp bounded(output) when is_binary(output),
    do: output |> String.trim() |> String.slice(-8_192, 8_192)
end
