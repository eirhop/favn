defmodule Favn.Dev.RunnerControlTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.NodeControl
  alias Favn.Dev.RunnerControl
  alias Favn.Manifest.Version

  defmodule StubRunnerV2 do
    def register_manifest(_version, _opts), do: :ok
  end

  defmodule StubRunnerV1 do
    def register_manifest(_version), do: :ok
  end

  defmodule MissingRunner do
  end

  defmodule DelayedRunner do
    use GenServer

    def start_link(test_pid) when is_pid(test_pid) do
      GenServer.start_link(__MODULE__, test_pid, name: __MODULE__)
    end

    def register_manifest(version), do: GenServer.call(__MODULE__, {:register_manifest, version})

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:register_manifest, version}, _from, test_pid) do
      send(test_pid, {:register_manifest_called, version})
      {:reply, :ok, test_pid}
    end
  end

  setup do
    cookie = "favn_runner_control_test_cookie"

    case NodeControl.ensure_local_node_started(cookie) do
      :ok ->
        build_context(cookie)

      {:error, reason} ->
        {:ok,
         %{
           distributed?: false,
           skip_reason: "distributed Erlang unavailable in test environment: #{inspect(reason)}"
         }}
    end
  end

  test "register_manifest/2 uses a remote /2 entrypoint when available", ctx do
    if maybe_run_distributed?(ctx) do
      assert :ok =
               RunnerControl.register_manifest(ctx.version,
                 runner_node_name: ctx.runner_node_name,
                 rpc_cookie: ctx.cookie,
                 runner_module: StubRunnerV2
               )
    end
  end

  test "register_manifest/2 falls back to a remote /1 entrypoint", ctx do
    if maybe_run_distributed?(ctx) do
      assert :ok =
               RunnerControl.register_manifest(ctx.version,
                 runner_node_name: ctx.runner_node_name,
                 rpc_cookie: ctx.cookie,
                 runner_module: StubRunnerV1
               )
    end
  end

  test "register_manifest/2 returns a structured error when no entrypoint exists", ctx do
    if maybe_run_distributed?(ctx) do
      assert {:error, {:runner_manifest_register_unavailable, runner_node, attempted}} =
               RunnerControl.register_manifest(ctx.version,
                 runner_node_name: ctx.runner_node_name,
                 rpc_cookie: ctx.cookie,
                 runner_module: MissingRunner
               )

      assert runner_node == Node.self()

      assert attempted == [
               %{module: MissingRunner, function: :register_manifest, arity: 2},
               %{module: MissingRunner, function: :register_manifest, arity: 1}
             ]
    end
  end

  test "register_manifest/2 retries startup-unavailable entrypoints until they become ready",
       ctx do
    if maybe_run_distributed?(ctx) do
      test_pid = self()

      on_exit(fn ->
        if pid = Process.whereis(DelayedRunner) do
          GenServer.stop(pid)
        end
      end)

      spawn(fn ->
        Process.sleep(150)
        {:ok, _pid} = DelayedRunner.start_link(test_pid)
      end)

      assert :ok =
               RunnerControl.register_manifest(ctx.version,
                 runner_node_name: ctx.runner_node_name,
                 rpc_cookie: ctx.cookie,
                 runner_module: DelayedRunner
               )

      assert_receive {:register_manifest_called, %Version{} = version}, 5_000
      assert version.manifest_version_id == ctx.version.manifest_version_id
    end
  end

  defp build_context(cookie) do
    manifest = %{
      schema_version: 1,
      runner_contract_version: 1,
      assets: [],
      pipelines: [],
      schedules: [],
      graph: %{},
      metadata: %{}
    }

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_runner_control_test")

    {:ok,
     %{
       distributed?: true,
       version: version,
       cookie: cookie,
       runner_node_name: Atom.to_string(Node.self())
     }}
  end

  defp maybe_run_distributed?(%{distributed?: true}), do: true

  defp maybe_run_distributed?(%{distributed?: false, skip_reason: reason}) do
    IO.puts("Skipping RunnerControl distributed test: #{reason}")
    false
  end
end
