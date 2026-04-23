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

  defmodule MissingRunnerServer do
  end

  defmodule MissingRunner do
  end

  setup do
    cookie = "favn_runner_control_test_cookie"
    assert :ok = NodeControl.ensure_local_node_started(cookie)

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

    %{version: version, cookie: cookie, runner_node_name: Atom.to_string(Node.self())}
  end

  test "register_manifest/2 uses a remote /2 entrypoint when available", ctx do
    assert :ok =
             RunnerControl.register_manifest(ctx.version,
               runner_node_name: ctx.runner_node_name,
               rpc_cookie: ctx.cookie,
               runner_module: StubRunnerV2,
               runner_server_module: MissingRunnerServer
             )
  end

  test "register_manifest/2 falls back to a remote /1 entrypoint", ctx do
    assert :ok =
             RunnerControl.register_manifest(ctx.version,
               runner_node_name: ctx.runner_node_name,
               rpc_cookie: ctx.cookie,
               runner_module: StubRunnerV1,
               runner_server_module: MissingRunnerServer
             )
  end

  test "register_manifest/2 returns a structured error when no entrypoint exists", ctx do
    assert {:error, {:runner_manifest_register_unavailable, runner_node, attempted}} =
             RunnerControl.register_manifest(ctx.version,
               runner_node_name: ctx.runner_node_name,
               rpc_cookie: ctx.cookie,
               runner_module: MissingRunner,
               runner_server_module: MissingRunnerServer
             )

    assert runner_node == Node.self()

    assert attempted == [
             %{module: MissingRunnerServer, function: :register_manifest, arity: 2},
             %{module: MissingRunner, function: :register_manifest, arity: 2},
             %{module: MissingRunner, function: :register_manifest, arity: 1}
           ]
  end
end
