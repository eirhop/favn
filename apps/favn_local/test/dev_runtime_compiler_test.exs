defmodule Favn.Dev.RuntimeCompilerTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.RuntimeCompiler

  test "runtime compilation preserves Mix incremental compilation" do
    parent = self()
    runtime = %{"materialized_root" => "/tmp/favn-runtime-compiler"}

    runner = fn executable, args, opts ->
      send(parent, {:command, executable, args, opts})
      {"", 0}
    end

    assert :ok = RuntimeCompiler.compile_runtime(runtime, runtime_command_runner: runner)

    assert_receive {:command, _executable, ["compile"], opts}
    assert opts[:cd] == runtime["materialized_root"]
    assert opts[:env]["MIX_ENV"] == "dev"
  end

  test "runtime compilation reports bounded command failures" do
    runner = fn _executable, _args, _opts -> {" compiler failed \n", 9} end

    assert {:error, {:runtime_compile_failed, :runtime_root, 9, "compiler failed"}} =
             RuntimeCompiler.compile_runtime(
               %{"materialized_root" => "/tmp/favn-runtime-compiler"},
               runtime_command_runner: runner
             )
  end

  test "service overrides do not compile the installed runtime" do
    runner = fn _executable, _args, _opts -> flunk("runtime compiler should not run") end

    assert :ok =
             RuntimeCompiler.compile_runtime(
               %{"materialized_root" => "/tmp/favn-runtime-compiler"},
               service_specs_override: [],
               runtime_command_runner: runner
             )
  end
end
