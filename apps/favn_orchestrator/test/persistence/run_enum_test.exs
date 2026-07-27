defmodule FavnOrchestrator.Persistence.RunEnumTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Persistence.RunEnum

  test "decodes the bounded persisted run values" do
    assert RunEnum.decode!(:status, "timed_out") == :timed_out
    assert RunEnum.decode!(:submit_kind, "backfill_pipeline") == :backfill_pipeline
    assert RunEnum.decode!(:trigger_type, "resource_recovery") == :resource_recovery
  end

  test "decodes a backfill summary in a cold BEAM" do
    code = """
    initial =
      try do
        String.to_existing_atom("backfill_pipeline")
        "present"
      rescue
        ArgumentError -> "missing"
      end

    module = Module.concat(["FavnOrchestrator", "Persistence", "RunEnum"])
    decoded = apply(module, :decode!, [:submit_kind, "backfill_pipeline"])

    IO.write(initial <> "|" <> Atom.to_string(decoded))
    """

    executable = System.find_executable("elixir")
    erl_libs = Path.join(Mix.Project.build_path(), "lib")

    assert {"missing|backfill_pipeline", 0} =
             System.cmd(executable, ["-e", code], env: [{"ERL_LIBS", erl_libs}])
  end

  test "rejects unknown persisted values without creating atoms" do
    unknown = "unknown-#{System.unique_integer([:positive])}"

    assert_raise ArgumentError, "invalid persisted run submit_kind", fn ->
      RunEnum.decode!(:submit_kind, unknown)
    end

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown) end
  end
end
