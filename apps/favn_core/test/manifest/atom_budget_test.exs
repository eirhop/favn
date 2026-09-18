defmodule Favn.Manifest.AtomBudgetTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.AtomBudget

  test "checking trusted identifiers does not intern absent atoms" do
    absent = "BudgetProbe#{System.unique_integer([:positive])}"
    assert :ok = AtomBudget.check_headroom(["nil", absent])
    assert_raise ArgumentError, fn -> String.to_existing_atom(absent) end
  end

  test "a small VM atom limit rejects identifiers before creating them" do
    source = Path.expand("../../lib/favn/manifest/atom_budget.ex", __DIR__)

    script = """
    Code.compile_file(#{inspect(source)})
    {:error, {:manifest_atom_headroom_exceeded, _, 32768, 1}} =
      Favn.Manifest.AtomBudget.check_headroom(["NeverInternThisBudgetProbe"])
    try do
      String.to_existing_atom("NeverInternThisBudgetProbe")
      System.halt(2)
    rescue
      ArgumentError -> IO.puts("budget-preserved")
    end
    """

    assert {output, 0} =
             System.cmd(System.find_executable("elixir"), ["-e", script],
               env: [{"ERL_FLAGS", "+S 2:2 +t 32768"}],
               stderr_to_stdout: true
             )

    assert output =~ "budget-preserved"
  end
end
