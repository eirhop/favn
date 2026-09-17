defmodule FavnDuckdbADBC.SemanticCompilerLifecycleTest do
  use ExUnit.Case, async: true

  @moduletag :adbc_integration

  test "worker confirms exit, escalates blocked calls and handles caller or supervisor death" do
    assert {"ok\n", 0} =
             System.cmd("python3", ["-I", Path.join(__DIR__, "semantic_compiler_lifecycle.py")])
  end
end
