defmodule Favn.Azure.RunnerPluginTest do
  use ExUnit.Case, async: true

  alias Favn.Azure.RunnerPlugin

  test "contributes one supervised credential subtree" do
    assert {:ok, [:favn_azure]} = RunnerPlugin.applications([])

    assert {:ok, [{Favn.Azure.Credentials.Supervisor, [max_entries: 10]}]} =
             RunnerPlugin.child_specs(max_entries: 10)
  end

  test "rejects unknown options" do
    assert {:error, :invalid_azure_runner_plugin_options} =
             RunnerPlugin.child_specs(unknown: true)
  end
end
