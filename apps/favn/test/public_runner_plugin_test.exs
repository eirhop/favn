defmodule Favn.PublicRunnerPluginTest do
  use ExUnit.Case, async: true

  defmodule ConsumerPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: {:ok, []}
  end

  test "the public package exposes runner extension contracts without favn_runner" do
    assert {:ok, []} = ConsumerPlugin.child_specs([])
    assert {:ok, []} = Favn.Runner.SupervisedChildren.child_specs(children: [])
    assert Code.ensure_loaded?(Favn.RuntimeValue)
  end
end
