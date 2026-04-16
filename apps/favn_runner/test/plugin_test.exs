defmodule FavnRunner.PluginTest do
  use ExUnit.Case, async: true

  defmodule FakePlugin do
    @behaviour FavnRunner.Plugin

    @impl true
    def child_specs(_opts), do: []
  end

  test "normalizes module and tuple plugin entries" do
    assert {:ok, [{FakePlugin, []}, {FakePlugin, [mode: :test]}]} =
             FavnRunner.Plugin.normalize_config([FakePlugin, {FakePlugin, [mode: :test]}])
  end

  test "rejects invalid plugin entry shape" do
    assert {:error, {:invalid_runner_plugin, :bad}} =
             FavnRunner.Plugin.normalize_config([:bad])
  end
end
