defmodule FavnRunner.PluginTest do
  use ExUnit.Case, async: true

  alias FavnRunner.{ExtensionSupervisor, PluginLoader}

  defmodule RuntimeService do
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      send(Keyword.fetch!(opts, :test), {:runtime_service_started, self()})
      {:ok, opts}
    end
  end

  defmodule FakePlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(opts), do: {:ok, [{RuntimeService, opts}]}
  end

  defmodule ErrorPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: {:error, :bad_config}
  end

  defmodule InvalidResultPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: []
  end

  defmodule InvalidChildPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: {:ok, [%{start: :invalid}]}
  end

  defmodule DuckTypedPlugin do
    def child_specs(_opts), do: {:ok, []}
  end

  defmodule ApplicationPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def applications(_opts), do: {:ok, [:inets]}

    @impl true
    def child_specs(_opts), do: {:ok, []}
  end

  defmodule SlowChildSpec do
    def child_spec(_opts) do
      Process.sleep(:infinity)
    end
  end

  defmodule SlowChildPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: {:ok, [SlowChildSpec]}
  end

  defmodule TooManyChildrenPlugin do
    @behaviour Favn.Runner.Plugin

    @impl true
    def child_specs(_opts), do: {:ok, List.duplicate(SlowChildSpec, 257)}
  end

  test "loads public plugins and normalizes their child specifications" do
    assert {:ok, [child]} = PluginLoader.load([{FakePlugin, test: self()}])
    assert child.id == RuntimeService
    assert child.start == {RuntimeService, :start_link, [[test: self()]]}
  end

  test "plugin children start before use and follow OTP restart semantics" do
    assert {:ok, children} = PluginLoader.load([{FakePlugin, test: self()}])
    assert {:ok, supervisor} = ExtensionSupervisor.start_link(children: children, name: nil)
    assert_receive {:runtime_service_started, first_pid}

    Process.exit(first_pid, :kill)

    assert_receive {:runtime_service_started, second_pid}
    assert second_pid != first_pid
    assert Process.alive?(supervisor)
  end

  test "supports the built-in supervised children plugin" do
    entries = [
      {Favn.Runner.SupervisedChildren, children: [{RuntimeService, test: self()}]}
    ]

    assert {:ok, [child]} = PluginLoader.load(entries)
    assert child.id == RuntimeService
  end

  test "starts explicitly declared plugin applications before loading children" do
    assert {:ok, []} = PluginLoader.load([ApplicationPlugin])
    assert Enum.any?(Application.started_applications(), &match?({:inets, _, _}, &1))
  end

  test "rejects modules that do not declare the public behaviour" do
    assert {:error, {:invalid_runner_plugin, DuckTypedPlugin}} =
             PluginLoader.load([DuckTypedPlugin])
  end

  test "normalizes callback errors without exposing plugin options" do
    assert {:error, {:plugin_callback_failed, ErrorPlugin, :bad_config}} =
             PluginLoader.load([{ErrorPlugin, secret: "do-not-report"}])
  end

  test "rejects oversized plugin options before invoking the callback" do
    oversized = :binary.copy("x", 1_048_577)

    assert {:error, {:plugin_options_too_large, FakePlugin}} =
             PluginLoader.load([{FakePlugin, payload: oversized}])
  end

  test "rejects old untagged callback results" do
    assert {:error, {:invalid_plugin_result, InvalidResultPlugin}} =
             PluginLoader.load([InvalidResultPlugin])
  end

  test "rejects invalid child specifications with a bounded location" do
    assert {:error, {:invalid_plugin_child_spec, InvalidChildPlugin, 0}} =
             PluginLoader.load([InvalidChildPlugin])
  end

  test "bounds module child_spec expansion inside the plugin callback timeout" do
    started_at = System.monotonic_time(:millisecond)

    assert {:error, {:plugin_callback_failed, SlowChildPlugin, :timeout}} =
             PluginLoader.load([SlowChildPlugin])

    assert System.monotonic_time(:millisecond) - started_at < 6_000
  end

  test "rejects too many children before expanding any child spec" do
    started_at = System.monotonic_time(:millisecond)

    assert {:error, :too_many_plugin_children} = PluginLoader.load([TooManyChildrenPlugin])
    assert System.monotonic_time(:millisecond) - started_at < 1_000
  end

  test "rejects duplicate child ids before starting the extension supervisor" do
    entries = [
      {Favn.Runner.SupervisedChildren, children: [{RuntimeService, test: self()}]},
      {Favn.Runner.SupervisedChildren, children: [{RuntimeService, test: self()}]}
    ]

    assert {:error, :duplicate_plugin_child_id} = PluginLoader.load(entries)
  end

  test "rejects invalid plugin entry shapes without inspecting their values" do
    assert {:error, {:invalid_runner_plugin_entry, 0}} = PluginLoader.load([{"bad", %{}}])
  end
end
