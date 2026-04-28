defmodule Favn.Dev.DoctorTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Doctor

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_doctor_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule DoctorSample.MixProject do end\n")
    File.write!(Path.join(root_dir, "config/config.exs"), "import Config\n")

    keys = [:asset_modules, :pipeline_modules, :connection_modules, :connections, :runner_plugins]
    previous = Map.new(keys, &{&1, Application.get_env(:favn, &1, :__missing__)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, :__missing__} -> Application.delete_env(:favn, key)
        {key, value} -> Application.put_env(:favn, key, value)
      end)

      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "passes for configured modules, connections, plugins, and manifest", %{root_dir: root_dir} do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "Configured#{suffix}"])
    asset = Module.concat([base, Asset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    plugin = Module.concat([base, Plugin])

    Code.compile_string("""
    defmodule #{inspect(plugin)} do
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :warehouse,
          adapter: #{inspect(plugin)},
          config_schema: []
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      @relation [connection: :warehouse, schema: "raw", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_smoke do
        asset(#{inspect(asset)})
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])
    Application.put_env(:favn, :connections, warehouse: [database: ":memory:"])
    Application.put_env(:favn, :runner_plugins, [{plugin, []}])

    assert {:ok, checks} = Doctor.run(root_dir: root_dir)
    assert Enum.any?(checks, &(&1.name == "manifest" and &1.status == :ok))
  end

  test "reports missing local setup", %{root_dir: root_dir} do
    Application.delete_env(:favn, :asset_modules)
    Application.delete_env(:favn, :pipeline_modules)
    Application.delete_env(:favn, :connection_modules)
    Application.delete_env(:favn, :connections)
    Application.delete_env(:favn, :runner_plugins)

    assert {:error, checks} = Doctor.run(root_dir: root_dir)
    assert Enum.any?(checks, &(&1.name == "asset_modules" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "manifest" and &1.status == :ok))
  end
end
