defmodule Favn.Dev.DoctorTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.{Doctor, Install}
  alias Favn.Dev.Init.Compose, as: ComposeInit

  @control_build_id String.duplicate("d", 64)
  @control_image_id "sha256:" <> String.duplicate("e", 64)

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_doctor_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(root_dir, "config"))
    File.write!(Path.join(root_dir, "mix.exs"), "defmodule DoctorSample.MixProject do end\n")
    File.write!(Path.join(root_dir, "config/config.exs"), "import Config\n")
    assert {:ok, _scaffold} = ComposeInit.run(root_dir: root_dir)

    keys = [
      :asset_modules,
      :pipeline_modules,
      :connection_modules,
      :connections,
      :runner_plugins,
      :discovery
    ]

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
          config_schema: [%{key: :database, type: :string}]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :warehouse, schema: "raw", name: "orders"]
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

    assert {:ok, checks} = doctor(root_dir)
    assert Enum.any?(checks, &(&1.name == "manifest" and &1.status == :ok))
  end

  test "reports missing local setup", %{root_dir: root_dir} do
    Application.delete_env(:favn, :asset_modules)
    Application.delete_env(:favn, :pipeline_modules)
    Application.delete_env(:favn, :connection_modules)
    Application.delete_env(:favn, :connections)
    Application.delete_env(:favn, :runner_plugins)

    assert {:error, checks} = doctor(root_dir)
    assert Enum.any?(checks, &(&1.name == "asset_modules" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "manifest" and &1.status == :ok))
  end

  test "reports malformed config without crashing", %{root_dir: root_dir} do
    Application.put_env(:favn, :asset_modules, :not_a_list)
    Application.put_env(:favn, :pipeline_modules, ["not_a_module"])
    Application.put_env(:favn, :connection_modules, [__MODULE__])
    Application.put_env(:favn, :connections, %{warehouse: []})
    Application.put_env(:favn, :runner_plugins, [{"not_a_module", %{}}])

    assert {:error, checks} = doctor(root_dir)

    assert Enum.any?(checks, &(&1.name == "asset_modules" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "pipeline_modules" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "connections" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "runner_plugins" and &1.status == :error))
    assert Enum.any?(checks, &(&1.name == "manifest" and &1.status == :error))
  end

  test "validates catalog-qualified relations and ignores assets without owned relations", %{
    root_dir: root_dir
  } do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "CatalogConfigured#{suffix}"])
    asset = Module.concat([base, Asset])
    operational_asset = Module.concat([base, OperationalAsset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    adapter = Module.concat([base, Adapter])

    Code.compile_string("""
    defmodule #{inspect(adapter)} do
      def configured_catalogs(resolved), do: {:ok, Map.fetch!(resolved.config, :catalogs)}
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :lakehouse,
          adapter: #{inspect(adapter)},
          config_schema: [
            %{key: :database, type: :string},
            %{key: :catalogs}
          ]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :lakehouse, catalog: :raw, schema: "sales", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(operational_asset)} do
      use Favn.Asset

      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_catalogs do
        assets([#{inspect(asset)}, #{inspect(operational_asset)}])
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset, operational_asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])

    Application.put_env(:favn, :connections,
      lakehouse: [database: ":memory:", catalogs: [:raw, :int]]
    )

    Application.put_env(:favn, :runner_plugins, [{adapter, []}])

    assert {:ok, checks} = doctor(root_dir)

    assert Enum.any?(
             checks,
             &(&1.name == "relation catalogs" and &1.status == :ok and
                 &1.message =~ "validated 1 connection")
           )
  end

  test "reports relation catalogs missing from adapter config", %{root_dir: root_dir} do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "CatalogMissing#{suffix}"])
    asset = Module.concat([base, Asset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    adapter = Module.concat([base, Adapter])

    Code.compile_string("""
    defmodule #{inspect(adapter)} do
      def configured_catalogs(resolved), do: {:ok, Map.fetch!(resolved.config, :catalogs)}
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :lakehouse,
          adapter: #{inspect(adapter)},
          config_schema: [
            %{key: :database, type: :string},
            %{key: :catalogs},
            %{key: :password, secret: true}
          ]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :lakehouse, catalog: :raw, schema: "sales", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_catalogs do
        asset(#{inspect(asset)})
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])

    Application.put_env(:favn, :connections,
      lakehouse: [database: ":memory:", catalogs: [:int], password: "super-secret"]
    )

    Application.put_env(:favn, :runner_plugins, [{adapter, []}])

    assert {:error, checks} = doctor(root_dir)
    check = Enum.find(checks, &(&1.name == "relation catalogs"))

    assert check.status == :error
    assert check.message =~ "missing configured catalog(s) [\"raw\"]"
    refute check.message =~ "super-secret"
  end

  test "skips relation catalog validation for adapters without callback", %{root_dir: root_dir} do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "CatalogSkipped#{suffix}"])
    asset = Module.concat([base, Asset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    adapter = Module.concat([base, Adapter])

    Code.compile_string("""
    defmodule #{inspect(adapter)} do
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :warehouse,
          adapter: #{inspect(adapter)},
          config_schema: [%{key: :database, type: :string}]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :warehouse, catalog: :raw, schema: "sales", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_catalogs do
        asset(#{inspect(asset)})
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])
    Application.put_env(:favn, :connections, warehouse: [database: ":memory:"])
    Application.put_env(:favn, :runner_plugins, [{adapter, []}])

    assert {:ok, checks} = doctor(root_dir)

    assert Enum.any?(
             checks,
             &(&1.name == "relation catalogs" and &1.status == :ok and
                 &1.message =~ "skipped 1 adapter")
           )
  end

  test "reports catalogless relations for catalog-aware adapters without default catalog", %{
    root_dir: root_dir
  } do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "CataloglessMissingDefault#{suffix}"])
    asset = Module.concat([base, Asset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    adapter = Module.concat([base, Adapter])

    Code.compile_string("""
    defmodule #{inspect(adapter)} do
      def configured_catalogs(resolved), do: {:ok, Map.fetch!(resolved.config, :catalogs)}
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :lakehouse,
          adapter: #{inspect(adapter)},
          config_schema: [%{key: :catalogs}]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :lakehouse, schema: "sales", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_catalogs do
        asset(#{inspect(asset)})
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])
    Application.put_env(:favn, :connections, lakehouse: [catalogs: [:raw]])
    Application.put_env(:favn, :runner_plugins, [{adapter, []}])

    assert {:error, checks} = doctor(root_dir)
    check = Enum.find(checks, &(&1.name == "relation catalogs"))

    assert check.status == :error
    assert check.message =~ "catalogless asset relation"
    assert check.message =~ "configure relation.catalog"
  end

  test "allows catalogless relations when adapter default catalog is attached", %{
    root_dir: root_dir
  } do
    suffix = System.unique_integer([:positive])
    base = Module.concat([Favn, Dev, DoctorTest, "CataloglessDefault#{suffix}"])
    asset = Module.concat([base, Asset])
    pipeline = Module.concat([base, Pipeline])
    connection = Module.concat([base, Connection])
    adapter = Module.concat([base, Adapter])

    Code.compile_string("""
    defmodule #{inspect(adapter)} do
      def configured_catalogs(resolved), do: {:ok, Map.fetch!(resolved.config, :catalogs)}
      def default_catalog(_resolved), do: {:ok, :raw}
    end

    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection

      @impl true
      def definition do
        %Favn.Connection.Definition{
          name: :lakehouse,
          adapter: #{inspect(adapter)},
          config_schema: [%{key: :catalogs}]
        }
      end
    end

    defmodule #{inspect(asset)} do
      use Favn.Asset

      relation [connection: :lakehouse, schema: "sales", name: "orders"]
      def asset(_ctx), do: :ok
    end

    defmodule #{inspect(pipeline)} do
      use Favn.Pipeline

      pipeline :doctor_catalogs do
        asset(#{inspect(asset)})
        deps(:all)
      end
    end
    """)

    Application.put_env(:favn, :asset_modules, [asset])
    Application.put_env(:favn, :pipeline_modules, [pipeline])
    Application.put_env(:favn, :connection_modules, [connection])
    Application.put_env(:favn, :connections, lakehouse: [catalogs: [:raw]])
    Application.put_env(:favn, :runner_plugins, [{adapter, []}])

    assert {:ok, checks} = doctor(root_dir)

    assert Enum.any?(checks, &(&1.name == "relation catalogs" and &1.status == :ok))
  end

  defp doctor(root_dir) do
    opts = [
      root_dir: root_dir,
      favn_version: Favn.RunnerRelease.current_favn_version(),
      docker_executable: "docker",
      docker_command_runner: &docker_command/3,
      candidate_control_plane: %{
        "reference" => "favn-control-plane-candidate:#{@control_build_id}",
        "image_id" => @control_image_id
      }
    ]

    assert {:ok, install_status} = Install.run(opts)
    assert install_status in [:installed, :already_installed]
    Doctor.run(opts)
  end

  defp docker_command("docker", args, _opts) do
    case args do
      ["version", "--format", "{{json .Server}}"] ->
        {JSON.encode!(%{"Os" => "linux", "Arch" => "amd64", "Version" => "28.3.0"}), 0}

      ["compose", "version", "--short"] ->
        {"2.39.1\n", 0}

      ["image", "inspect", _reference] ->
        {JSON.encode!([control_image_inspection()]), 0}

      ["compose" | compose_args] ->
        if Enum.take(compose_args, -2) == ["config", "--quiet"] or
             Enum.take(compose_args, -3) == ["ps", "--format", "json"] do
          {"", 0}
        else
          {"unexpected Compose command", 97}
        end
    end
  end

  defp control_image_inspection do
    %{
      "Id" => @control_image_id,
      "RepoDigests" => [],
      "Architecture" => "amd64",
      "Os" => "linux",
      "Config" => %{
        "User" => "10001:10001",
        "Labels" => %{
          "org.opencontainers.image.version" => Favn.RunnerRelease.current_favn_version(),
          "io.favn.control-plane.build-id" => @control_build_id,
          "io.favn.manifest-schema-version" =>
            Favn.Manifest.Compatibility.current_schema_version() |> Integer.to_string(),
          "io.favn.runner-contract-version" =>
            Favn.Manifest.Compatibility.current_runner_contract_version()
            |> Integer.to_string(),
          "io.favn.target" => "linux/amd64"
        }
      }
    }
  end
end
