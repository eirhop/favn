defmodule Favn.CoverageDSLTest do
  use ExUnit.Case, async: false

  alias Favn.Coverage.{Effective, Spec}

  setup do
    previous_timezone = Application.get_env(:favn, :default_timezone)
    previous_scope = Application.get_env(:favn, :coverage_scope)

    on_exit(fn ->
      restore_env(:default_timezone, previous_timezone)
      restore_env(:coverage_scope, previous_scope)
    end)

    :ok
  end

  test "coverage is accepted by single, SQL, and multi-asset DSLs" do
    single = unique_module("Single")
    sql = unique_module("SQL")
    multi = unique_module("Multi")

    compile_module!(single, """
    defmodule #{inspect(single)} do
      use Favn.Asset
      window Favn.Window.daily()
      coverage from: ~D[2020-01-01], availability_delay: {:hours, 6}
      def asset(_ctx), do: :ok
    end
    """)

    compile_module!(sql, """
    defmodule #{inspect(sql)} do
      use Favn.SQLAsset
      relation connection: :warehouse
      window Favn.Window.daily()
      coverage from: ~D[2021-01-01]
      materialized :table
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    compile_module!(multi, """
    defmodule #{inspect(multi)} do
      use Favn.MultiAsset
      window Favn.Window.daily()
      coverage from: ~D[2022-01-01]
      asset :first do
      end
      asset :second do
        coverage from: ~D[2023-01-01]
      end
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{coverage_spec: %Spec{from: ~D[2020-01-01]}}] =
             single.__favn_assets__()

    assert %{asset: %Favn.Asset{coverage_spec: %Spec{from: ~D[2021-01-01]}}} =
             sql.__favn_sql_asset_definition__()

    assert [
             %Favn.Asset{coverage_spec: %Spec{from: ~D[2022-01-01]}},
             %Favn.Asset{coverage_spec: %Spec{from: ~D[2023-01-01]}}
           ] = multi.__favn_assets__()
  end

  test "namespace coverage uses closest replacement and explicit nil clearing" do
    root = unique_module("NamespaceRoot")
    nested = Module.concat(root, Nested)
    inherited = Module.concat(nested, Inherited)
    cleared = Module.concat(nested, Cleared)

    compile_module!(root, """
    defmodule #{inspect(root)} do
      use Favn.Namespace
      window Favn.Window.daily(timezone: "Europe/Oslo")
      coverage from: ~D[2020-01-01], availability_delay: {:hours, 6}
    end
    """)

    compile_module!(nested, """
    defmodule #{inspect(nested)} do
      use Favn.Namespace
      coverage from: ~D[2022-01-01]
    end
    """)

    compile_module!(inherited, """
    defmodule #{inspect(inherited)} do
      use Favn.Asset
      def asset(_ctx), do: :ok
    end
    """)

    compile_module!(cleared, """
    defmodule #{inspect(cleared)} do
      use Favn.Asset
      coverage nil
      def asset(_ctx), do: :ok
    end
    """)

    assert [%Favn.Asset{coverage_spec: %Spec{} = coverage, window_spec: window}] =
             inherited.__favn_assets__()

    assert coverage.from == ~D[2022-01-01]
    assert coverage.availability_delay_seconds == 0
    assert window.timezone_source == :namespace
    assert [%Favn.Asset{coverage_spec: nil}] = cleared.__favn_assets__()
  end

  test "coverage on a non-windowed leaf is a compile error" do
    module = unique_module("NoWindow")

    assert_raise CompileError, ~r/coverage requires an effective asset window/, fn ->
      compile_module!(module, """
      defmodule #{inspect(module)} do
        use Favn.Asset
        coverage from: ~D[2020-01-01]
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "manifest construction freezes environment timezone, scope, and provenance" do
    module = unique_module("ManifestAsset")

    compile_module!(module, """
    defmodule #{inspect(module)} do
      use Favn.Asset
      window Favn.Window.monthly()
      coverage from: ~D[2020-01-01]
      freshness :daily
      def asset(_ctx), do: :ok
    end
    """)

    Application.put_env(:favn, :default_timezone, "Europe/Oslo")
    Application.put_env(:favn, :coverage_scope, from: "2026-07-01")

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [module],
               pipeline_modules: [],
               schedule_modules: [],
               connection_modules: [],
               runner_release_id: FavnTestSupport.runner_release_id()
             )

    assert [asset] = manifest.assets
    assert asset.window.timezone == "Europe/Oslo"
    assert asset.window.timezone_source == :application_default
    assert asset.freshness.timezone == "Europe/Oslo"
    assert asset.freshness.timezone_source == :application_default
    assert %Effective{scope_source: :environment_floor} = asset.coverage
    assert asset.coverage.declared_from.start_at.year == 2020
    assert asset.coverage.effective_from.start_at.year == 2026
    assert manifest.schema_version == 12
    assert manifest.runner_contract_version == 12

    assert manifest.metadata.environment == %{
             default_timezone: "Europe/Oslo",
             default_timezone_source: :application_default,
             coverage_scope: %{from: "2026-07-01"}
           }
  end

  test "manifest SQL tables receive a desired target descriptor from connection definitions" do
    connection = unique_module("Connection")
    asset_module = unique_module("Target")
    adapter = unique_module("Adapter")

    compile_module!(connection, """
    defmodule #{inspect(connection)} do
      @behaviour Favn.Connection
      def definition do
        %Favn.Connection.Definition{
          name: :warehouse,
          adapter: #{inspect(adapter)},
          config_schema: []
        }
      end
    end
    """)

    compile_module!(asset_module, """
    defmodule #{inspect(asset_module)} do
      use Favn.SQLAsset
      relation connection: :warehouse, schema: "mart", name: "target"
      window Favn.Window.daily()
      coverage from: ~D[2026-01-01]
      materialized :table
      query do
        ~SQL"select 1 as id"
      end
    end
    """)

    assert {:ok, manifest} =
             FavnAuthoring.generate_manifest(
               asset_modules: [asset_module],
               pipeline_modules: [],
               schedule_modules: [],
               connection_modules: [connection],
               runner_release_id: FavnTestSupport.runner_release_id()
             )

    assert [asset] = manifest.assets
    assert asset.semantic_generation_id == nil
    assert asset.target_descriptor.adapter == Atom.to_string(adapter)

    assert asset.target_descriptor.connection_identity.definition_module ==
             Atom.to_string(connection)

    assert asset.target_descriptor.window_identity == %{kind: "day", timezone: "Etc/UTC"}
    assert byte_size(asset.target_descriptor.descriptor_hash) == 64

    assert {:ok, encoded} = Favn.Manifest.Serializer.encode_manifest(manifest)
    assert {:ok, decoded} = Favn.Manifest.Serializer.decode_manifest(encoded)
    assert {:ok, round_tripped} = Favn.Manifest.Rehydrate.manifest(decoded)
    assert [rehydrated] = round_tripped.assets
    assert %Favn.Coverage.Effective{} = rehydrated.coverage
    assert %Favn.Manifest.TargetDescriptor{} = rehydrated.target_descriptor
    assert rehydrated.target_descriptor.descriptor_hash == asset.target_descriptor.descriptor_hash
  end

  defp compile_module!(module, source) do
    assert [{^module, _binary}] = Code.compile_string(source, "test/coverage_dsl_test.exs")
  end

  defp unique_module(prefix) do
    Module.concat(__MODULE__, "#{prefix}#{System.unique_integer([:positive])}")
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn, key)
  defp restore_env(key, value), do: Application.put_env(:favn, key, value)
end
