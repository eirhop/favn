Code.require_file(Path.join(:code.priv_dir(:favn_test_support), "fixtures/catalog.exs"))

defmodule Favn.CatalogPublishTest do
  use ExUnit.Case, async: false
  alias FavnTestSupport.CatalogFixture

  test "invalid inputs and target expectations fail without opening a connection" do
    assert {:error, %{"reason" => "invalid_timeout"}} =
             Favn.Catalog.publish([timeout_ms: 900_001], [])

    assert {:error, %{"reason" => "unknown_catalog_target"}} =
             Favn.Catalog.publish([target: "missing"], [])

    assert {:error, %{"reason" => "invalid_publication_request"}} =
             Favn.Catalog.publish([target: "analytics"],
               catalog_targets: [
                 analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]
               ]
             )
  end

  test "rebuild validates its own options without artifact inputs" do
    assert {:error, %{"reason" => "invalid_rebuild_request"}} =
             Favn.Catalog.rebuild([manifest: "file"], [])

    assert {:error, %{"reason" => "invalid_timeout"}} =
             Favn.Catalog.rebuild([timeout_ms: 900_001], [])

    assert {:error, %{"reason" => "unknown_catalog_target"}} =
             Favn.Catalog.rebuild([target: "missing"], [])
  end

  test "overall deadline preserves operation identity for recovery" do
    Code.require_file("fixtures/catalog/providers.ex", __DIR__)
    root = Path.join(System.tmp_dir!(), "catalog-timeout-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, path} = Favn.Catalog.Artifact.write(CatalogFixture.manifest(), root)

    config = [
      catalog_targets: [analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]],
      connection_modules: [warehouse: CatalogTest.Selected],
      connections: [warehouse: [value: "slow"]]
    ]

    started = System.monotonic_time(:millisecond)

    assert {:error, result} =
             Favn.Catalog.publish(
               [manifest: path, target: "analytics", expect_manifest: "none:0", timeout_ms: 500],
               config
             )

    assert System.monotonic_time(:millisecond) - started < 2500
    assert result["operation_id"] =~ "cp_"
    assert result["reason"] == "publication_outcome_unknown"
    assert {:error, result} = Favn.Catalog.rebuild([target: "analytics", timeout_ms: 500], config)
    assert result["operation_id"] =~ "cr_"
    assert result["reason"] == "rebuild_outcome_unknown"
  end

  @tag :acceptance
  test "precompiled fresh Mix command skips runtime config, application boot and unrelated providers" do
    root = Path.join(System.tmp_dir!(), "catalog-cli-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "config"))
    on_exit(fn -> File.rm_rf!(root) end)
    {:ok, artifact} = Favn.Catalog.Artifact.write(CatalogFixture.full_manifest(), root)

    File.write!(Path.join(root, "mix.exs"), """
    defmodule CatalogTest.Project do
      use Mix.Project
      def project, do: [app: :catalog_test, version: "0.1.0", deps: []]
      def application, do: [mod: {CatalogTest.Trap, []}]
    end
    """)

    File.write!(Path.join(root, "config/runtime.exs"), "raise \"runtime config was evaluated\"")

    File.write!(Path.join(root, "config/catalog_publish.exs"), """
    import Config
    config :favn,
      catalog_targets: [analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]],
      connection_modules: [warehouse: CatalogTest.Selected, unrelated: CatalogTest.Unrelated],
      connections: [warehouse: [value: "only-selected"], unrelated: [value: Favn.RuntimeConfig.Ref.env!("MISSING_CATALOG_TEST_SECRET")]]
    """)

    fixture = Path.expand("fixtures/catalog/providers.ex", __DIR__)
    ebin = Path.join(root, "ebin")
    File.mkdir_p!(ebin)

    build = :code.lib_dir(:favn_core) |> to_string() |> Path.dirname()
    paths = Path.wildcard(Path.join(build, "*/ebin"))

    erl = Enum.map_join([ebin | paths], " ", &("-pa " <> &1))

    assert {_, 0} =
             System.cmd(System.find_executable("elixirc"), ["-o", ebin, fixture],
               env: [{"ELIXIR_ERL_OPTIONS", erl}],
               stderr_to_stdout: true
             )

    args = [
      "favn.catalog.publish",
      "--manifest",
      artifact,
      "--target",
      "analytics",
      "--expect-manifest",
      "none:0"
    ]

    {output, code} =
      System.cmd(System.find_executable("mix"), args,
        cd: root,
        env: [{"ELIXIR_ERL_OPTIONS", erl}, {"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ ~s("outcome":"committed")
    refute output =~ "unrelated connection"
    refute File.exists?(Path.join(root, "_build"))

    {output, code} =
      System.cmd(System.find_executable("mix"), args ++ ["--reconcile"],
        cd: root,
        env: [{"ELIXIR_ERL_OPTIONS", erl}, {"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ ~s("outcome":"replayed")

    {output, code} =
      System.cmd(
        System.find_executable("mix"),
        ["favn.catalog.rebuild", "--target", "analytics"],
        cd: root,
        env: [{"ELIXIR_ERL_OPTIONS", erl}, {"MIX_ENV", "test"}],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ ~s("outcome":"rebuilt")
    refute File.exists?(Path.join(root, "_build"))

    # A fresh VM must stop only applications this timed-out invocation started.
    script = Path.join(root, "timeout.exs")

    File.write!(script, """
    Code.ensure_loaded!(Favn.Catalog)
    started = fn -> MapSet.new(Application.started_applications(), &elem(&1, 0)) end
    before = started.()
    config = [catalog_targets: [analytics: [connection: :warehouse, catalog: "mart", schema: "meta"]],
      connection_modules: [warehouse: CatalogTest.Selected], connections: [warehouse: [value: "slow"]]]
    {:error, result} = Favn.Catalog.publish([manifest: #{inspect(artifact)}, target: "analytics",
      expect_manifest: "none:0", timeout_ms: 500], config)
    true = result["reason"] == "publication_outcome_unknown"
    true = String.starts_with?(result["operation_id"], "cp_")
    true = started.() == before
    IO.puts("timeout cleanup passed")
    """)

    {output, code} =
      System.cmd(System.find_executable("elixir"), [script],
        env: [{"ELIXIR_ERL_OPTIONS", erl}],
        stderr_to_stdout: true
      )

    assert code == 0, output
    assert output =~ "timeout cleanup passed"
  end
end
