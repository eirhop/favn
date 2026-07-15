defmodule Favn.RuntimeConfigDSLTest do
  use ExUnit.Case, async: false

  alias Favn.Assets.Compiler
  alias Favn.Manifest.Serializer
  alias Favn.RuntimeConfig.Ref

  defmodule Bundles do
    use Favn.RuntimeConfig

    bundle(:platform,
      environment: env!("APP_ENVIRONMENT"),
      landing_root: env!("LANDING_ROOT")
    )

    bundle(:github,
      url: env!("GITHUB_URL"),
      username: env!("GITHUB_USERNAME"),
      api_key: secret_env!("GITHUB_API_KEY"),
      enterprise_url: env!("GITHUB_ENTERPRISE_URL", required?: false)
    )
  end

  defmodule Landing do
    use Favn.Namespace,
      relation: [connection: :warehouse],
      runtime_config: [Bundles.platform()]
  end

  defmodule Landing.GitHub do
    use Favn.Namespace, runtime_config: [Bundles.github()]
  end

  defmodule Landing.GitHub.Repositories do
    use Favn.Asset

    runtime_config(:github, organization: env!("GITHUB_ORGANIZATION"))
    def asset(_ctx), do: :ok
  end

  defmodule Landing.Other do
    use Favn.Asset

    def asset(_ctx), do: :ok
  end

  defmodule Landing.GitHub.SQLSummary do
    use Favn.SQLAsset

    @materialized :view
    query do
      ~SQL"select 1 as value"
    end
  end

  defmodule Unrelated do
    use Favn.Asset

    runtime_config(Bundles.github())
    def asset(_ctx), do: :ok
  end

  defmodule GitHubResources do
    use Favn.MultiAsset

    runtime_config(Bundles.github())

    asset :repositories do
      rest do
        path("/repositories")
      end
    end

    asset :pull_requests do
      rest do
        path("/pull_requests")
      end
    end

    def asset(_ctx), do: :ok
  end

  defmodule Landing.GitHub.InheritedResources do
    use Favn.MultiAsset

    asset :issues do
      rest do
        path("/issues")
      end
    end

    def asset(_ctx), do: :ok
  end

  test "nested namespace bundles and direct declarations compose field by field" do
    assert {:ok, [asset]} = Compiler.compile_module_assets(Landing.GitHub.Repositories)

    assert asset.runtime_config == %{
             platform: %{
               environment: Ref.env!("APP_ENVIRONMENT"),
               landing_root: Ref.env!("LANDING_ROOT")
             },
             github: %{
               url: Ref.env!("GITHUB_URL"),
               username: Ref.env!("GITHUB_USERNAME"),
               api_key: Ref.secret_env!("GITHUB_API_KEY"),
               enterprise_url: Ref.env!("GITHUB_ENTERPRISE_URL", required?: false),
               organization: Ref.env!("GITHUB_ORGANIZATION")
             }
           }

    manifest_asset = Favn.Manifest.Asset.from_asset(asset)

    assert {:ok, encoded} =
             Serializer.encode_manifest(%{
               schema_version: 4,
               runner_contract_version: 4,
               assets: [manifest_asset]
             })

    assert encoded =~ ~s|"required?":false|
    assert encoded =~ ~s|"secret?":true|
    refute encoded =~ inspect(Bundles)
    refute encoded =~ "runtime_config_dsl_test.exs"
  end

  test "sibling namespaces do not receive unselected bundles" do
    assert {:ok, [asset]} = Compiler.compile_module_assets(Landing.Other)

    assert asset.runtime_config == %{
             platform: %{
               environment: Ref.env!("APP_ENVIRONMENT"),
               landing_root: Ref.env!("LANDING_ROOT")
             }
           }
  end

  test "unrelated assets can select the same bundle explicitly" do
    assert {:ok, [asset]} = Compiler.compile_module_assets(Unrelated)
    assert asset.runtime_config.github.api_key == Ref.secret_env!("GITHUB_API_KEY")
  end

  test "namespace runtime config is not injected into SQL assets" do
    assert {:ok, [asset]} = Compiler.compile_module_assets(Landing.GitHub.SQLSummary)
    assert asset.runtime_config == %{}
  end

  test "multi-assets flatten a selected bundle into every generated asset" do
    assert {:ok, assets} = Compiler.compile_module_assets(GitHubResources)
    assert Enum.map(assets, & &1.name) == [:repositories, :pull_requests]
    assert Enum.all?(assets, &(&1.runtime_config.github.url == Ref.env!("GITHUB_URL")))
  end

  test "multi-assets inherit namespace-selected bundles" do
    assert {:ok, [asset]} = Compiler.compile_module_assets(Landing.GitHub.InheritedResources)

    assert asset.runtime_config.platform.environment == Ref.env!("APP_ENVIRONMENT")
    assert asset.runtime_config.github.api_key == Ref.secret_env!("GITHUB_API_KEY")
  end

  test "conflicting inherited and direct declarations fail with safe provenance" do
    suffix = System.unique_integer([:positive])
    root = Module.concat(__MODULE__, "Conflict#{suffix}")
    asset = Module.concat(root, Asset)

    Code.compile_string("""
    defmodule #{inspect(root)} do
      use Favn.Namespace, runtime_config: [Favn.RuntimeConfigDSLTest.Bundles.github()]
    end
    """)

    assert_raise CompileError, ~r/conflicting runtime config :github.api_key/, fn ->
      Code.compile_string("""
      defmodule #{inspect(asset)} do
        use Favn.Asset

        runtime_config :github, api_key: secret_env!("OTHER_GITHUB_API_KEY")
        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "conflicting duplicate fields in one inline declaration fail at compile time" do
    module = Module.concat(__MODULE__, "DuplicateFields#{System.unique_integer([:positive])}")

    assert_raise ArgumentError, ~r/conflicting runtime config :github.api_key/, fn ->
      Code.compile_string("""
      defmodule #{inspect(module)} do
        use Favn.Asset

        runtime_config :github,
          api_key: secret_env!("GITHUB_API_KEY"),
          api_key: secret_env!("OTHER_GITHUB_API_KEY")

        def asset(_ctx), do: :ok
      end
      """)
    end
  end

  test "namespace invalid-input diagnostics do not inspect supplied values" do
    resolved_secret = "resolved-super-secret"

    error =
      assert_raise ArgumentError, fn ->
        Favn.Namespace.normalize_config!(runtime_config: resolved_secret)
      end

    refute error.message =~ resolved_secret
  end

  test "legacy source_config macro is removed" do
    refute {:source_config, 2} in Favn.Asset.__info__(:macros)
    refute {:source_config, 2} in Favn.MultiAsset.__info__(:macros)
  end

  test "resolved runtime config has no global getter" do
    refute function_exported?(Favn.RuntimeConfig, :get, 1)
  end
end
