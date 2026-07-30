defmodule FavnView.ControlPlaneRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.ControlPlaneRuntimeConfig

  @token "cP7!mQ2#vL9@xR4$kM8%pC6&zH3*eW5?"
  @pin_key :binary.copy(<<4>>, 32) |> Base.encode64()
  @secret_key_base String.duplicate("v", 64)

  setup do
    directory =
      Path.join(
        System.tmp_dir!(),
        "favn-control-plane-runtime-config-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    credential = Path.join(directory, "credential.pem")
    options_file = Path.join(directory, "ssl_dist.config")
    File.write!(credential, "test")

    options = [
      server: tls_peer_options(credential, fail_if_no_peer_cert: true),
      client: tls_peer_options(credential)
    ]

    File.write!(options_file, IO.iodata_to_binary(:io_lib.format("~p.~n", [options])))
    on_exit(fn -> File.rm_rf(directory) end)

    %{tls_options_file: options_file}
  end

  test "validates both same-BEAM control-plane applications from one environment", context do
    assert {:ok, config} =
             ControlPlaneRuntimeConfig.validate(base_env(context.tls_options_file))

    assert config.orchestrator.postgres[:ssl_mode] == :verify_full

    assert config.orchestrator.runner.control_plane_node ==
             "control@control-plane.internal"

    assert config.view.public_origin == "https://favn.example.com"
    assert config.view.bind_ip == {0, 0, 0, 0}
    assert config.view.forwarded_for_policy == :replace
    assert config.view.http_server == config.orchestrator.http_server

    diagnostics = %{
      orchestrator: FavnOrchestrator.ProductionRuntimeConfig.diagnostics(config.orchestrator),
      view: FavnView.ProductionRuntimeConfig.diagnostics(config.view)
    }

    refute inspect(diagnostics) =~ @token
    refute inspect(diagnostics) =~ @pin_key
    refute inspect(diagnostics) =~ @secret_key_base
    refute inspect(diagnostics) =~ "database-password"
  end

  test "reports bounded component errors without applying a partial config", context do
    invalid =
      base_env(context.tls_options_file)
      |> Map.delete("FAVN_DATABASE_URL")
      |> Map.delete("FAVN_VIEW_SECRET_KEY_BASE")

    assert {:error, %{status: :invalid, errors: errors}} =
             ControlPlaneRuntimeConfig.validate(invalid)

    assert errors.orchestrator.error == {:missing_env, "FAVN_DATABASE_URL"}
    assert errors.view.error == {:missing_env, "FAVN_VIEW_SECRET_KEY_BASE"}
  end

  defp base_env(tls_options_file) do
    %{
      "FAVN_DATABASE_URL" => "ecto://runtime:database-password@postgres.internal.example/favn",
      "FAVN_DATABASE_SSL_MODE" => "verify-full",
      "FAVN_RUNTIME_INPUT_PIN_KEYS" => Jason.encode!(%{"1" => @pin_key}),
      "FAVN_WORKSPACE_IDS" => "workspace-one",
      "FAVN_ORCHESTRATOR_API_SERVICE_TOKENS" => "view-v1:#{@token}",
      "FAVN_CONTROL_PLANE_NODE" => "control@control-plane.internal",
      "FAVN_DISTRIBUTION_COOKIE" => "bN7!tQ2#vL9@xR4$kM8%pC6&zH3*eW5?",
      "FAVN_BEAM_DISTRIBUTION_PORT" => "9100",
      "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE" => tls_options_file,
      "FAVN_VIEW_PUBLIC_ORIGIN" => "https://favn.example.com",
      "FAVN_VIEW_SECRET_KEY_BASE" => @secret_key_base,
      "FAVN_VIEW_TRUSTED_PROXY_CIDRS" => "10.42.0.5/32",
      "FAVN_VIEW_FORWARDED_FOR_POLICY" => "replace"
    }
  end

  defp tls_peer_options(credential, extra \\ []) do
    [
      certfile: String.to_charlist(credential),
      keyfile: String.to_charlist(credential),
      cacertfile: String.to_charlist(credential),
      verify: :verify_peer
    ]
    |> Keyword.merge(extra)
  end
end
