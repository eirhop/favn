defmodule FavnRunner.ProductionRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnRunner.ProductionRuntimeConfig

  @cookie "bN7!tQ2#vL9@xR4$kM8%pC6&zH3*eW5?"

  setup do
    credential =
      Path.join(System.tmp_dir!(), "favn-runner-tls-#{System.unique_integer()}.pem")

    options_file = credential <> ".config"
    File.write!(credential, "test credential")
    write_tls_options(options_file, credential)
    Process.put(:runner_tls_options_file, options_file)

    on_exit(fn ->
      File.rm(credential)
      File.rm(options_file)
    end)

    :ok
  end

  test "validate/1 requires and accepts the distributed runner contract" do
    assert {:ok, config} = ProductionRuntimeConfig.validate(base_env())

    assert config == %{
             topology: :beam_node,
             runner_node: "undefined@runner.internal",
             runner_node_host_alias: "runner.internal",
             expected_control_plane_node: "control@control-plane.internal",
             epmd_port: 4_369,
             transport: :tls,
             mutual_tls?: true,
             shutdown_drain_timeout_ms: 120_000,
             cookie_configured?: true
           }

    diagnostics = ProductionRuntimeConfig.diagnostics(config)
    assert diagnostics.runner.runner_node == "undefined@runner.internal"
    assert diagnostics.runner.cookie_configured?
    refute inspect(diagnostics) =~ @cookie
  end

  test "Mix startup may omit production node config while partial config fails closed" do
    assert :ok = ProductionRuntimeConfig.apply_from_env_if_configured(%{})

    assert {:error, %{status: :invalid, error: {:missing_env, "FAVN_CONTROL_PLANE_NODE"}}} =
             ProductionRuntimeConfig.apply_from_env_if_configured(%{
               "FAVN_RUNNER_NODE_HOST_ALIAS" => "runner.internal"
             })
  end

  test "rejects invalid or loopback runner aliases and invalid control-plane names" do
    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_env, "FAVN_RUNNER_NODE_HOST_ALIAS", expected}
            }} =
             base_env()
             |> Map.put("FAVN_RUNNER_NODE_HOST_ALIAS", "runner@internal")
             |> ProductionRuntimeConfig.validate()

    assert expected == "stable private DNS host alias"

    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_env, "FAVN_RUNNER_NODE_HOST_ALIAS", ^expected}
            }} =
             base_env()
             |> Map.put("FAVN_RUNNER_NODE_HOST_ALIAS", "localhost")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              status: :invalid,
              error:
                {:invalid_env, "FAVN_CONTROL_PLANE_NODE",
                 "long name@fully-qualified-private-dns-name"}
            }} =
             base_env()
             |> Map.put("FAVN_CONTROL_PLANE_NODE", "control@localhost")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              status: :invalid,
              error:
                {:invalid_env, "FAVN_CONTROL_PLANE_NODE",
                 "long name@fully-qualified-private-dns-name"}
            }} =
             base_env()
             |> Map.put("FAVN_CONTROL_PLANE_NODE", "control@short-host")
             |> ProductionRuntimeConfig.validate()

    assert {:error, %{status: :invalid}} =
             base_env()
             |> Map.put("FAVN_CONTROL_PLANE_NODE", "control@bad_host.internal")
             |> ProductionRuntimeConfig.validate()
  end

  test "rejects weak cookies and invalid EPMD ports without echoing values" do
    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :insufficient_entropy}
            }} =
             base_env()
             |> Map.put("FAVN_DISTRIBUTION_COOKIE", "weak")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_env, "ERL_EPMD_PORT", "1..65535"}
            }} =
             base_env()
             |> Map.put("ERL_EPMD_PORT", "0")
             |> ProductionRuntimeConfig.validate()
  end

  test "FAVN_RUNNER_MODE is not a production configuration surface" do
    assert {:ok, config} =
             base_env()
             |> Map.put("FAVN_RUNNER_MODE", "anything")
             |> ProductionRuntimeConfig.validate()

    refute Map.has_key?(config, :mode)
  end

  test "validates the bounded shutdown drain timeout" do
    assert {:ok, %{shutdown_drain_timeout_ms: 45_000}} =
             base_env()
             |> Map.put("FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS", "45000")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_env, "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS", "1000..3600000"}
            }} =
             base_env()
             |> Map.put("FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS", "999")
             |> ProductionRuntimeConfig.validate()
  end

  defp base_env do
    %{
      "FAVN_RUNNER_NODE_HOST_ALIAS" => "runner.internal",
      "FAVN_CONTROL_PLANE_NODE" => "control@control-plane.internal",
      "FAVN_DISTRIBUTION_COOKIE" => @cookie,
      "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE" => Process.get(:runner_tls_options_file)
    }
  end

  defp write_tls_options(path, credential) do
    options = [
      server: [
        certfile: String.to_charlist(credential),
        keyfile: String.to_charlist(credential),
        cacertfile: String.to_charlist(credential),
        verify: :verify_peer,
        fail_if_no_peer_cert: true
      ],
      client: [
        certfile: String.to_charlist(credential),
        keyfile: String.to_charlist(credential),
        cacertfile: String.to_charlist(credential),
        verify: :verify_peer
      ]
    ]

    File.write!(path, IO.iodata_to_binary(:io_lib.format("~p.~n", [options])))
  end
end
