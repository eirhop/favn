defmodule FavnRunner.ProductionRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnRunner.ProductionRuntimeConfig

  @cookie "bN7!tQ2#vL9@xR4$kM8%pC6&zH3*eW5?"

  test "validate/1 requires and accepts the distributed runner contract" do
    assert {:ok, config} = ProductionRuntimeConfig.validate(base_env())

    assert config == %{
             topology: :beam_node,
             runner_node: "runner@runner.internal",
             expected_control_plane_node: "control@control-plane.internal",
             distribution_port: 9_100,
             epmd_port: 4_369,
             cookie_configured?: true
           }

    diagnostics = ProductionRuntimeConfig.diagnostics(config)
    assert diagnostics.runner.runner_node == "runner@runner.internal"
    assert diagnostics.runner.cookie_configured?
    refute inspect(diagnostics) =~ @cookie
  end

  test "Mix startup may omit production node config while partial config fails closed" do
    assert :ok = ProductionRuntimeConfig.apply_from_env_if_configured(%{})

    assert {:error, %{status: :invalid, error: {:missing_env, "FAVN_CONTROL_PLANE_NODE"}}} =
             ProductionRuntimeConfig.apply_from_env_if_configured(%{
               "FAVN_RUNNER_NODE" => "runner@runner.internal"
             })
  end

  test "rejects loopback, short, and equal node names" do
    assert {:error, %{status: :invalid, error: {:invalid_env, "FAVN_RUNNER_NODE", expected}}} =
             base_env()
             |> Map.put("FAVN_RUNNER_NODE", "runner")
             |> ProductionRuntimeConfig.validate()

    assert expected == "long name@private-dns-name"

    assert {:error, %{status: :invalid, error: {:invalid_env, "FAVN_RUNNER_NODE", ^expected}}} =
             base_env()
             |> Map.put("FAVN_RUNNER_NODE", "runner@localhost")
             |> ProductionRuntimeConfig.validate()

    assert {:error,
            %{
              status: :invalid,
              error: {:invalid_env, "FAVN_CONTROL_PLANE_NODE", "different from runner node"}
            }} =
             base_env()
             |> Map.put("FAVN_CONTROL_PLANE_NODE", "runner@runner.internal")
             |> ProductionRuntimeConfig.validate()
  end

  test "rejects weak cookies and invalid fixed ports without echoing values" do
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
              error: {:invalid_env, "FAVN_BEAM_DISTRIBUTION_PORT", "1..65535"}
            }} =
             base_env()
             |> Map.put("FAVN_BEAM_DISTRIBUTION_PORT", "0")
             |> ProductionRuntimeConfig.validate()
  end

  test "FAVN_RUNNER_MODE is not a production configuration surface" do
    assert {:ok, config} =
             base_env()
             |> Map.put("FAVN_RUNNER_MODE", "anything")
             |> ProductionRuntimeConfig.validate()

    refute Map.has_key?(config, :mode)
  end

  defp base_env do
    %{
      "FAVN_RUNNER_NODE" => "runner@runner.internal",
      "FAVN_CONTROL_PLANE_NODE" => "control@control-plane.internal",
      "FAVN_DISTRIBUTION_COOKIE" => @cookie,
      "FAVN_BEAM_DISTRIBUTION_PORT" => "9100"
    }
  end
end
