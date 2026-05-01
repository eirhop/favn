defmodule FavnRunner.ProductionRuntimeConfigTest do
  use ExUnit.Case, async: false

  alias FavnRunner.ProductionRuntimeConfig

  test "validate/1 accepts missing runner mode as local single-node" do
    assert {:ok, config} = ProductionRuntimeConfig.validate(%{})
    assert config == %{mode: :local, topology: :single_node}
  end

  test "validate/1 accepts explicit local runner mode" do
    assert {:ok, config} =
             ProductionRuntimeConfig.validate(%{"FAVN_RUNNER_MODE" => "local"})

    assert config.mode == :local
    assert config.topology == :single_node

    assert ProductionRuntimeConfig.diagnostics(config) == %{
             status: :ok,
             runner: %{mode: :local, topology: :single_node}
           }
  end

  test "validate/1 rejects unsupported distributed and remote runner modes" do
    for mode <- ["distributed", "remote"] do
      assert {:error, %{status: :invalid, error: {:invalid_env, "FAVN_RUNNER_MODE", "local"}}} =
               ProductionRuntimeConfig.validate(%{"FAVN_RUNNER_MODE" => mode})
    end
  end

  test "validate/1 rejects malformed runner modes without echoing values" do
    assert {:error, %{status: :invalid, error: {:invalid_env, "FAVN_RUNNER_MODE", "local"}}} =
             ProductionRuntimeConfig.validate(%{"FAVN_RUNNER_MODE" => "LOCAL"})
  end
end
