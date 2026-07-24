defmodule Favn.DeploymentModeTest do
  use ExUnit.Case, async: true

  alias Favn.DeploymentMode

  test "production is the only deployment mode" do
    assert {:ok, :production} = DeploymentMode.from_env(%{})
    assert {:ok, :production} = DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "production"})

    assert {:error, {:invalid_env, "FAVN_DEPLOYMENT_MODE", "production"}} =
             DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "local-development"})

    assert {:error, {:invalid_env, "FAVN_DEPLOYMENT_MODE", "production"}} =
             DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "dev"})
  end
end
