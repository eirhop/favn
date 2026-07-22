defmodule Favn.DeploymentModeTest do
  use ExUnit.Case, async: true

  alias Favn.DeploymentMode

  test "production is default and local development is explicit" do
    assert {:ok, :production} = DeploymentMode.from_env(%{})
    assert {:ok, :production} = DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "production"})

    assert {:ok, :local_development} =
             DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "local-development"})

    assert {:error, {:invalid_env, "FAVN_DEPLOYMENT_MODE", "production|local-development"}} =
             DeploymentMode.from_env(%{"FAVN_DEPLOYMENT_MODE" => "dev"})
  end
end
