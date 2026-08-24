defmodule FavnOrchestrator.Auth.ManifestDeployerTokensTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Auth.ManifestDeployerTokens

  @token "a9f4c83e72b641d59e0387acb251640d"

  test "parses a versioned identity with an exact workspace allowlist" do
    raw =
      Jason.encode!([
        %{
          "service_identity" => "data-ci-v2",
          "workspace_ids" => ["salmon", "trout"],
          "token" => @token
        }
      ])

    assert {:ok, [config]} = ManifestDeployerTokens.from_env_string(raw)
    assert config.service_identity == "data-ci-v2"
    assert config.workspace_ids == MapSet.new(["salmon", "trout"])
    refute inspect(config) =~ @token

    assert {:ok, "data-ci-v2"} =
             ManifestDeployerTokens.authenticate(@token, "salmon", [config])

    assert {:error, :workspace_forbidden} =
             ManifestDeployerTokens.authenticate(@token, "cod", [config])

    assert {:error, :service_unauthorized} =
             ManifestDeployerTokens.authenticate("wrong-token", "salmon", [config])
  end

  test "rejects unversioned identities, unknown keys, and duplicate credentials" do
    assert {:error, {:invalid_manifest_deployer_tokens, :invalid_credential}} =
             ManifestDeployerTokens.from_env_string(
               Jason.encode!([
                 %{
                   "service_identity" => "data-ci",
                   "workspace_ids" => ["salmon"],
                   "token" => @token
                 }
               ])
             )

    assert {:error, {:invalid_manifest_deployer_tokens, :invalid_credential}} =
             ManifestDeployerTokens.from_env_string(
               Jason.encode!([
                 %{
                   "service_identity" => "data-ci-v1",
                   "workspace_ids" => ["salmon"],
                   "token" => @token,
                   "extra" => true
                 }
               ])
             )

    credential = %{
      "service_identity" => "data-ci-v1",
      "workspace_ids" => ["salmon"],
      "token" => @token
    }

    assert {:error, {:invalid_manifest_deployer_tokens, :duplicate_credential}} =
             ManifestDeployerTokens.from_env_string(Jason.encode!([credential, credential]))
  end
end
