defmodule FavnOrchestrator.API.ManifestsRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Manifest.Serializer
  alias Favn.Manifest.Version
  alias FavnOrchestrator.API.ManifestsRouter
  alias FavnOrchestrator.Auth.ServiceTokens

  @token "manifest-router-test-token-with-32-bytes"

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "manifest_router_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true
      ]
    ])

    on_exit(fn -> restore_env(:api_service_tokens, previous_tokens) end)

    :ok
  end

  test "accepts a publication envelope whose runner release matches its manifest" do
    params = valid_envelope()

    assert {:ok, version} = ManifestsRouter.build_version(params)
    assert version.required_runner_release_id == FavnTestSupport.runner_release_id()
  end

  test "returns stable validation errors for missing, malformed, and mismatched release ids" do
    valid = valid_envelope()

    cases = [
      {Map.delete(valid, "required_runner_release_id"), "Invalid required runner release id"},
      {Map.put(valid, "required_runner_release_id", "rr_INVALID"),
       "Invalid required runner release id"},
      {Map.put(
         valid,
         "required_runner_release_id",
         FavnTestSupport.runner_release_id(:alternate)
       ), "Manifest runner release id does not match payload"}
    ]

    for {params, expected_message} <- cases do
      response = request(params)

      assert response.status == 422

      assert %{
               "error" => %{
                 "code" => "validation_failed",
                 "message" => ^expected_message,
                 "status" => 422
               }
             } = Jason.decode!(response.resp_body)
    end
  end

  defp valid_envelope do
    manifest =
      FavnTestSupport.with_manifest_contract(%{
        assets: [],
        pipelines: [],
        schedules: [],
        graph: %{},
        metadata: %{}
      })

    {:ok, version} = Version.new(manifest, manifest_version_id: "mv_router_release_binding")
    {:ok, encoded_manifest} = Serializer.encode_manifest(version.manifest)
    {:ok, decoded_manifest} = Jason.decode(encoded_manifest)

    %{
      "manifest_version_id" => version.manifest_version_id,
      "content_hash" => version.content_hash,
      "schema_version" => version.schema_version,
      "runner_contract_version" => version.runner_contract_version,
      "required_runner_release_id" => version.required_runner_release_id,
      "serialization_format" => version.serialization_format,
      "manifest" => decoded_manifest
    }
  end

  defp request(params) do
    :post
    |> conn("/", "")
    |> put_req_header("authorization", "Bearer #{@token}")
    |> Map.put(:body_params, params)
    |> ManifestsRouter.call(ManifestsRouter.init([]))
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
