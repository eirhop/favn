defmodule FavnOrchestrator.API.ExecutionPackagesRouterTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.SQL.Template
  alias FavnOrchestrator.API.Router
  alias FavnOrchestrator.Auth.ServiceTokens
  alias FavnOrchestrator.Storage.Adapter.Memory

  @opts Router.init([])
  @token "execution-package-test-token"

  setup do
    previous_tokens = Application.get_env(:favn_orchestrator, :api_service_tokens)

    Application.put_env(:favn_orchestrator, :api_service_tokens, [
      [
        service_identity: "execution_package_test",
        token_hash: ServiceTokens.hash_token(@token),
        enabled: true
      ]
    ])

    Memory.reset()

    on_exit(fn ->
      if is_nil(previous_tokens) do
        Application.delete_env(:favn_orchestrator, :api_service_tokens)
      else
        Application.put_env(:favn_orchestrator, :api_service_tokens, previous_tokens)
      end

      Memory.reset()
    end)

    :ok
  end

  test "uploads only missing immutable packages before publishing the compact index" do
    ref = {MyApp.Orders, :asset}
    package = execution_package(ref)
    version = manifest_version(ref, package.content_hash)

    assert missing_packages([package.content_hash]) == [package.content_hash]

    missing_index_response = publish_manifest(version)
    assert missing_index_response.status == 422

    assert %{
             "error" => %{
               "code" => "missing_execution_packages",
               "details" => %{"hashes" => [missing_hash]}
             }
           } = JSON.decode!(missing_index_response.resp_body)

    assert missing_hash == package.content_hash

    upload_response =
      gzip_post("/api/orchestrator/v1/execution-packages", %{
        packages: [canonical_json(package)]
      })

    assert upload_response.status == 201
    assert %{"data" => %{"stored" => 1}} = JSON.decode!(upload_response.resp_body)
    assert missing_packages([package.content_hash]) == []

    wrong_ref = {MyApp.Customers, :asset}
    wrong_version = manifest_version(wrong_ref, package.content_hash)
    wrong_response = publish_manifest(wrong_version)

    assert wrong_response.status == 422

    assert %{
             "error" => %{
               "code" => "execution_package_asset_mismatch",
               "details" => %{"hash" => wrong_hash}
             }
           } = JSON.decode!(wrong_response.resp_body)

    assert wrong_hash == package.content_hash

    published = publish_manifest(version)
    assert published.status == 201
    assert {:ok, stored} = FavnOrchestrator.get_manifest(version.manifest_version_id)
    assert stored.manifest == version.manifest
  end

  test "enforces the bounded package batch before decoding package payloads" do
    response =
      gzip_post("/api/orchestrator/v1/execution-packages", %{
        packages: List.duplicate(%{}, 101)
      })

    assert response.status == 422
    assert %{"error" => %{"code" => "validation_failed"}} = JSON.decode!(response.resp_body)
  end

  test "rejects non-canonical package wire payloads before storage" do
    package = execution_package({MyApp.Orders, :asset})
    raw = canonical_json(package)

    invalid_payloads = [
      put_in(raw, ["sql_execution", "template", "requires"], nil),
      Map.put(raw, "unknown", true)
    ]

    Enum.each(invalid_payloads, fn invalid ->
      response =
        gzip_post("/api/orchestrator/v1/execution-packages", %{packages: [invalid]})

      assert response.status == 422
      assert %{"error" => %{"code" => "validation_failed"}} = JSON.decode!(response.resp_body)
    end)

    assert missing_packages([package.content_hash]) == [package.content_hash]
  end

  test "missing-package response advertises effective publication limits" do
    previous = Application.get_env(:favn_orchestrator, :manifest_publication)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:favn_orchestrator, :manifest_publication)
      else
        Application.put_env(:favn_orchestrator, :manifest_publication, previous)
      end
    end)

    Application.put_env(:favn_orchestrator, :manifest_publication,
      compressed_limit_bytes: 4_096,
      decompressed_limit_bytes: 16_384
    )

    response = gzip_post("/api/orchestrator/v1/execution-packages/missing", %{hashes: []})

    assert response.status == 200

    assert %{
             "data" => %{
               "publication_limits" => %{
                 "max_packages" => 100,
                 "compressed_limit_bytes" => 4_096,
                 "decompressed_limit_bytes" => 16_384
               }
             }
           } = JSON.decode!(response.resp_body)
  end

  test "authenticates package routes before reading compressed input" do
    response =
      conn(:post, "/api/orchestrator/v1/execution-packages", "not-gzip")
      |> put_req_header("content-type", "application/json")
      |> put_req_header("content-encoding", "gzip")
      |> Router.call(@opts)

    assert response.status == 401
    assert %{"error" => %{"code" => "service_unauthorized"}} = JSON.decode!(response.resp_body)
  end

  defp execution_package(ref) do
    sql = "SELECT 1 AS id"

    template =
      Template.compile!(sql,
        file: "test/execution_packages_router_test.sql",
        line: 1,
        module: __MODULE__,
        scope: :query,
        enforce_query_root: true
      )

    {:ok, package} = ExecutionPackage.new(ref, %SQLExecution{sql: sql, template: template})
    package
  end

  defp manifest_version(ref, package_hash) do
    asset = %Asset{
      ref: ref,
      module: elem(ref, 0),
      name: elem(ref, 1),
      type: :sql,
      execution_package_hash: package_hash
    }

    {:ok, version} =
      Version.new(
        %Manifest{assets: [asset], graph: %Graph{nodes: [ref], topo_order: [ref]}},
        manifest_version_id: "mv_execution_package_router"
      )

    version
  end

  defp missing_packages(hashes) do
    response =
      gzip_post("/api/orchestrator/v1/execution-packages/missing", %{hashes: hashes})

    assert response.status == 200
    %{"data" => %{"missing" => missing}} = JSON.decode!(response.resp_body)
    missing
  end

  defp publish_manifest(version) do
    gzip_post("/api/orchestrator/v1/manifests", %{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      schema_version: version.schema_version,
      runner_contract_version: version.runner_contract_version,
      serialization_format: version.serialization_format,
      manifest: canonical_json(version.manifest)
    })
  end

  defp gzip_post(path, payload) do
    body = payload |> JSON.encode!() |> :zlib.gzip()

    conn(:post, path, body)
    |> put_req_header("authorization", "Bearer #{@token}")
    |> put_req_header("content-type", "application/json")
    |> put_req_header("content-encoding", "gzip")
    |> Router.call(@opts)
  end

  defp canonical_json(value) do
    value
    |> Serializer.encode_manifest!()
    |> JSON.decode!()
  end
end
