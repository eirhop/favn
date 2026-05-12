defmodule Favn.Azure.PostgresEntraTokenTest do
  use ExUnit.Case, async: true

  alias Favn.Azure.PostgresEntraToken
  alias Favn.Azure.PostgresEntraToken.ManagedIdentity
  alias Favn.Azure.TokenError

  @resource "https://ossrdbms-aad.database.windows.net"

  test "Azure CLI provider parses accessToken and expiresOn" do
    system_cmd = fn "az", args, opts ->
      assert args == ["account", "get-access-token", "--resource", @resource, "--output", "json"]
      assert opts == [stderr_to_stdout: false]
      {Jason.encode!(%{accessToken: "cli-token", expiresOn: "2026-05-12 12:00:00.000000"}), 0}
    end

    assert {:ok, token} =
             PostgresEntraToken.fetch_token([provider: :azure_cli], system_cmd: system_cmd)

    assert token.access_token == "cli-token"
    assert token.expires_on == "2026-05-12 12:00:00.000000"
  end

  test "managed identity auto uses Azure App Service endpoint when environment is present" do
    env = fn
      "IDENTITY_ENDPOINT" -> "http://localhost/msi/token"
      "IDENTITY_HEADER" -> "identity-header"
    end

    http_client = fn :get, {url, headers}, _http_opts, _opts ->
      url = to_string(url)
      assert url =~ "http://localhost/msi/token?"
      assert url =~ "api-version=2019-08-01"
      assert url =~ "resource=https%3A%2F%2Fossrdbms-aad.database.windows.net"
      assert url =~ "client_id=client-1"
      assert {~c"X-IDENTITY-HEADER", ~c"identity-header"} in headers

      {:ok,
       {{~c"HTTP/1.1", 200, ~c"OK"}, [],
        Jason.encode!(%{access_token: "msi-token", expires_on: "1770000000"})}}
    end

    assert {:ok, token} =
             ManagedIdentity.fetch_token(
               [provider: :managed_identity, client_id: "client-1", endpoint: :auto],
               env: env,
               http_client: http_client
             )

    assert token.access_token == "msi-token"
    assert token.expires_on == "1770000000"
  end

  test "managed identity auto falls back to IMDS" do
    env = fn _name -> nil end

    http_client = fn :get, {url, headers}, _http_opts, _opts ->
      assert to_string(url) =~ "http://169.254.169.254/metadata/identity/oauth2/token?"
      assert to_string(url) =~ "api-version=2018-02-01"
      assert {~c"Metadata", ~c"true"} in headers

      {:ok,
       {{~c"HTTP/1.1", 200, ~c"OK"}, [],
        ~s({"access_token":"imds-token","expires_on":"1770000000"})}}
    end

    assert {:ok, token} =
             ManagedIdentity.fetch_token([provider: :managed_identity],
               env: env,
               http_client: http_client
             )

    assert token.access_token == "imds-token"
  end

  test "managed identity retries only transient failures" do
    parent = self()

    http_client = fn :get, _request, _http_opts, _opts ->
      send(parent, :request)

      case Process.get(:requests, 0) do
        0 ->
          Process.put(:requests, 1)
          {:ok, {{~c"HTTP/1.1", 429, ~c"Too Many Requests"}, [], ""}}

        _count ->
          {:ok,
           {{~c"HTTP/1.1", 200, ~c"OK"}, [],
            ~s({"access_token":"retry-token","expires_on":"1770000000"})}}
      end
    end

    sleeper = fn delay -> send(parent, {:sleep, delay}) end

    assert {:ok, token} =
             ManagedIdentity.fetch_token([provider: :managed_identity, endpoint: :imds],
               env: fn _ -> nil end,
               http_client: http_client,
               sleeper: sleeper
             )

    assert token.access_token == "retry-token"
    assert_received :request
    assert_received :request
    assert_received {:sleep, 100}
  end

  test "managed identity does not retry normal auth errors" do
    parent = self()

    http_client = fn :get, _request, _http_opts, _opts ->
      send(parent, :request)
      {:ok, {{~c"HTTP/1.1", 401, ~c"Unauthorized"}, [], ""}}
    end

    assert {:error, %TokenError{type: :authentication_error, retryable?: false}} =
             ManagedIdentity.fetch_token([provider: :managed_identity, endpoint: :imds],
               env: fn _ -> nil end,
               http_client: http_client,
               sleeper: fn _ -> flunk("should not sleep") end
             )

    assert_received :request
    refute_received :request
  end
end
