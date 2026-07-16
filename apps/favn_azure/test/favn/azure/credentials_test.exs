defmodule Favn.Azure.CredentialsTest do
  use ExUnit.Case, async: true

  alias Favn.Azure.Credentials
  alias Favn.Azure.Credentials.{Cache, Request}
  alias Favn.Azure.Credentials.Supervisor, as: CredentialsSupervisor
  alias Favn.Azure.{PostgresEntraToken, Token, TokenError}

  @postgres_resource "https://ossrdbms-aad.database.windows.net"

  defmodule TestProvider do
    @behaviour Favn.Azure.CredentialProvider

    @impl true
    def fetch_token(_request, opts) do
      agent = Keyword.fetch!(opts, :responses)

      response =
        Agent.get_and_update(agent, fn [response | rest] -> {response, rest} end)

      case response do
        {:block, owner, token} ->
          send(owner, {:provider_fetch, self()})

          receive do
            :release -> {:ok, token}
          end

        response ->
          response
      end
    end
  end

  defmodule KillingProvider do
    @behaviour Favn.Azure.CredentialProvider

    @impl true
    def fetch_token(_request, _opts), do: Process.exit(self(), :kill)
  end

  test "Azure CLI uses the requested resource and normalizes expiry" do
    system_cmd = fn "az", args, opts ->
      assert args == [
               "account",
               "get-access-token",
               "--resource",
               @postgres_resource,
               "--output",
               "json"
             ]

      assert opts == [stderr_to_stdout: false]

      {Jason.encode!(%{accessToken: "cli-token", expires_on: "1893456000"}), 0}
    end

    assert {:ok, %Token{} = token} =
             Credentials.fetch_token(@postgres_resource,
               provider: :azure_cli,
               cache: false,
               provider_options: [system_cmd: system_cmd]
             )

    assert token.access_token == "cli-token"
    assert token.expires_at == DateTime.from_unix!(1_893_456_000)
    refute inspect(token) =~ "cli-token"
  end

  test "Azure CLI accepts a timezone-qualified legacy expiresOn timestamp" do
    system_cmd = fn _command, _args, _opts ->
      {Jason.encode!(%{accessToken: "cli-token", expiresOn: "2030-05-12 12:00:00.000000Z"}),
       0}
    end

    assert {:ok, token} =
             Credentials.fetch_token(@postgres_resource,
               provider: :azure_cli,
               cache: false,
               provider_options: [system_cmd: system_cmd]
             )

    assert token.expires_at == ~U[2030-05-12 12:00:00.000000Z]
  end

  test "Azure CLI rejects an ambiguous timezone-less expiresOn timestamp" do
    system_cmd = fn _command, _args, _opts ->
      {Jason.encode!(%{accessToken: "cli-token", expiresOn: "2030-05-12 12:00:00.000000"}),
       0}
    end

    assert {:error, %TokenError{type: :execution_error}} =
             Credentials.fetch_token(@postgres_resource,
               provider: :azure_cli,
               cache: false,
               provider_options: [system_cmd: system_cmd]
             )
  end

  test "managed identity auto uses Azure App Service when environment is present" do
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
        Jason.encode!(%{access_token: "msi-token", expires_on: "1893456000"})}}
    end

    assert {:ok, token} =
             Credentials.fetch_token(@postgres_resource,
               provider: :managed_identity,
               client_id: "client-1",
               endpoint: :auto,
               cache: false,
               provider_options: [env: env, http_client: http_client]
             )

    assert token.access_token == "msi-token"
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
            ~s({"access_token":"retry-token","expires_on":"1893456000"})}}
      end
    end

    sleeper = fn delay -> send(parent, {:sleep, delay}) end

    assert {:ok, token} =
             Credentials.fetch_token(@postgres_resource,
               provider: :managed_identity,
               endpoint: :imds,
               cache: false,
               provider_options: [
                 env: fn _ -> nil end,
                 http_client: http_client,
                 sleeper: sleeper
               ]
             )

    assert token.access_token == "retry-token"
    assert_received :request
    assert_received :request
    assert_received {:sleep, 100}
  end

  test "concurrent cache misses share one provider fetch" do
    {cache, responses} = start_cache([{:block, self(), token("shared", 3_600)}])
    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    first =
      Task.async(fn ->
        Credentials.fetch_token(request,
          cache: cache,
          provider_options: [responses: responses]
        )
      end)

    assert_receive {:provider_fetch, provider_pid}

    second =
      Task.async(fn ->
        Credentials.fetch_token(request,
          cache: cache,
          provider_options: [responses: responses]
        )
      end)

    refute_receive {:provider_fetch, _other_pid}, 50
    send(provider_pid, :release)

    assert {:ok, %Token{access_token: "shared"}} = Task.await(first)
    assert {:ok, %Token{access_token: "shared"}} = Task.await(second)
  end

  test "cache entries are isolated by provider options" do
    {cache, first_responses} = start_cache([{:ok, token("first", 3_600)}])
    second_responses = start_response_agent([{:ok, token("second", 3_600)}])
    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    assert {:ok, %Token{access_token: "first"}} =
             Credentials.fetch_token(request,
               cache: cache,
               provider_options: [responses: first_responses]
             )

    assert {:ok, %Token{access_token: "second"}} =
             Credentials.fetch_token(request,
               cache: cache,
               provider_options: [responses: second_responses]
             )
  end

  test "concurrent calls with different provider options do not share a fetch" do
    {cache, first_responses} = start_cache([{:block, self(), token("first", 3_600)}])
    owner = self()

    second_responses = start_response_agent([{:block, owner, token("second", 3_600)}])

    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    first =
      Task.async(fn ->
        Credentials.fetch_token(request,
          cache: cache,
          provider_options: [responses: first_responses]
        )
      end)

    second =
      Task.async(fn ->
        Credentials.fetch_token(request,
          cache: cache,
          provider_options: [responses: second_responses]
        )
      end)

    assert_receive {:provider_fetch, first_pid}
    assert_receive {:provider_fetch, second_pid}
    assert first_pid != second_pid
    send(first_pid, :release)
    send(second_pid, :release)

    assert {:ok, %Token{access_token: first_token}} = Task.await(first)
    assert {:ok, %Token{access_token: second_token}} = Task.await(second)
    assert MapSet.new([first_token, second_token]) == MapSet.new(["first", "second"])
  end

  test "cache rejects unique-key overload without starting unbounded work" do
    {cache, responses} =
      start_cache_with_opts([{:block, self(), token("first", 3_600)}], max_inflight: 1)

    first_request = Request.new!("https://vault.azure.net/one", provider: TestProvider)
    second_request = Request.new!("https://vault.azure.net/two", provider: TestProvider)
    provider_options = [responses: responses]

    first =
      Task.async(fn ->
        Credentials.fetch_token(first_request,
          cache: cache,
          provider_options: provider_options
        )
      end)

    assert_receive {:provider_fetch, provider_pid}

    assert {:error, %TokenError{details: %{reason: :too_many_inflight_fetches}}} =
             Credentials.fetch_token(second_request,
               cache: cache,
               provider_options: provider_options
             )

    refute_receive {:provider_fetch, _other_pid}, 50
    send(provider_pid, :release)
    assert {:ok, %Token{access_token: "first"}} = Task.await(first)
  end

  test "cache bounds waiters for one in-flight key" do
    {cache, responses} =
      start_cache_with_opts([{:block, self(), token("shared", 3_600)}],
        max_waiters_per_key: 1
      )

    request = Request.new!("https://vault.azure.net", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]
    first = Task.async(fn -> Credentials.fetch_token(request, opts) end)

    assert_receive {:provider_fetch, provider_pid}

    assert {:error, %TokenError{details: %{reason: :too_many_waiters}}} =
             Credentials.fetch_token(request, opts)

    send(provider_pid, :release)
    assert {:ok, %Token{access_token: "shared"}} = Task.await(first)
  end

  test "fetch timeout cleans up in-flight state for a retry" do
    responses = [
      {:block, self(), token("never-returned", 3_600)},
      {:ok, token("retried", 3_600)}
    ]

    {cache, responses} = start_cache_with_opts(responses, fetch_timeout: 20)
    request = Request.new!("https://vault.azure.net", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]

    assert {:error, %TokenError{details: %{reason: :timeout}}} =
             Credentials.fetch_token(request, opts)

    assert {:ok, %Token{access_token: "retried"}} = Credentials.fetch_token(request, opts)
  end

  test "an expired stale token is never returned after refresh failure" do
    error = %TokenError{type: :connection_error, message: "offline", retryable?: true}
    expiring = token_at("expiring", DateTime.add(DateTime.utc_now(), 40, :millisecond))
    {cache, responses} = start_cache([{:ok, expiring}, {:error, error}], 3_600)
    request = Request.new!("https://storage.azure.com/", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]

    assert {:ok, %Token{access_token: "expiring"}} = Credentials.fetch_token(request, opts)
    Process.sleep(60)
    assert {:error, %TokenError{message: "offline"}} = Credentials.fetch_token(request, opts)
  end

  test "bounded entries evict an older token" do
    responses = [
      {:ok, token("one", 3_600)},
      {:ok, token("two", 3_600)},
      {:ok, token("one-refetched", 3_600)}
    ]

    {cache, responses} = start_cache_with_opts(responses, max_entries: 1)
    one = Request.new!("https://vault.azure.net/one", provider: TestProvider)
    two = Request.new!("https://vault.azure.net/two", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]

    assert {:ok, %Token{access_token: "one"}} = Credentials.fetch_token(one, opts)
    assert {:ok, %Token{access_token: "two"}} = Credentials.fetch_token(two, opts)
    assert {:ok, %Token{access_token: "one-refetched"}} = Credentials.fetch_token(one, opts)
  end

  test "cache restart discards tokens and safely refetches" do
    {cache, responses} =
      start_cache([{:ok, token("before-restart", 3_600)}, {:ok, token("after-restart", 3_600)}])

    request = Request.new!("https://vault.azure.net", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]

    assert {:ok, %Token{access_token: "before-restart"}} = Credentials.fetch_token(request, opts)
    previous = Process.whereis(cache)
    Process.exit(previous, :kill)
    assert new_cache_pid(cache, previous)

    assert {:ok, %Token{access_token: "after-restart"}} = Credentials.fetch_token(request, opts)
  end

  test "cache restart terminates owned in-flight provider tasks" do
    supervisor = unique_name("credentials_supervisor")
    task_supervisor = unique_name("owned_task_supervisor")
    cache = unique_name("owned_cache")

    responses =
      start_response_agent([
        {:block, self(), token("orphan", 3_600)},
        {:ok, token("after-restart", 3_600)}
      ])

    start_supervised!(%{
      id: supervisor,
      start:
        {CredentialsSupervisor, :start_link,
         [
           [
             supervisor_name: supervisor,
             task_supervisor: task_supervisor,
             cache_name: cache
           ]
         ]}
    })

    request = Request.new!("https://vault.azure.net", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]
    fetch = Task.async(fn -> Credentials.fetch_token(request, opts) end)

    assert_receive {:provider_fetch, provider_pid}
    provider_monitor = Process.monitor(provider_pid)
    previous_cache = Process.whereis(cache)
    Process.exit(previous_cache, :kill)

    assert_receive {:DOWN, ^provider_monitor, :process, ^provider_pid, _reason}
    assert new_cache_pid(cache, previous_cache)
    assert {:error, %TokenError{details: %{reason: :cache_unavailable}}} = Task.await(fetch)

    assert {:ok, %Token{access_token: "after-restart"}} = Credentials.fetch_token(request, opts)
  end

  test "a refresh failure returns a cached token while it remains valid" do
    error = %TokenError{type: :connection_error, message: "offline", retryable?: true}
    {cache, responses} = start_cache([{:ok, token("cached", 600)}, {:error, error}], 3_600)
    request = Request.new!("https://storage.azure.com/", provider: TestProvider)
    opts = [cache: cache, provider_options: [responses: responses]]

    assert {:ok, %Token{access_token: "cached"}} = Credentials.fetch_token(request, opts)
    assert {:ok, %Token{access_token: "cached"}} = Credentials.fetch_token(request, opts)
  end

  test "direct provider calls never return an expired token" do
    responses = start_supervised!({Agent, fn -> [{:ok, token("expired", -1)}] end})
    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    assert {:error, %TokenError{type: :authentication_error, retryable?: true}} =
             Credentials.fetch_token(request,
               cache: false,
               provider_options: [responses: responses]
             )
  end

  test "direct provider calls run in an owned process with a finite timeout" do
    owner = self()

    responses =
      start_supervised!(
        {Agent, fn -> [{:block, owner, token("never-returned", 3_600)}] end}
      )

    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    assert {:error,
            %TokenError{
              type: :connection_error,
              retryable?: true,
              details: %{reason: :provider_timeout}
            }} =
             Credentials.fetch_token(request,
               cache: false,
               timeout: 20,
               provider_options: [responses: responses]
             )

    assert_receive {:provider_fetch, provider_pid}
    monitor = Process.monitor(provider_pid)
    assert_receive {:DOWN, ^monitor, :process, ^provider_pid, _reason}
  end

  test "prebuilt requests reject selector options that cannot change their identity" do
    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    assert {:error, %TokenError{type: :invalid_config}} =
             Credentials.fetch_token(request, provider: :azure_cli, cache: false)
  end

  test "a direct provider kill does not exit its caller" do
    request = Request.new!("https://vault.azure.net", provider: KillingProvider)

    assert {:error,
            %TokenError{
              type: :connection_error,
              retryable?: true,
              details: %{reason: :provider_exited}
            }} = Credentials.fetch_token(request, cache: false, timeout: 100)
  end

  test "token error inspection always redacts provider messages and details" do
    error = %TokenError{
      type: :authentication_error,
      message: "response contained secret-token",
      details: %{authorization: "Bearer secret-token"}
    }

    inspected = inspect(error)

    assert inspected =~ "Favn.Azure.TokenError"
    refute inspected =~ "secret-token"
    refute inspected =~ "authorization"
  end

  test "credential requests and provider options are size bounded" do
    assert {:error, %TokenError{type: :invalid_config}} =
             Request.new(String.duplicate("r", 4_097), provider: :azure_cli)

    request = Request.new!("https://vault.azure.net", provider: TestProvider)

    assert {:error, %TokenError{type: :invalid_config}} =
             Credentials.fetch_token(request,
               cache: false,
               provider_options: [payload: String.duplicate("x", 65_537)]
             )
  end

  test "token refs are secret, inert, and do not inspect credentials" do
    ref =
      Credentials.token_ref("https://storage.azure.com/",
        provider: :managed_identity,
        client_id: "identity-1"
      )

    assert %Favn.RuntimeValue.Ref{provider: Credentials, secret?: true} = ref
    refute inspect(ref) =~ "identity-1"
  end

  test "the PostgreSQL facade delegates to the shared credential source" do
    system_cmd = fn _command, _args, _opts ->
      {Jason.encode!(%{accessToken: "postgres-token", expires_on: "1893456000"}), 0}
    end

    assert {:ok, %Token{access_token: "postgres-token"}} =
             PostgresEntraToken.fetch_token(
               [provider: :azure_cli],
               cache: false,
               system_cmd: system_cmd
             )
  end

  defp start_cache(responses, refresh_before_seconds \\ 300) do
    start_cache_with_opts(responses, refresh_before_seconds: refresh_before_seconds)
  end

  defp start_cache_with_opts(responses, cache_opts) do
    task_supervisor = unique_name("task_supervisor")
    cache = unique_name("cache")
    agent = start_response_agent(responses)

    start_supervised!(%{
      id: task_supervisor,
      start: {Task.Supervisor, :start_link, [[name: task_supervisor]]}
    })

    start_supervised!(%{
      id: cache,
      start:
        {Cache, :start_link,
         [
           Keyword.merge(cache_opts, name: cache, task_supervisor: task_supervisor)
         ]}
    })

    {cache, agent}
  end

  defp token(value, valid_for_seconds) do
    token_at(value, DateTime.add(DateTime.utc_now(), valid_for_seconds, :second))
  end

  defp token_at(value, expires_at) do
    {:ok, token} = Token.new(value, expires_at)
    token
  end

  defp start_response_agent(responses) do
    id = {:responses, System.unique_integer([:positive])}
    start_supervised!(%{id: id, start: {Agent, :start_link, [fn -> responses end]}})
  end

  defp unique_name(suffix),
    do: String.to_atom("#{__MODULE__}.#{suffix}.#{System.unique_integer([:positive])}")

  defp new_cache_pid(name, previous, attempts \\ 100)
  defp new_cache_pid(_name, _previous, 0), do: nil

  defp new_cache_pid(name, previous, attempts) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != previous -> pid
      _other ->
        Process.sleep(5)
        new_cache_pid(name, previous, attempts - 1)
    end
  end
end
