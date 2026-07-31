defmodule FavnStoragePostgres.StorageV2.AuthenticationTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias FavnStoragePostgres.Authentication
  alias FavnStoragePostgres.Backend

  defmodule SuccessfulProvider do
    def connection_password(_options), do: {:ok, "token-canary"}
  end

  defmodule FailingProvider do
    def connection_password(_options) do
      {:error, %{class: :token_timeout, retryable?: true, private: "provider-canary"}}
    end
  end

  defmodule InvalidProvider do
    def connection_password(_options), do: :invalid
  end

  defmodule SignallingProvider do
    def connection_password(options) do
      send(Keyword.fetch!(options, :test_pid), :authentication_attempt)
      {:error, %{class: :token_timeout, retryable?: true}}
    end
  end

  defmodule UnavailableLifecycleProvider do
    def applications(_options), do: {:ok, [:favn_storage_postgres]}
    def child_specs(_options), do: {:ok, []}
    def connection_reference(_options), do: {:ok, [server: :unavailable_test_provider]}
    def connection_password(_reference), do: {:error, %{class: :provider_unavailable}}

    def status(_options),
      do: %{lifecycle_ready?: false, last_failure_class: :provider_unavailable}
  end

  test "a successful provider supplies only the transient connection password" do
    options =
      Authentication.configure_connection(
        [hostname: "postgres.example", password: nil],
        SuccessfulProvider,
        []
      )

    assert options[:hostname] == "postgres.example"
    assert options[:password] == "token-canary"
  end

  test "expected provider failure becomes a non-secret rejected credential" do
    handler_id = "authentication-failure-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler_id,
      [:favn, :storage_postgres, :authentication, :failure],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    options = Authentication.configure_connection([], FailingProvider, [])
    password = Keyword.fetch!(options, :password)

    assert String.starts_with?(password, "favn-managed-identity-unavailable-")
    refute password =~ "provider-canary"

    assert_receive {:telemetry, [:favn, :storage_postgres, :authentication, :failure],
                    %{system_time: _time}, %{class: :token_timeout, retryable?: true}}
  end

  test "programmer defects still raise" do
    assert_raise RuntimeError, fn ->
      Authentication.configure_connection([], InvalidProvider, [])
    end
  end

  test "DBConnection keeps the pool alive and backs off after provider failure" do
    test_pid = self()

    capture_log(fn ->
      assert {:ok, pool} =
               Postgrex.start_link(
                 endpoints: [],
                 database: "favn",
                 username: "favn_runtime",
                 password: nil,
                 pool_size: 1,
                 backoff_min: 10,
                 backoff_max: 20,
                 configure:
                   {Authentication, :configure_connection,
                    [SignallingProvider, [test_pid: test_pid]]}
               )

      assert_receive :authentication_attempt, 500
      assert_receive :authentication_attempt, 500
      assert Process.alive?(pool)
      GenServer.stop(pool)
    end)
  end

  test "Postgrex notifications stay alive and reconnect after provider failure" do
    test_pid = self()

    capture_log(fn ->
      assert {:ok, notifications} =
               Postgrex.Notifications.start_link(
                 endpoints: [],
                 database: "favn",
                 username: "favn_runtime",
                 password: nil,
                 auto_reconnect: true,
                 sync_connect: true,
                 reconnect_backoff: 10,
                 configure:
                   {Authentication, :configure_connection,
                    [SignallingProvider, [test_pid: test_pid]]}
               )

      assert_receive :authentication_attempt, 500
      assert_receive :authentication_attempt, 500
      assert Process.alive?(notifications)
      GenServer.stop(notifications)
    end)
  end

  test "readiness fails as retryable unavailable before querying an unavailable provider" do
    assert {:error, error} =
             Backend.readiness(
               authentication: {:dynamic, UnavailableLifecycleProvider, []},
               hostname: "postgres.example",
               database: "favn",
               username: "favn_runtime",
               ssl_mode: :disable
             )

    assert error.kind == :unavailable
    assert error.retryable?
    assert error.message == "database authentication lifecycle unavailable"
  end
end
