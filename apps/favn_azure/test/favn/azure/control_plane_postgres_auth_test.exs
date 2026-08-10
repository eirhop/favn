defmodule Favn.Azure.ControlPlanePostgresAuthTest do
  use ExUnit.Case, async: false

  alias Favn.Azure.ControlPlanePostgresAuth
  alias Favn.Azure.Credentials.Request
  alias Favn.Azure.{PostgresAuthenticationError, Token, TokenError}

  @now ~U[2030-01-01 00:00:00Z]

  defmodule TestProvider do
    @behaviour Favn.Azure.CredentialProvider

    @impl true
    def fetch_token(request, options) do
      if owner = Keyword.get(options, :request_owner) do
        send(owner, {:credential_request, request.client_id})
      end

      response =
        options
        |> Keyword.fetch!(:responses)
        |> Agent.get_and_update(fn [response | rest] -> {response, rest} end)

      case response do
        {:block, owner, result} ->
          send(owner, {:provider_fetch, self()})

          receive do
            :release -> result
          end

        result ->
          result
      end
    end
  end

  setup do
    names =
      [AuthSupervisor, AuthServer, AuthTaskSupervisor, AuthCache] ++
        Enum.flat_map(
          [
            BootstrapLifecycle,
            BootstrapMaintenanceLifecycle,
            BootstrapTargetLifecycle,
            MigratorLifecycle,
            RuntimeLifecycle
          ],
          &lifecycle_names/1
        )

    Enum.each(names, &stop_named/1)

    on_exit(fn -> Enum.each(names, &stop_named/1) end)

    :ok
  end

  test "independent lifecycle returns a valid token and exposes only redacted status" do
    {:ok, token} = Token.new("token-canary", DateTime.add(@now, 3_600, :second))
    {:ok, responses} = Agent.start_link(fn -> [{:ok, token}] end)
    options = provider_options(responses, client_id: "client-id-canary")

    start_provider!(options)

    assert {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)
    refute inspect(reference) =~ "client-id-canary"
    assert {:ok, "token-canary"} = ControlPlanePostgresAuth.connection_password(reference)

    status = eventually(fn -> ControlPlanePostgresAuth.status(options) end)

    assert %{
             lifecycle_ready?: true,
             user_assigned?: true,
             successful_provider_fetches: 1,
             successful_password_deliveries: 1,
             failed_provider_fetches: 0,
             last_provider_fetch_monotonic_ms: provider_fetch_at,
             last_password_delivery_monotonic_ms: password_delivery_at
           } = status

    assert is_integer(provider_fetch_at)
    assert is_integer(password_delivery_at)

    refute inspect(:sys.get_status(AuthServer)) =~ "client-id-canary"
    refute inspect(:sys.get_status(AuthServer)) =~ "token-canary"
    refute inspect(:sys.get_status(AuthCache)) =~ "client-id-canary"
    refute inspect(:sys.get_status(AuthCache)) =~ "token-canary"
  end

  test "three sequential lifecycles request tokens for their exact configured client ids" do
    profiles = [
      {BootstrapLifecycle, "11111111-1111-1111-1111-111111111111"},
      {MigratorLifecycle, "22222222-2222-2222-2222-222222222222"},
      {RuntimeLifecycle, "33333333-3333-3333-3333-333333333333"}
    ]

    Enum.each(profiles, fn {namespace, client_id} ->
      {:ok, token} =
        Token.new("token-for-#{namespace}", DateTime.add(@now, 3_600, :second))

      {:ok, responses} = Agent.start_link(fn -> [{:ok, token}] end)

      options =
        provider_options(responses,
          client_id: client_id,
          provider_options: [responses: responses, request_owner: self()]
        )
        |> Keyword.merge(lifecycle_options(namespace))

      lifecycle = start_isolated_provider!(options)

      try do
        assert {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)
        assert {:ok, _token} = ControlPlanePostgresAuth.connection_password(reference)
        assert_receive {:credential_request, ^client_id}
      after
        Supervisor.stop(lifecycle)
      end
    end)

    refute_receive {:credential_request, _unexpected_client_id}
  end

  test "bootstrap maintenance and target lifecycles run concurrently and restart cleanly" do
    for _attempt <- 1..2 do
      {maintenance_options, maintenance_client_id} =
        concurrent_lifecycle_options(
          BootstrapMaintenanceLifecycle,
          "11111111-1111-1111-1111-111111111111",
          "maintenance-token-canary",
          self()
        )

      {target_options, target_client_id} =
        concurrent_lifecycle_options(
          BootstrapTargetLifecycle,
          "11111111-1111-1111-1111-111111111111",
          "target-token-canary",
          self()
        )

      maintenance_lifecycle = start_isolated_provider!(maintenance_options)
      target_lifecycle = start_isolated_provider!(target_options)

      assert {:ok, maintenance_reference} =
               ControlPlanePostgresAuth.connection_reference(maintenance_options)

      assert {:ok, target_reference} =
               ControlPlanePostgresAuth.connection_reference(target_options)

      assert {:ok, "maintenance-token-canary"} =
               ControlPlanePostgresAuth.connection_password(maintenance_reference)

      assert {:ok, "target-token-canary"} =
               ControlPlanePostgresAuth.connection_password(target_reference)

      assert_receive {:credential_request, ^maintenance_client_id}
      assert_receive {:credential_request, ^target_client_id}

      Supervisor.stop(target_lifecycle)
      Supervisor.stop(maintenance_lifecycle)

      for name <-
            lifecycle_names(BootstrapMaintenanceLifecycle) ++
              lifecycle_names(BootstrapTargetLifecycle) do
        refute Process.whereis(name)
      end
    end
  end

  test "a migrator token failure remains classified after bootstrap token success" do
    bootstrap_client_id = "11111111-1111-1111-1111-111111111111"
    migrator_client_id = "22222222-2222-2222-2222-222222222222"

    {:ok, bootstrap_token} =
      Token.new("bootstrap-token-canary", DateTime.add(@now, 3_600, :second))

    {:ok, bootstrap_responses} = Agent.start_link(fn -> [{:ok, bootstrap_token}] end)

    bootstrap_options =
      provider_options(bootstrap_responses,
        client_id: bootstrap_client_id,
        provider_options: [responses: bootstrap_responses, request_owner: self()]
      )
      |> Keyword.merge(lifecycle_options(BootstrapLifecycle))

    bootstrap_lifecycle = start_isolated_provider!(bootstrap_options)
    {:ok, bootstrap_reference} = ControlPlanePostgresAuth.connection_reference(bootstrap_options)

    assert {:ok, "bootstrap-token-canary"} =
             ControlPlanePostgresAuth.connection_password(bootstrap_reference)

    assert_receive {:credential_request, ^bootstrap_client_id}
    Supervisor.stop(bootstrap_lifecycle)

    provider_failure = %TokenError{
      type: :connection_error,
      message: "migrator-provider-detail-canary",
      retryable?: true,
      details: %{reason: :provider_timeout, header: "identity-header-canary"}
    }

    {:ok, migrator_responses} = Agent.start_link(fn -> [{:error, provider_failure}] end)

    migrator_options =
      provider_options(migrator_responses,
        client_id: migrator_client_id,
        provider_options: [responses: migrator_responses, request_owner: self()]
      )
      |> Keyword.merge(lifecycle_options(MigratorLifecycle))

    migrator_lifecycle = start_isolated_provider!(migrator_options)

    try do
      {:ok, migrator_reference} = ControlPlanePostgresAuth.connection_reference(migrator_options)

      assert {:error,
              %PostgresAuthenticationError{class: :token_timeout, retryable?: true} = error} =
               ControlPlanePostgresAuth.connection_password(migrator_reference)

      assert_receive {:credential_request, ^migrator_client_id}
      refute inspect(error) =~ "migrator-provider-detail-canary"
      refute inspect(error) =~ "identity-header-canary"
    after
      Supervisor.stop(migrator_lifecycle)
    end
  end

  test "near-expiry and provider failures remain classified and redacted" do
    {:ok, near_expiry} = Token.new("near-expiry-canary", DateTime.add(@now, 60, :second))

    provider_failure = %TokenError{
      type: :connection_error,
      message: "raw-provider-message-canary",
      retryable?: true,
      details: %{reason: :provider_timeout, header: "identity-header-canary"}
    }

    {:ok, responses} = Agent.start_link(fn -> [{:ok, near_expiry}] end)

    options = provider_options(responses)
    start_provider!(options)
    {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)

    assert {:error,
            %PostgresAuthenticationError{
              class: :insufficient_validity,
              retryable?: true
            } = validity_error} = ControlPlanePostgresAuth.connection_password(reference)

    refute inspect(validity_error) =~ "near-expiry-canary"

    :ok = stop_supervised(AuthSupervisor)
    {:ok, failure_responses} = Agent.start_link(fn -> [{:error, provider_failure}] end)
    failure_options = provider_options(failure_responses)
    start_provider!(failure_options)
    {:ok, failure_reference} = ControlPlanePostgresAuth.connection_reference(failure_options)

    assert {:error,
            %PostgresAuthenticationError{class: :token_timeout, retryable?: true} =
              timeout_error} = ControlPlanePostgresAuth.connection_password(failure_reference)

    inspected = inspect(timeout_error)
    refute inspected =~ "raw-provider-message-canary"
    refute inspected =~ "identity-header-canary"

    assert %{last_failure_class: :token_timeout} =
             eventually(fn -> ControlPlanePostgresAuth.status(failure_options) end)
  end

  test "concurrent password requests share one bounded provider fetch" do
    {:ok, token} = Token.new("shared-token-canary", DateTime.add(@now, 3_600, :second))
    owner = self()
    {:ok, responses} = Agent.start_link(fn -> [{:block, owner, {:ok, token}}] end)
    options = provider_options(responses)

    start_provider!(options)
    {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)

    callers =
      for _index <- 1..8 do
        Task.async(fn -> ControlPlanePostgresAuth.connection_password(reference) end)
      end

    assert_receive {:provider_fetch, provider_pid}
    refute_receive {:provider_fetch, _other_provider_pid}, 50
    send(provider_pid, :release)

    assert Enum.all?(callers, fn caller ->
             Task.await(caller) == {:ok, "shared-token-canary"}
           end)

    status =
      eventually(
        fn -> ControlPlanePostgresAuth.status(options) end,
        &match?(%{successful_password_deliveries: 8}, &1)
      )

    assert %{
             successful_provider_fetches: 1,
             successful_password_deliveries: 8,
             max_inflight: max_inflight,
             max_waiters_per_key: max_waiters
           } = status

    assert is_integer(max_inflight)
    assert is_integer(max_waiters)
  end

  test "a later connection refreshes a token inside the validity margin" do
    {:ok, clock} = Agent.start_link(fn -> @now end)
    first_expiry = DateTime.add(@now, 600, :second)
    later_now = DateTime.add(@now, 400, :second)
    second_expiry = DateTime.add(later_now, 3_600, :second)
    {:ok, first} = Token.new("first-token-canary", first_expiry)
    {:ok, second} = Token.new("second-token-canary", second_expiry)
    {:ok, responses} = Agent.start_link(fn -> [{:ok, first}, {:ok, second}] end)

    options = provider_options(responses, clock: fn -> Agent.get(clock, & &1) end)
    start_provider!(options)
    {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)

    assert {:ok, "first-token-canary"} =
             ControlPlanePostgresAuth.connection_password(reference)

    Agent.update(clock, fn _previous -> later_now end)

    assert {:ok, "second-token-canary"} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert %{successful_provider_fetches: 2, successful_password_deliveries: 2} =
             eventually(
               fn -> ControlPlanePostgresAuth.status(options) end,
               &match?(%{successful_password_deliveries: 2}, &1)
             )
  end

  test "provider failure classes remain distinct and a later attempt recovers" do
    rejected = %TokenError{
      type: :authentication_error,
      message: "identity-rejected-canary",
      retryable?: false
    }

    unavailable = %TokenError{
      type: :connection_error,
      message: "identity-unavailable-canary",
      retryable?: true,
      details: %{reason: :connection_refused}
    }

    timeout = %TokenError{
      type: :connection_error,
      message: "timeout-canary",
      retryable?: true,
      details: %{reason: :provider_timeout}
    }

    {:ok, recovered} = Token.new("recovered-token-canary", DateTime.add(@now, 3_600, :second))

    {:ok, responses} =
      Agent.start_link(fn ->
        [{:error, rejected}, {:error, unavailable}, {:error, timeout}, {:ok, recovered}]
      end)

    options = provider_options(responses)
    start_provider!(options)
    {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)

    assert {:error, %PostgresAuthenticationError{class: :identity_rejected, retryable?: false}} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert {:error, %PostgresAuthenticationError{class: :identity_unavailable, retryable?: true}} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert {:error, %PostgresAuthenticationError{class: :token_timeout, retryable?: true}} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert {:ok, "recovered-token-canary"} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert %{
             successful_provider_fetches: 1,
             failed_provider_fetches: 3,
             successful_password_deliveries: 1,
             last_failure_class: nil,
             last_provider_failure_class: nil
           } =
             eventually(
               fn -> ControlPlanePostgresAuth.status(options) end,
               &match?(%{successful_provider_fetches: 1, failed_provider_fetches: 3}, &1)
             )
  end

  test "a configured provider fails closed while its lifecycle is unavailable" do
    {:ok, responses} = Agent.start_link(fn -> [] end)
    options = provider_options(responses)
    assert {:ok, reference} = ControlPlanePostgresAuth.connection_reference(options)

    assert {:error, %PostgresAuthenticationError{class: :provider_unavailable, retryable?: true}} =
             ControlPlanePostgresAuth.connection_password(reference)

    assert %{lifecycle_ready?: false} = ControlPlanePostgresAuth.status(options)
  end

  test "credential request inspection redacts resource and client identity" do
    assert {:ok, request} =
             Request.new("https://resource-canary.example",
               provider: "managed_identity",
               client_id: "client-id-canary"
             )

    inspected = inspect(request)
    refute inspected =~ "resource-canary"
    refute inspected =~ "client-id-canary"
    assert inspected =~ "identity: :user_assigned"
  end

  test "identity mapping inspection distinguishes exact, missing, and conflicting principals" do
    mapping = identity_mapping()

    assert {:ok, :exact} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([[mapping.role, "service", mapping.object_id, 0, 0]]),
               [],
               mapping
             )

    assert {:ok, :missing} =
             ControlPlanePostgresAuth.identity_status(fixed_executor([]), [], mapping)

    assert {:ok, :conflict} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([
                 [mapping.role, "service", "ffffffff-ffff-ffff-ffff-ffffffffffff", 0, 0]
               ]),
               [],
               mapping
             )

    assert {:ok, :conflict} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([["another_role", "service", mapping.object_id, 0, 0]]),
               [],
               mapping
             )

    assert {:ok, :conflict} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([[mapping.role, "service", mapping.object_id, 1, 0]]),
               [],
               mapping
             )

    assert {:ok, :conflict} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([[mapping.role, "service", mapping.object_id, 0, 1]]),
               [],
               mapping
             )

    assert {:ok, :conflict} =
             ControlPlanePostgresAuth.identity_status(
               fixed_executor([[mapping.role, "user", mapping.object_id, 0, 0]]),
               [],
               mapping
             )
  end

  test "identity inspection assigns stable local names to provider columns by position" do
    mapping = identity_mapping()
    parent = self()

    executor = fn sql, params ->
      send(parent, {:identity_inspection, sql, params})
      {:ok, %{rows: []}}
    end

    assert {:ok, :missing} =
             ControlPlanePostgresAuth.identity_status(executor, [], mapping)

    assert_receive {:identity_inspection, sql, [role, object_id]}
    assert role == mapping.role
    assert object_id == mapping.object_id

    assert sql =~
             "AS principal(role_name, principal_type, object_id, tenant_id, is_mfa, is_admin)"

    assert sql =~ "principal.role_name"
    assert sql =~ "principal.is_mfa"
    assert sql =~ "principal.is_admin"
    refute sql =~ "principal.rolename"
    refute sql =~ "principal.rolname"
  end

  test "identity creation uses the provider function, verifies the result, and does not embed ids in SQL" do
    mapping = identity_mapping()
    parent = self()

    {:ok, responses} =
      Agent.start_link(fn ->
        [
          {:ok, %{rows: []}},
          {:ok, %{rows: [[false]]}},
          {:ok, %{rows: [[mapping.role]]}},
          {:ok, %{rows: [[mapping.role, "service", mapping.object_id, 0, 0]]}}
        ]
      end)

    executor = fn sql, params ->
      send(parent, {:identity_sql, sql, params})
      Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
    end

    assert {:ok, :created} =
             ControlPlanePostgresAuth.ensure_identity(executor, [], mapping)

    assert_receive {:identity_sql, inspection_sql, [role, search]}
    assert inspection_sql =~ "pgaadauth_list_principals"
    refute inspection_sql =~ mapping.object_id
    assert role == mapping.role
    assert search == mapping.object_id

    assert_receive {:identity_sql, role_sql, [role]}
    assert role_sql =~ "pg_roles"
    assert role == mapping.role

    assert_receive {:identity_sql, create_sql, [role, object_id]}
    assert create_sql =~ "pgaadauth_create_principal_with_oid"
    refute create_sql =~ mapping.role
    refute create_sql =~ mapping.object_id
    assert role == mapping.role
    assert object_id == mapping.object_id
  end

  test "identity mapping refuses to claim an existing plain PostgreSQL role" do
    mapping = identity_mapping()
    parent = self()

    {:ok, responses} =
      Agent.start_link(fn ->
        [
          {:ok, %{rows: []}},
          {:ok, %{rows: [[true]]}}
        ]
      end)

    executor = fn sql, params ->
      send(parent, {:identity_sql, sql, params})
      Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
    end

    assert {:error, :identity_mapping_conflict} =
             ControlPlanePostgresAuth.ensure_identity(executor, [], mapping)

    assert_receive {:identity_sql, _inspection_sql, [_role, _object_id]}
    assert_receive {:identity_sql, _role_sql, [_role]}
    refute_receive {:identity_sql, _mutation_sql, _params}
  end

  test "identity creation fails closed on conflicts and reports an unknown post-write outcome" do
    mapping = identity_mapping()

    assert {:error, :identity_mapping_conflict} =
             ControlPlanePostgresAuth.ensure_identity(
               fixed_executor([
                 [mapping.role, "service", "ffffffff-ffff-ffff-ffff-ffffffffffff", 0, 0]
               ]),
               [],
               mapping
             )

    {:ok, responses} =
      Agent.start_link(fn ->
        [
          {:ok, %{rows: []}},
          {:ok, %{rows: [[false]]}},
          {:error, :connection_lost_after_request}
        ]
      end)

    executor = fn _sql, _params ->
      Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
    end

    assert {:error, :unknown_outcome} =
             ControlPlanePostgresAuth.ensure_identity(executor, [], mapping)

    {:ok, responses} =
      Agent.start_link(fn ->
        [
          {:ok, %{rows: []}},
          {:ok, %{rows: [[false]]}},
          {:ok, %{rows: [[mapping.role]]}},
          {:error, :connection_lost_during_verification}
        ]
      end)

    executor = fn _sql, _params ->
      Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
    end

    assert {:error, :unknown_outcome} =
             ControlPlanePostgresAuth.ensure_identity(executor, [], mapping)
  end

  test "identity provider prerequisites and definite authority rejection remain actionable" do
    mapping = identity_mapping()

    assert {:error, :identity_provider_prerequisite} =
             ControlPlanePostgresAuth.identity_status(
               fn _sql, _params -> {:error, :identity_provider_prerequisite} end,
               [],
               mapping
             )

    assert {:error, :identity_mapping_not_authorized} =
             ControlPlanePostgresAuth.identity_status(
               fn _sql, _params -> {:error, :identity_mapping_not_authorized} end,
               [],
               mapping
             )

    {:ok, responses} =
      Agent.start_link(fn ->
        [
          {:ok, %{rows: []}},
          {:ok, %{rows: [[false]]}},
          {:error, :identity_mapping_not_authorized}
        ]
      end)

    executor = fn _sql, _params ->
      Agent.get_and_update(responses, fn [response | rest] -> {response, rest} end)
    end

    assert {:error, :identity_mapping_not_authorized} =
             ControlPlanePostgresAuth.ensure_identity(executor, [], mapping)
  end

  test "unexpected identity inspection errors remain bounded and redacted" do
    mapping = identity_mapping()

    result =
      ControlPlanePostgresAuth.identity_status(
        fn _sql, _params ->
          {:error,
           %{
             message: "provider-message-canary",
             username: "database-user-canary",
             url: "ecto://database-user-canary:database-password-canary@postgres.example/favn",
             access_token: "access-token-canary"
           }}
        end,
        [],
        mapping
      )

    assert result == {:error, :identity_inspection_failed}

    inspected = inspect(result)

    for canary <- [
          "provider-message-canary",
          "database-user-canary",
          "database-password-canary",
          "postgres.example",
          "access-token-canary"
        ] do
      refute inspected =~ canary
    end
  end

  defp identity_mapping do
    %{
      role: "favn_migrator",
      object_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
      purpose: :migrator
    }
  end

  defp fixed_executor(rows) do
    fn _sql, _params -> {:ok, %{rows: rows}} end
  end

  defp provider_options(responses, overrides \\ []) do
    [
      credential_provider: TestProvider,
      provider_options: [responses: responses],
      clock: fn -> @now end,
      minimum_validity_seconds: 300,
      fetch_timeout: 1_000,
      supervisor_name: AuthSupervisor,
      server_name: AuthServer,
      task_supervisor: AuthTaskSupervisor,
      cache_name: AuthCache
    ]
    |> Keyword.merge(overrides)
  end

  defp start_provider!(options) do
    assert {:ok, [child_spec]} = ControlPlanePostgresAuth.child_specs(options)
    start_supervised!(child_spec)
  end

  defp start_isolated_provider!(options) do
    assert {:ok, [child_spec]} = ControlPlanePostgresAuth.child_specs(options)
    assert {:ok, lifecycle} = Supervisor.start_link([child_spec], strategy: :one_for_one)
    lifecycle
  end

  defp concurrent_lifecycle_options(namespace, client_id, access_token, owner) do
    {:ok, token} = Token.new(access_token, DateTime.add(@now, 3_600, :second))
    {:ok, responses} = Agent.start_link(fn -> [{:ok, token}] end)

    options =
      provider_options(responses,
        client_id: client_id,
        provider_options: [responses: responses, request_owner: owner]
      )
      |> Keyword.merge(lifecycle_options(namespace))

    {options, client_id}
  end

  defp lifecycle_options(namespace) do
    [
      supervisor_name,
      credentials_supervisor_name,
      server_name,
      task_supervisor,
      cache_name
    ] = lifecycle_names(namespace)

    [
      supervisor_name: supervisor_name,
      credentials_supervisor_name: credentials_supervisor_name,
      server_name: server_name,
      task_supervisor: task_supervisor,
      cache_name: cache_name
    ]
  end

  defp lifecycle_names(namespace) do
    [
      Module.concat(namespace, Supervisor),
      Module.concat(namespace, CredentialsSupervisor),
      Module.concat(namespace, Server),
      Module.concat(namespace, TaskSupervisor),
      Module.concat(namespace, Cache)
    ]
  end

  defp eventually(function, predicate \\ &settled?/1, attempts \\ 20)

  defp eventually(function, predicate, attempts) when attempts > 1 do
    result = function.()

    if predicate.(result) do
      result
    else
      Process.sleep(10)
      eventually(function, predicate, attempts - 1)
    end
  end

  defp eventually(function, _predicate, _attempts), do: function.()

  defp settled?(result) do
    match?(%{successful_password_deliveries: count} when count > 0, result) or
      match?(%{last_failure_class: class} when not is_nil(class), result)
  end

  defp stop_named(name) do
    if pid = Process.whereis(name), do: Supervisor.stop(pid)
  catch
    :exit, _reason -> :ok
  end
end
