defmodule FavnOrchestrator.API.IdempotentCommandTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test
  import ExUnit.CaptureLog

  alias FavnOrchestrator.API.IdempotentCommand
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Runtime
  alias FavnOrchestrator.Persistence.Stores
  alias FavnOrchestrator.Persistence.WorkspaceContext

  defmodule IdentityStore do
    def reserve_operator_command(command) do
      Agent.get_and_update(agent(), fn state ->
        next = %{state | reservations: [command | state.reservations]}

        reply =
          case state.reserve do
            :ok ->
              {:ok,
               %{
                 key_hash: command.key_hash,
                 request_fingerprint: command.request_fingerprint,
                 expires_at: command.expires_at,
                 replayed?: false
               }}

            :fail ->
              {:error, Error.new(:unavailable, "audit reservation unavailable")}
          end

        {reply, next}
      end)
    end

    def complete_operator_command(command) do
      Agent.get_and_update(agent(), fn state ->
        reply =
          if state.complete == :ok,
            do: :ok,
            else: {:error, Error.new(:unavailable, "audit completion unavailable")}

        {reply, %{state | completions: [command | state.completions]}}
      end)
    end

    defp agent,
      do: Application.fetch_env!(:favn_orchestrator, :idempotent_command_test_agent)
  end

  setup do
    previous_key = Application.get_env(:favn_orchestrator, :operator_command_hmac_key)

    Application.put_env(
      :favn_orchestrator,
      :operator_command_hmac_key,
      :crypto.strong_rand_bytes(32)
    )

    {:ok, agent} =
      Agent.start(fn ->
        %{
          reserve: :ok,
          complete: :ok,
          reservations: [],
          completions: [],
          executions: 0,
          mutations: MapSet.new()
        }
      end)

    Application.put_env(:favn_orchestrator, :idempotent_command_test_agent, agent)

    assert {:ok, runtime} =
             Runtime.start_link(%Runtime{
               backend: __MODULE__,
               options: [],
               stores: struct(Stores, identity: IdentityStore)
             })

    Process.unlink(runtime)

    on_exit(fn ->
      if Process.alive?(runtime), do: GenServer.stop(runtime)
      if Process.alive?(agent), do: Agent.stop(agent)
      Application.delete_env(:favn_orchestrator, :idempotent_command_test_agent)
      restore_env(:operator_command_hmac_key, previous_key)
    end)

    {:ok, context} =
      WorkspaceContext.new("workspace-audit", "service:api", [:workspace_admin],
        request_id: "api-service:api"
      )

    principal = %{
      kind: :service,
      id: "service:api",
      actor_id: nil,
      session_id: nil,
      service_identity: "api"
    }

    %{agent: agent, context: context, principal: principal}
  end

  test "reservation failure prevents command execution", fixture do
    Agent.update(fixture.agent, &%{&1 | reserve: :fail})

    conn = run(fixture, success_callback(fixture.agent))

    assert conn.status == 503
    assert Agent.get(fixture.agent, & &1.executions) == 0
    assert Agent.get(fixture.agent, & &1.completions) == []
  end

  test "completion failure after execution returns unknown instead of success", fixture do
    Agent.update(fixture.agent, &%{&1 | complete: :fail})

    conn = run(fixture, success_callback(fixture.agent))

    assert conn.status == 500
    details = Jason.decode!(conn.resp_body)["error"]["details"]
    assert details["outcome"] == "unknown"
    assert details["retry_with_same_idempotency_key"]
    assert Agent.get(fixture.agent, & &1.executions) == 1

    assert [%{outcome: "accepted", principal_kind: :service}] =
             Agent.get(fixture.agent, & &1.completions)
  end

  test "callback failures are logged without leaking callback data", fixture do
    secret = "callback-secret-never-log"

    log =
      capture_log(fn ->
        conn = run(fixture, fn _idempotency -> raise "password=#{secret}" end)
        assert conn.status == 500
      end)

    refute log =~ secret
    assert log =~ "atomic idempotent command failed"

    malformed_log =
      capture_log(fn ->
        conn = run(fixture, fn _idempotency -> %{password: secret} end)
        assert conn.status == 500
      end)

    refute malformed_log =~ secret
    assert malformed_log =~ "[REDACTED]"
  end

  test "server error callbacks return explicit same-key retry guidance", fixture do
    secret = "server-error-private-detail"

    conn =
      run(fixture, fn _idempotency ->
        {:error, 503, "service_unavailable", "Command outcome is unknown",
         %{
           password: secret
         }}
      end)

    assert conn.status == 503
    response = Jason.decode!(conn.resp_body)

    assert response["error"]["details"] == %{
             "outcome" => "unknown",
             "retry_with_same_idempotency_key" => true
           }

    refute conn.resp_body =~ secret
    assert [%{outcome: "unknown"}] = Agent.get(fixture.agent, & &1.completions)
  end

  test "service commands use service idempotency and durable terminal evidence", fixture do
    conn = run(fixture, success_callback(fixture.agent))

    assert conn.status == 202

    assert [%{principal_kind: :service, principal_id: "service:api", actor_id: nil}] =
             Agent.get(fixture.agent, & &1.reservations)

    assert [%{principal_kind: :service, principal_id: "service:api", outcome: "accepted"}] =
             Agent.get(fixture.agent, & &1.completions)
  end

  test "exact-key replay can heal a lost completion without repeating the domain mutation",
       fixture do
    Agent.update(fixture.agent, &%{&1 | complete: :fail})
    callback = idempotent_domain_callback(fixture.agent)

    assert run(fixture, callback).status == 500
    Agent.update(fixture.agent, &%{&1 | complete: :ok})
    assert run(fixture, callback).status == 202

    state = Agent.get(fixture.agent, & &1)
    assert state.executions == 2
    assert MapSet.size(state.mutations) == 1
    assert length(state.completions) == 2
  end

  defp run(fixture, callback) do
    conn =
      :post
      |> conn("/commands", "")
      |> put_req_header("idempotency-key", "command-key:0123456789abcdef")

    IdempotentCommand.run(
      conn,
      fixture.context,
      "run.submit",
      fixture.principal,
      fn idempotency -> {"run", idempotency.run_id} end,
      %{target: "asset-a"},
      callback
    )
  end

  defp success_callback(agent) do
    fn idempotency ->
      Agent.update(agent, &%{&1 | executions: &1.executions + 1})

      assert idempotency.command_idempotency.principal_kind == :service
      {:ok, 202, %{run_id: idempotency.run_id}, "run", idempotency.run_id}
    end
  end

  defp idempotent_domain_callback(agent) do
    fn idempotency ->
      Agent.update(agent, fn state ->
        %{
          state
          | executions: state.executions + 1,
            mutations: MapSet.put(state.mutations, idempotency.key_hash)
        }
      end)

      {:ok, 202, %{run_id: idempotency.run_id}, "run", idempotency.run_id}
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
