defmodule FavnSQLRuntime.SQLRetryTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.Error
  alias Favn.SQL.Retry
  alias Favn.SQL.Retry.Classification

  describe "classification" do
    test "normalizes SQLSTATE 53300 as capacity exhaustion" do
      classification =
        Classification.classify(
          error(sqlstate: "53300", message: "too_many_connections"),
          phase: :session_bootstrap
        )

      assert classification.class == :capacity
      assert classification.reason == :metadata_store_connection_exhausted
      assert classification.retryable?
      assert classification.safe_phase?
    end

    test "recognizes common pool capacity messages" do
      classification =
        Classification.classify(
          error(message: "PgBouncer pool timeout: no more connections allowed"),
          phase: :connect
        )

      assert classification.class == :capacity
      assert classification.reason == :pgbouncer_pool_exhausted
    end

    test "treats connection timeout as transient unless capacity is explicit" do
      classification =
        Classification.classify(
          error(type: :connection_error, message: "connection timeout while opening socket"),
          phase: :session_creation
        )

      assert classification.class == :transient
      assert classification.reason == :connection_error
    end

    test "does not retry permanent config or user SQL errors" do
      config = Classification.classify(error(type: :invalid_config), phase: :session_bootstrap)
      sql = Classification.classify(error(type: :execution_error), phase: :read_only)

      assert config.class == :permanent_config
      refute config.retryable?
      assert sql.class == :user_sql_error
      refute sql.retryable?
    end

    test "unknown commit state is never retryable" do
      classification =
        Classification.classify(
          error(
            retryable?: true,
            details: %{classification: :unknown_outcome_timeout},
            message: "operation outcome is unknown"
          ),
          phase: :session_bootstrap
        )

      assert classification.class == :unsafe_unknown_commit_state
      refute classification.retryable?
      assert classification.safe_phase?
    end
  end

  describe "run/2" do
    test "does not retry retryable errors unless the caller supplies a safe phase" do
      {:ok, calls} = Agent.start_link(fn -> 0 end)

      {:error, failed} =
        Retry.run(
          fn ->
            Agent.update(calls, &(&1 + 1))
            {:error, error(type: :connection_error)}
          end,
          policy: [base_delay_ms: 10, jitter: 0.0],
          sleep_fun: fn _delay -> flunk("unsafe phase should not sleep") end
        )

      assert Agent.get(calls, & &1) == 1
      assert failed.details.retry.attempts == 1
      assert failed.details.retry.classification.safe_phase? == false
    end

    test "retries transient failures in session bootstrap and returns success" do
      {:ok, calls} = Agent.start_link(fn -> 0 end)
      parent = self()

      assert {:ok, :connected} =
               Retry.run(
                 fn ->
                   attempt = Agent.get_and_update(calls, &{&1 + 1, &1 + 1})

                   if attempt < 3 do
                     {:error, error(type: :connection_error, message: "connection refused")}
                   else
                     {:ok, :connected}
                   end
                 end,
                 phase: :session_bootstrap,
                 policy: [base_delay_ms: 10, max_delay_ms: 100, jitter: 0.0],
                 sleep_fun: fn delay -> send(parent, {:delay, delay}) end,
                 random_fun: fn -> 0.5 end
               )

      assert Agent.get(calls, & &1) == 3
      assert_received {:delay, 10}
      assert_received {:delay, 20}
    end

    test "uses capacity backoff and attaches retry metadata on final failure" do
      parent = self()

      {:error, failed} =
        Retry.run(
          fn -> {:error, error(sqlstate: "53300", message: "too many clients already")} end,
          phase: :session_creation,
          policy: [
            max_attempts: 3,
            base_delay_ms: 10,
            capacity_base_delay_ms: 250,
            capacity_max_delay_ms: 1_000,
            jitter: 0.0
          ],
          sleep_fun: fn delay -> send(parent, {:delay, delay}) end,
          random_fun: fn -> 0.5 end
        )

      assert_received {:delay, 250}
      assert_received {:delay, 500}

      assert failed.details.retry.attempts == 3
      assert failed.details.retry.max_attempts == 3
      assert failed.details.retry.delays_ms == [250, 500]
      assert failed.details.retry.classification.class == :capacity
      assert failed.details.retry.classification.reason == :metadata_store_connection_exhausted
    end

    test "read-only callers can opt into retry with an explicit safe phase" do
      {:ok, calls} = Agent.start_link(fn -> 0 end)

      assert {:ok, :rows} =
               Retry.run(
                 fn ->
                   attempt = Agent.get_and_update(calls, &{&1 + 1, &1 + 1})

                   if attempt == 1 do
                     {:error, error(type: :connection_error)}
                   else
                     {:ok, :rows}
                   end
                 end,
                 phase: :read_only,
                 policy: [base_delay_ms: 1, jitter: 0.0],
                 sleep_fun: fn _delay -> :ok end
               )

      assert Agent.get(calls, & &1) == 2
    end
  end

  defp error(opts) do
    defaults = [
      type: :connection_error,
      message: "SQL operation failed",
      retryable?: nil,
      details: %{}
    ]

    struct!(Error, Keyword.merge(defaults, opts))
  end
end
