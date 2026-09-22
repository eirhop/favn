defmodule Favn.SQLAsset.MaterializationRetryTest do
  use ExUnit.Case, async: true
  alias Favn.SQL.{Deadline, Error}
  alias Favn.SQLAsset.MaterializationRetry

  defp rejected do
    %Error{
      type: :transaction_conflict,
      message: "native rejected commit",
      operation: :transaction,
      details: %{transaction_stage: :commit, transaction_outcome: :rolled_back}
    }
  end

  test "exactly four total attempts share the deadline and disable outer retries" do
    parent = self()
    deadline = Deadline.new(5000)
    event = [:favn, :sql_asset, :transaction_retry]
    handler = make_ref()

    :ok =
      :telemetry.attach(
        handler,
        event,
        fn _, measurements, metadata, observer ->
          send(observer, {:retry_event, measurements, metadata})
        end,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler) end)

    assert {:error, error} =
             MaterializationRetry.run(
               [
                 deadline: deadline,
                 runtime_publication: %{
                   asset_ref: "asset",
                   run_id: "run",
                   step_id: "step",
                   attempt: 1,
                   publication_id: "publication"
                 }
               ],
               fn opts ->
                 send(parent, {:attempt, opts[:deadline]})
                 {:supported, {:error, rejected()}}
               end
             )

    for _ <- 1..4, do: assert_receive({:attempt, ^deadline})
    refute_receive {:attempt, _}

    for number <- 1..3 do
      assert_receive {:retry_event, %{attempt: ^number, delay_ms: delay, remaining_ms: remaining},
                      %{
                        asset_ref: "asset",
                        run_id: "run",
                        step_id: "step",
                        attempt: 1,
                        publication_id: "publication",
                        conflict_type: :transaction_conflict
                      }}

      assert delay > 0 and remaining > delay and remaining <= 5000
    end

    refute_receive {:retry_event, _, _}
    assert error.details.transaction_retry_attempts == 4
    assert error.details.transaction_retry_stop == "attempt_limit"
    assert error.retryable? == false
    assert Error.rejected_transaction?(error)
  end

  test "success after rejection returns only final evidence" do
    Process.put(:attempt, 0)

    assert {:ok, :final_receipt} =
             MaterializationRetry.run([], fn _ ->
               number = Process.get(:attempt) + 1
               Process.put(:attempt, number)

               if number == 1,
                 do: {:supported, {:error, rejected()}},
                 else: {:supported, {:ok, :final_receipt}}
             end)

    assert Process.get(:attempt) == 2
  end

  test "unknown subsequent write wins over previous rejection" do
    Process.put(:attempt, 0)

    assert {:error, error} =
             MaterializationRetry.run([], fn _ ->
               number = Process.get(:attempt) + 1
               Process.put(:attempt, number)

               if number == 1,
                 do: {:supported, {:error, rejected()}},
                 else:
                   {:supported,
                    {:error,
                     %Error{
                       type: :operation_timeout,
                       message: "lost acknowledgement",
                       details: %{unknown_outcome?: true, transaction_outcome: :unknown}
                     }}}
             end)

    assert Process.get(:attempt) == 2
    assert error.details.transaction_outcome == :unknown
    refute error.retryable?
  end

  test "replacement acquisition failure preserves its classification and safe prior outcome" do
    for type <- [:connection_error, :admission_timeout, :operation_timeout] do
      Process.put(:attempt, 0)

      assert {:error, error} =
               MaterializationRetry.run([], fn _ ->
                 number = Process.get(:attempt) + 1
                 Process.put(:attempt, number)

                 if number == 1,
                   do: {:supported, {:error, rejected()}},
                   else:
                     {:error,
                      %Error{
                        type: type,
                        operation: :connect,
                        message: "unavailable",
                        retryable?: true,
                        details: %{session_phase: :acquiring, classification: :connection}
                      }}
               end)

      assert error.type == type
      assert error.details.classification == :connection
      assert error.details.transaction_outcome == :rolled_back
      assert error.details.prior_rejection.message == "native rejected commit"
      refute error.retryable?
      assert Process.get(:attempt) == 2
    end
  end

  test "unsupported and contradictory outcomes never replay" do
    for {support, error} <- [
          {:unsupported, rejected()},
          {:supported,
           %{rejected() | details: Map.put(rejected().details, :unknown_outcome?, true)}}
        ] do
      parent = self()

      assert {:error, ^error} =
               MaterializationRetry.run([], fn _ ->
                 send(parent, :called)
                 {support, {:error, error}}
               end)

      assert_receive :called
      refute_receive :called
    end
  end

  test "insufficient wait budget retains the proven rejection" do
    assert {:error, error} =
             MaterializationRetry.run([timeout_ms: 1], fn _ ->
               {:supported, {:error, rejected()}}
             end)

    assert error.details.transaction_retry_attempts == 1
    assert error.details.transaction_retry_stop == "deadline"
    assert Error.rejected_transaction?(error)
  end

  test "killing the existing execution worker during wait prevents another attempt" do
    parent = self()

    {pid, ref} =
      spawn_monitor(fn ->
        MaterializationRetry.run([], fn _ ->
          send(parent, :called)
          {:supported, {:error, rejected()}}
        end)
      end)

    assert_receive :called
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}
    refute_receive :called, 150
  end
end
