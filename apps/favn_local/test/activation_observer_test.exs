defmodule FavnLocal.ActivationObserverTest do
  use ExUnit.Case, async: true

  alias FavnLocal.ActivationObserver
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @receipt %{"deployment_id" => "deployment", "runtime_revision" => 7}
  @runtime %{deployment_id: "deployment", revision: 7}
  @committed {:ok, %{state: :succeeded, activation_receipt: @receipt}}
  @pending {:ok, %{state: :activating}}

  setup do
    Process.put(:observer_time, 0)
    {:ok, workspace} = WorkspaceContext.new("local-dev", "favn-local", [:platform_operator])

    opts = [
      now: fn -> Process.get(:observer_time) end,
      wait: fn ms ->
        assert ms in 1..250
        Process.put(:observer_time, Process.get(:observer_time) + ms)
      end,
      read_operation: fn ^workspace, "operation" -> flunk("unexpected operation read") end,
      read_runtime: fn ^workspace -> {:ok, @runtime} end,
      cancel: fn ^workspace, "operation", :startup_timeout -> flunk("unexpected cancellation") end
    ]

    %{workspace: workspace, opts: opts}
  end

  test "retries transient operation reads and observes the same committed operation", ctx do
    reads = sequence([{:error, unavailable()}, @pending, @committed])

    opts =
      Keyword.put(ctx.opts, :read_operation, fn workspace, id ->
        assert workspace == ctx.workspace
        assert id == "operation"
        reads.()
      end)

    assert {:ok, @runtime} = ActivationObserver.await(ctx.workspace, "operation", 1_000, opts)
    assert Process.get(:observer_time) == 500
  end

  test "reconciles a committed receipt through transient runtime-read failures", ctx do
    reads = sequence([{:error, unavailable()}, {:ok, @runtime}])

    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> @committed end,
        read_runtime: fn _ -> reads.() end
      )

    assert {:ok, @runtime} = ActivationObserver.await(ctx.workspace, "operation", 1_000, opts)
    assert Process.get(:observer_time) == 250
  end

  test "sustained unreadability returns the initiating error at the original deadline", ctx do
    first = unavailable()
    later = Error.new(:timeout, "later read timed out", retryable?: true)
    reads = sequence([{:error, first}, {:error, later}, {:error, later}])
    opts = Keyword.put(ctx.opts, :read_operation, fn _, _ -> reads.() end)

    assert {:error, {:reload_outcome_unknown, %{operation_id: "operation", reason: ^first}}} =
             ActivationObserver.await(ctx.workspace, "operation", 300, opts)

    assert Process.get(:observer_time) == 300
  end

  test "sustained receipt-read failure cannot turn committed activation into cancellation", ctx do
    error = unavailable()

    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> @committed end,
        read_runtime: fn _ -> {:error, error} end
      )

    assert {:error, {:reload_outcome_unknown, %{reason: ^error}}} =
             ActivationObserver.await(ctx.workspace, "operation", 300, opts)

    assert Process.get(:observer_time) == 300
  end

  test "non-retryable read errors return immediately", ctx do
    error = Error.new(:fenced, "owner fenced")
    opts = Keyword.put(ctx.opts, :read_operation, fn _, _ -> {:error, error} end)

    assert {:error, {:reload_outcome_unknown, %{reason: ^error}}} =
             ActivationObserver.await(ctx.workspace, "operation", 1_000, opts)

    assert Process.get(:observer_time) == 0
  end

  test "pending timeout cancels once and reports authoritative cleanup", ctx do
    cancel = sequence([{:ok, %{state: :cancelled, cleanup_state: :settled}}])

    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> @pending end,
        cancel: fn workspace, "operation", :startup_timeout ->
          assert workspace == ctx.workspace
          cancel.()
        end
      )

    assert {:error,
            {:deployment_interrupted,
             %{operation_id: "operation", activation: :not_committed, cleanup: :settled}}} =
             ActivationObserver.await(ctx.workspace, "operation", 300, opts)

    assert Process.get(:observer_time) == 300
  end

  test "cancellation receipt is reconciled once even after deadline", ctx do
    reads = sequence([@pending])
    cancel = sequence([@committed])

    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> reads.() end,
        cancel: fn _, _, _ -> cancel.() end
      )

    assert {:ok, @runtime} = ActivationObserver.await(ctx.workspace, "operation", 0, opts)
  end

  test "unreadable cancellation receipt returns unknown without an expired polling loop", ctx do
    error = unavailable()

    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> @pending end,
        cancel: fn _, _, _ -> @committed end,
        read_runtime: fn _ -> {:error, error} end
      )

    assert {:error, {:reload_outcome_unknown, %{reason: ^error}}} =
             ActivationObserver.await(ctx.workspace, "operation", 0, opts)

    assert Process.get(:observer_time) == 0
  end

  test "unknown cancellation stays unknown", ctx do
    for outcome <- [{:error, unavailable()}, {:ok, %{state: :unknown, cleanup_state: :pending}}] do
      opts =
        Keyword.merge(ctx.opts,
          read_operation: fn _, _ -> @pending end,
          cancel: fn _, _, _ -> outcome end
        )

      assert {:error, {:reload_outcome_unknown, %{reason: :operation_wait_timeout}}} =
               ActivationObserver.await(ctx.workspace, "operation", 0, opts)
    end
  end

  test "supersession remains a distinct result", ctx do
    opts =
      Keyword.merge(ctx.opts,
        read_operation: fn _, _ -> @committed end,
        read_runtime: fn _ -> {:ok, %{@runtime | revision: 8}} end
      )

    assert {:error, {:deployment_superseded, "operation"}} =
             ActivationObserver.await(ctx.workspace, "operation", 1_000, opts)
  end

  test "terminal states remain authoritative after a retry", ctx do
    for status <- [:failed, :cancelled, :unknown] do
      reads =
        sequence([
          {:error, unavailable()},
          {:ok, %{state: status, failure_class: :session_expired}}
        ])

      opts = Keyword.put(ctx.opts, :read_operation, fn _, _ -> reads.() end)

      assert {:error, {:deployment_operation, "operation", ^status, :session_expired}} =
               ActivationObserver.await(ctx.workspace, "operation", 1_000, opts)
    end
  end

  test "needs-attention receipt preserves the existing success contract", ctx do
    opts =
      Keyword.put(ctx.opts, :read_operation, fn _, _ ->
        {:ok, %{state: :needs_attention, activation_receipt: @receipt}}
      end)

    assert {:ok, @runtime} = ActivationObserver.await(ctx.workspace, "operation", 0, opts)
  end

  defp unavailable,
    do: Error.new(:unavailable, "database connection unavailable", retryable?: true)

  defp sequence(results) do
    key = make_ref()
    Process.put(key, results)

    fn ->
      assert [result | rest] = Process.get(key), "unexpected extra read or cancellation"
      Process.put(key, rest)
      result
    end
  end
end
