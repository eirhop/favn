defmodule FavnRunner do
  @moduledoc """
  Runtime runner boundary facade for manifest-pinned execution.

  `FavnRunner` implements the runner client contract used by the orchestrator
  and plugin/runtime integrations. It is not an ordinary stable authoring API.
  """

  @behaviour Favn.Contracts.RunnerClient

  alias Favn.Contracts.RelationInspectionRequest
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerCancellation
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnRunner.Server

  @type execution_id :: String.t()

  @doc """
  Reports whether the runner server process is available.

  This is a local process availability check only. It does not submit work or
  validate runtime dependencies behind the runner boundary.
  """
  @spec readiness() :: :ok | {:error, :runner_not_available}
  def readiness do
    case Process.whereis(Server) do
      pid when is_pid(pid) -> :ok
      nil -> {:error, :runner_not_available}
    end
  end

  @doc """
  Returns redacted runner availability diagnostics.
  """
  @spec diagnostics(keyword()) :: {:ok, map()} | {:error, term()}
  def diagnostics(opts \\ []) when is_list(opts) do
    Server.diagnostics(opts)
  end

  @doc """
  Registers one pinned manifest version in the runner.
  """
  @spec register_manifest(Version.t(), keyword()) :: :ok | {:error, term()}
  def register_manifest(version, opts \\ [])

  def register_manifest(%Version{} = version, opts) when is_list(opts),
    do: Server.register_manifest(version, opts)

  @doc """
  Submits one manifest-pinned work request for asynchronous execution.
  """
  @spec submit_work(RunnerWork.t(), keyword()) :: {:ok, execution_id()} | {:error, term()}
  def submit_work(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    Server.submit_work(work, opts)
  end

  @doc """
  Waits for one execution result.
  """
  @spec await_result(execution_id(), timeout(), keyword()) ::
          {:ok, RunnerResult.t()} | {:error, term()}
  def await_result(execution_id, timeout \\ 5_000, opts \\ [])

  def await_result(execution_id, timeout, opts)
      when is_binary(execution_id) and is_integer(timeout) and timeout > 0 and is_list(opts) do
    Server.await_result(execution_id, timeout, opts)
  end

  def await_result(_execution_id, _timeout, _opts), do: {:error, :invalid_await_args}

  @doc """
  Cancels one in-flight execution.
  """
  @spec cancel_work(execution_id(), RunnerCancellation.t(), keyword()) ::
          {:ok, RunnerCancellation.outcome()} | {:error, RunnerError.t()}
  def cancel_work(execution_id, reason \\ %{}, opts \\ [])

  def cancel_work(execution_id, reason, opts)
      when is_binary(execution_id) and is_map(reason) and is_list(opts) do
    Server.cancel_work(execution_id, RunnerCancellation.from_map(reason), opts)
  end

  def cancel_work(_execution_id, _reason, _opts) do
    {:error,
     RunnerError.normalize(:invalid_cancel_args,
       kind: :boundary,
       type: :invalid_cancel_args,
       retryable?: false
     )}
  end

  @doc """
  Subscribes a process to live logs for one runner execution.
  """
  @spec subscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok | {:error, term()}
  def subscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def subscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    Server.subscribe_execution_logs(execution_id, subscriber, opts)
  end

  def subscribe_execution_logs(_execution_id, _subscriber, _opts),
    do: {:error, :invalid_log_subscription_args}

  @doc """
  Unsubscribes a process from live logs for one runner execution.
  """
  @spec unsubscribe_execution_logs(execution_id(), pid(), keyword()) :: :ok
  def unsubscribe_execution_logs(execution_id, subscriber, opts \\ [])

  def unsubscribe_execution_logs(execution_id, subscriber, opts)
      when is_binary(execution_id) and is_pid(subscriber) and is_list(opts) do
    Server.unsubscribe_execution_logs(execution_id, subscriber, opts)
  end

  def unsubscribe_execution_logs(_execution_id, _subscriber, _opts), do: :ok

  @doc """
  Runs one safe read-only relation inspection request through the runner boundary.
  """
  @spec inspect_relation(RelationInspectionRequest.t(), keyword()) ::
          {:ok, RelationInspectionResult.t()} | {:error, term()}
  def inspect_relation(%RelationInspectionRequest{} = request, opts \\ []) when is_list(opts) do
    Server.inspect_relation(request, opts)
  end

  @doc """
  Runs one work request synchronously through the same runner server boundary.
  """
  @spec run(RunnerWork.t(), keyword()) :: {:ok, RunnerResult.t()} | {:error, term()}
  def run(%RunnerWork{} = work, opts \\ []) when is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    with {:ok, execution_id} <- submit_work(work, opts) do
      await_result(execution_id, timeout, opts)
    end
  end
end
