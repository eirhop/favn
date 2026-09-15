defmodule FavnStoragePostgres.Maintenance.Worker do
  @moduledoc false
  use GenServer
  require Logger
  alias FavnOrchestrator.Persistence.Commands.RetentionBatch
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Retention.Policy
  alias FavnStoragePostgres.Maintenance.Store

  def start_link(options), do: GenServer.start_link(__MODULE__, options, name: __MODULE__)

  @impl true
  def init(options) do
    {:ok, policy} =
      case System.get_env("FAVN_RETENTION_POLICY_FILE") do
        nil ->
          Policy.new(Keyword.get(options, :policy, %Policy{}))

        path ->
          with {:ok, bytes} <- File.read(path),
               {:ok, value} <- Jason.decode(bytes),
               {:ok, policy} <- Policy.decode(value),
               do: {:ok, policy}
      end

    {:ok, context} = PlatformContext.new("retention-worker", "runtime", [:platform_operator])
    state = %{policy: policy, context: context, task: nil, last_warning: nil}
    schedule(policy)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, %{task: nil} = state) do
    task =
      Task.Supervisor.async_nolink(FavnStoragePostgres.Maintenance.Tasks, fn ->
        with {:ok, status} <- Store.retention_status(state.context) do
          Store.retention_batch(%RetentionBatch{
            platform_context: state.context,
            policy: state.policy,
            expected_version: status.version,
            scheduled?: true
          })
        end
      end)

    {:noreply, %{state | task: task}}
  end

  def handle_info(:tick, state), do: {:noreply, state}

  def handle_info({ref, result}, %{task: %{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    state = report(result, state)
    schedule(state.policy)
    {:noreply, %{state | task: nil}}
  end

  def handle_info({:DOWN, ref, :process, _, _reason}, %{task: %{ref: ref}} = state) do
    state = report({:error, :worker_failed}, state)
    schedule(state.policy)
    {:noreply, %{state | task: nil}}
  end

  defp report({:ok, _}, state), do: %{state | last_warning: nil}

  defp report({:error, error}, state) do
    now = System.monotonic_time(:millisecond)

    if is_nil(state.last_warning) or now - state.last_warning >= 60_000 do
      reason = failure_kind(error)

      Logger.warning("retention batch failed; check policy and PostgreSQL availability",
        retention_reason: reason
      )

      :telemetry.execute([:favn, :retention, :failure], %{count: 1}, %{reason: reason})
      %{state | last_warning: now}
    else
      state
    end
  end

  defp failure_kind(%{kind: kind})
       when kind in [:conflict, :invalid, :unavailable, :constraint, :internal], do: kind

  defp failure_kind(:invalid_retention_policy), do: :invalid
  defp failure_kind(_), do: :worker_failed

  defp schedule(policy) do
    Process.send_after(self(), :tick, policy.interval_ms + :rand.uniform(1_000))
  end
end
