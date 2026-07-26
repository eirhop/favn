defmodule FavnOrchestrator.RunSubmission.Supervisor do
  @moduledoc """
  Supervised bounded preparation and admission for durable run submissions.

  PostgreSQL owns queue and lease truth. This subtree owns only process-local
  concurrency, fair workspace rotation, and temporary worker lifetimes.
  """

  use Supervisor

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.RunSubmission.Coordinator

  @defaults [
    global_concurrency: 8,
    per_workspace_concurrency: 2,
    workspace_page_size: 100,
    poll_interval_ms: 250,
    lease_duration_ms: 30_000,
    renewal_interval_ms: 10_000,
    max_attempts: 5,
    retry_backoff_ms: 1_000
  ]

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) when is_list(opts) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    config =
      @defaults
      |> Keyword.merge(Keyword.get(opts, :config, []))
      |> validate!()

    task_supervisor =
      Keyword.get(
        opts,
        :task_supervisor,
        FavnOrchestrator.RunSubmission.TaskSupervisor
      )

    coordinator =
      Keyword.get(opts, :coordinator, FavnOrchestrator.RunSubmission.Coordinator)

    store = Keyword.get_lazy(opts, :store, fn -> Persistence.stores().run_submissions end)

    worker_options =
      Keyword.take(config, [
        :lease_duration_ms,
        :renewal_interval_ms,
        :max_attempts,
        :retry_backoff_ms
      ]) ++ Keyword.get(opts, :worker_options, [])

    children = [
      Supervisor.child_spec(
        {Task.Supervisor,
         name: task_supervisor, max_children: Keyword.fetch!(config, :global_concurrency)},
        id: :run_submission_tasks,
        shutdown: Keyword.fetch!(config, :lease_duration_ms)
      ),
      Supervisor.child_spec(
        {Coordinator,
         name: coordinator,
         task_supervisor: task_supervisor,
         store: store,
         lifecycle: Keyword.get(opts, :lifecycle, FavnOrchestrator.Lifecycle),
         worker: Keyword.get(opts, :worker, FavnOrchestrator.RunSubmission.Worker),
         worker_options: worker_options,
         global_concurrency: Keyword.fetch!(config, :global_concurrency),
         per_workspace_concurrency: Keyword.fetch!(config, :per_workspace_concurrency),
         workspace_page_size: Keyword.fetch!(config, :workspace_page_size),
         poll_interval_ms: Keyword.fetch!(config, :poll_interval_ms)},
        id: :run_submission_coordinator
      )
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  @doc false
  @spec defaults() :: keyword()
  def defaults, do: @defaults

  defp validate!(config) do
    expected = Keyword.keys(@defaults)

    valid? =
      Keyword.keyword?(config) and Keyword.keys(config) -- expected == [] and
        positive_in?(config, :global_concurrency, 1..256) and
        positive_in?(config, :per_workspace_concurrency, 1..256) and
        Keyword.fetch!(config, :per_workspace_concurrency) <=
          Keyword.fetch!(config, :global_concurrency) and
        positive_in?(config, :workspace_page_size, 1..200) and
        positive_in?(config, :poll_interval_ms, 10..60_000) and
        positive_in?(config, :lease_duration_ms, 3_000..3_600_000) and
        positive_in?(config, :renewal_interval_ms, 100..1_200_000) and
        Keyword.fetch!(config, :renewal_interval_ms) <
          Keyword.fetch!(config, :lease_duration_ms) and
        positive_in?(config, :max_attempts, 1..50) and
        positive_in?(config, :retry_backoff_ms, 1..60_000)

    if valid?, do: config, else: raise(ArgumentError, "invalid run submission worker config")
  end

  defp positive_in?(config, key, range) do
    value = Keyword.get(config, key)
    is_integer(value) and value in range
  end
end
