defmodule Favn.Azure.Credentials.Supervisor do
  @moduledoc false

  use Supervisor

  alias Favn.Azure.Credentials.Cache

  @task_supervisor Favn.Azure.Credentials.TaskSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :supervisor_name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    task_supervisor = Keyword.get(opts, :task_supervisor, @task_supervisor)
    cache = Keyword.get(opts, :cache_name, Cache)

    cache_opts =
      opts
      |> Keyword.take([
        :refresh_before_seconds,
        :fetch_timeout,
        :max_entries,
        :max_inflight,
        :max_waiters_per_key,
        :clock
      ])
      |> Keyword.merge(name: cache, task_supervisor: task_supervisor)

    children = [
      {Task.Supervisor, name: task_supervisor},
      {Cache, cache_opts}
    ]

    # Cache state owns the logical in-flight set. Restart the task supervisor
    # with it so provider tasks cannot outlive a cache crash and escape bounds.
    Supervisor.init(children, strategy: :one_for_all)
  end
end
