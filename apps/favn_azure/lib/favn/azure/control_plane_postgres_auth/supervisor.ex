defmodule Favn.Azure.ControlPlanePostgresAuth.Supervisor do
  @moduledoc false

  use Supervisor

  alias Favn.Azure.ControlPlanePostgresAuth.Server
  alias Favn.Azure.Credentials.Supervisor, as: CredentialsSupervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(options) do
    {name, options} = Keyword.pop(options, :supervisor_name)
    Supervisor.start_link(__MODULE__, options, name: name)
  end

  @impl true
  def init(options) do
    task_supervisor = Keyword.fetch!(options, :task_supervisor)
    cache_name = Keyword.fetch!(options, :cache_name)
    credentials_supervisor_name = Keyword.fetch!(options, :credentials_supervisor_name)
    server_name = Keyword.fetch!(options, :server_name)

    credential_options =
      options
      |> Keyword.take([
        :refresh_before_seconds,
        :fetch_timeout,
        :max_entries,
        :max_inflight,
        :max_waiters_per_key,
        :clock
      ])
      |> Keyword.merge(
        supervisor_name: credentials_supervisor_name,
        task_supervisor: task_supervisor,
        cache_name: cache_name
      )

    server_options =
      options
      |> Keyword.take([
        :client_id,
        :endpoint,
        :credential_provider,
        :provider_options,
        :minimum_validity_seconds,
        :fetch_timeout,
        :clock
      ])
      |> Keyword.merge(name: server_name, cache_name: cache_name)

    Supervisor.init(
      [
        {CredentialsSupervisor, credential_options},
        {Server, server_options}
      ],
      strategy: :one_for_all
    )
  end
end
