defmodule FavnOrchestrator.ManifestDeploymentReservation do
  @moduledoc false

  alias FavnOrchestrator.ManifestStore
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @default_heartbeat_interval_ms 15_000

  @doc false
  @spec run(
          WorkspaceContext.t(),
          CommandIdempotency.t() | nil,
          (-> result),
          keyword()
        ) :: result
        when result: term()
  def run(context, idempotency, deploy, opts \\ [])

  def run(_context, nil, deploy, _opts) when is_function(deploy, 0), do: deploy.()

  def run(context, idempotency, deploy, opts)
      when is_function(deploy, 0) and is_list(opts) do
    owner = self()
    stop_ref = make_ref()
    interval_ms = Keyword.get(opts, :heartbeat_interval_ms, @default_heartbeat_interval_ms)

    heartbeat =
      spawn(fn ->
        owner_ref = Process.monitor(owner)
        heartbeat_loop(owner_ref, stop_ref, context, idempotency, interval_ms)
      end)

    try do
      deploy.()
    after
      send(heartbeat, {:stop, stop_ref})
      _ = ManifestStore.abandon_manifest_deployment(context, idempotency)
    end
  end

  defp heartbeat_loop(owner_ref, stop_ref, context, idempotency, interval_ms) do
    receive do
      {:stop, ^stop_ref} ->
        Process.demonitor(owner_ref, [:flush])
        :ok

      {:DOWN, ^owner_ref, :process, _owner, _reason} ->
        :ok
    after
      interval_ms ->
        _ = ManifestStore.heartbeat_manifest_deployment(context, idempotency)
        heartbeat_loop(owner_ref, stop_ref, context, idempotency, interval_ms)
    end
  end
end
