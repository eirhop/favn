defmodule FavnOrchestrator.ManifestActivationLease do
  @moduledoc false

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.AcquireManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.ReleaseManifestActivationLease
  alias FavnOrchestrator.Persistence.Commands.RenewManifestActivationLease
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RuntimeConfig

  @lease_seconds 45
  @heartbeat_ms 15_000

  @spec run(WorkspaceContext.t(), String.t(), (map() -> term())) :: term()
  def run(%WorkspaceContext{} = context, operation_id, fun)
      when is_binary(operation_id) and is_function(fun, 1) do
    owner =
      "#{RuntimeConfig.instance_id()}:activation:#{System.unique_integer([:positive, :monotonic])}"

    now = DateTime.utc_now()

    command = %AcquireManifestActivationLease{
      workspace_context: context,
      operation_id: operation_id,
      owner: owner,
      occurred_at: now,
      expires_at: DateTime.add(now, @lease_seconds, :second)
    }

    case Persistence.stores().registry.acquire_manifest_activation_lease(command) do
      {:ok, fence} -> execute(context, operation_id, owner, fence, fun)
      {:error, _reason} = error -> error
    end
  end

  defp execute(context, operation_id, owner, fence, fun) do
    caller = self()
    stop_ref = make_ref()

    heartbeat =
      spawn(fn ->
        caller_ref = Process.monitor(caller)
        heartbeat(caller_ref, stop_ref, context, operation_id, owner, fence)
      end)

    try do
      fun.(%{owner: owner, fence: fence})
    after
      send(heartbeat, {:stop, stop_ref})

      _ =
        Persistence.stores().registry.release_manifest_activation_lease(
          %ReleaseManifestActivationLease{
            workspace_context: context,
            operation_id: operation_id,
            owner: owner,
            fence: fence
          }
        )
    end
  end

  defp heartbeat(caller_ref, stop_ref, context, operation_id, owner, fence) do
    receive do
      {:stop, ^stop_ref} ->
        Process.demonitor(caller_ref, [:flush])
        :ok

      {:DOWN, ^caller_ref, :process, _pid, _reason} ->
        :ok
    after
      @heartbeat_ms ->
        _ =
          Persistence.stores().registry.renew_manifest_activation_lease(
            %RenewManifestActivationLease{
              workspace_context: context,
              operation_id: operation_id,
              owner: owner,
              fence: fence,
              expires_at: DateTime.add(DateTime.utc_now(), @lease_seconds, :second)
            }
          )

        heartbeat(caller_ref, stop_ref, context, operation_id, owner, fence)
    end
  end
end
