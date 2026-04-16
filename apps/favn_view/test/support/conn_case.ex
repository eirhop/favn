defmodule FavnViewWeb.ConnCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias FavnOrchestrator.Storage.Adapter.Memory

  using do
    quote do
      @endpoint FavnView.Endpoint

      use Phoenix.VerifiedRoutes,
        endpoint: FavnView.Endpoint,
        router: FavnViewWeb.Router,
        statics: FavnView.static_paths()

      import Plug.Conn
      import Phoenix.ConnTest
      import Phoenix.LiveViewTest
    end
  end

  setup _tags do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)
    previous_runner_client = Application.get_env(:favn_orchestrator, :runner_client)
    previous_runner_opts = Application.get_env(:favn_orchestrator, :runner_client_opts)

    Application.put_env(:favn_orchestrator, :storage_adapter, Memory)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])
    Application.put_env(:favn_orchestrator, :runner_client, nil)
    Application.put_env(:favn_orchestrator, :runner_client_opts, [])

    Memory.reset()

    on_exit(fn ->
      await_run_supervisor_idle()

      restore_env(:favn_orchestrator, :storage_adapter, previous_adapter)
      restore_env(:favn_orchestrator, :storage_adapter_opts, previous_opts)
      restore_env(:favn_orchestrator, :runner_client, previous_runner_client)
      restore_env(:favn_orchestrator, :runner_client_opts, previous_runner_opts)

      Memory.reset()
    end)

    {:ok, conn: Phoenix.ConnTest.build_conn()}
  end

  defp await_run_supervisor_idle(attempts \\ 100)

  defp await_run_supervisor_idle(0), do: :ok

  defp await_run_supervisor_idle(attempts) do
    case Process.whereis(FavnOrchestrator.RunSupervisor) do
      nil ->
        :ok

      pid when is_pid(pid) ->
        if DynamicSupervisor.count_children(pid).active == 0 do
          :ok
        else
          Process.sleep(20)
          await_run_supervisor_idle(attempts - 1)
        end
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)
end
