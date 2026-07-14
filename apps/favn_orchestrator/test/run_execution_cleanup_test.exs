defmodule FavnOrchestrator.RunExecutionCleanupTest do
  use ExUnit.Case, async: false

  alias FavnOrchestrator.RunExecutionCleanup
  alias FavnOrchestrator.Storage.Adapter.Memory

  defmodule ReleaseFailingAdapter do
    @moduledoc false
    @behaviour Favn.Storage.Adapter

    callbacks =
      Favn.Storage.Adapter.behaviour_info(:callbacks) --
        Favn.Storage.Adapter.behaviour_info(:optional_callbacks)

    for {name, arity} <- callbacks, name != :release_execution_leases_for_run do
      args = Macro.generate_arguments(arity, __MODULE__)

      @impl true
      def unquote(name)(unquote_splicing(args)) do
        apply(Memory, unquote(name), [unquote_splicing(args)])
      end
    end

    @impl true
    def release_execution_leases_for_run(_run_id, _opts), do: {:error, :storage_unavailable}
  end

  setup do
    previous_adapter = Application.get_env(:favn_orchestrator, :storage_adapter)
    previous_opts = Application.get_env(:favn_orchestrator, :storage_adapter_opts)

    on_exit(fn ->
      restore_env(:storage_adapter, previous_adapter)
      restore_env(:storage_adapter_opts, previous_opts)
    end)

    :ok
  end

  test "terminal cleanup does not crash after an admission release failure" do
    Application.put_env(:favn_orchestrator, :storage_adapter, ReleaseFailingAdapter)
    Application.put_env(:favn_orchestrator, :storage_adapter_opts, [])

    assert :ok = RunExecutionCleanup.release_admission("run_terminal")
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_orchestrator, key)
  defp restore_env(key, value), do: Application.put_env(:favn_orchestrator, key, value)
end
