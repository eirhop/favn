defmodule Favn.Runtime.Executor.Local do
  @moduledoc """
  Local asynchronous executor for invoking one asset function.
  """

  @behaviour Favn.Runtime.Executor

  alias Favn.Asset
  alias Favn.Asset.Output
  alias Favn.Run.Context

  @impl true
  def start_step(%Asset{} = asset, %Context{} = ctx, deps, reply_to, step_ref)
      when is_map(deps) and is_pid(reply_to) do
    exec_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        result = invoke(asset, ctx, deps)
        send(reply_to, {:executor_step_result, exec_ref, step_ref, result})
      end)

    {:ok, %{exec_ref: exec_ref, monitor_ref: monitor_ref, pid: pid}}
  end

  @impl true
  def cancel_step(%{pid: pid}, _reason) when is_pid(pid) do
    Process.exit(pid, :kill)
    :ok
  rescue
    error -> {:error, error}
  end

  defp invoke(asset, %Context{} = ctx, deps) do
    try do
      case apply(asset.module, asset.name, [ctx, deps]) do
        {:ok, %Output{} = asset_output} ->
          {:ok, %{output: asset_output.output, meta: asset_output.meta}}

        {:error, reason} ->
          {:error, %{kind: :error, reason: reason, stacktrace: []}}

        other ->
          {:error,
           %{
             kind: :error,
             reason:
               {:invalid_return_shape, other,
                expected: "{:ok, %Favn.Asset.Output{}} | {:error, reason}"},
             stacktrace: []
           }}
      end
    rescue
      error ->
        {:error,
         %{
           kind: :error,
           reason: error,
           stacktrace: __STACKTRACE__,
           message: Exception.message(error)
         }}
    catch
      :throw, reason -> {:error, %{kind: :throw, reason: reason, stacktrace: __STACKTRACE__}}
      :exit, reason -> {:error, %{kind: :exit, reason: reason, stacktrace: __STACKTRACE__}}
    end
  end
end
