defmodule Favn.Runtime.Executor.Local do
  @moduledoc """
  Local asynchronous executor for invoking one asset function.
  """

  @behaviour Favn.Runtime.Executor

  alias Favn.Asset
  alias Favn.Run.Context
  require Logger

  @impl true
  def start_step(%Asset{} = asset, %Context{} = ctx, reply_to, step_ref)
      when is_pid(reply_to) do
    exec_ref = make_ref()

    {pid, monitor_ref} =
      spawn_monitor(fn ->
        Logger.metadata(
          run_id: ctx.run_id,
          ref: inspect(step_ref),
          stage: ctx.stage,
          attempt: ctx.attempt
        )

        result = invoke(asset, ctx)
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

  defp invoke(asset, %Context{} = ctx) do
    entrypoint = asset.entrypoint || asset.name

    case apply(asset.module, entrypoint, [ctx]) do
      :ok ->
        {:ok, %{}}

      {:ok, meta} when is_map(meta) ->
        {:ok, meta}

      {:error, reason} ->
        {:error, %{kind: :error, reason: reason, stacktrace: []}}

      other ->
        {:error,
         %{
           kind: :error,
           reason:
             {:invalid_return_shape, other, expected: ":ok | {:ok, map()} | {:error, reason}"},
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
