defmodule Favn.Runtime.Executor.Local do
  @moduledoc """
  Local task-based executor for invoking one asset function.
  """

  @behaviour Favn.Runtime.Executor

  alias Favn.Asset
  alias Favn.Asset.Output
  alias Favn.Run.Context

  @impl true
  def execute_step(%Asset{} = asset, %Context{} = ctx, deps) when is_map(deps) do
    task = Task.async(fn -> invoke(asset, ctx, deps) end)
    Task.await(task, :infinity)
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
