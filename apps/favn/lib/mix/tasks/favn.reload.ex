defmodule Mix.Tasks.Favn.Reload do
  use Mix.Task

  @shortdoc "Rebuilds and reloads manifest into running local stack"

  @moduledoc """
  Recompiles the project, rebuilds the manifest, publishes it to orchestrator,
  and activates it without restarting orchestrator. The project's `.env` is
  loaded before the consumer project's `config/runtime.exs` is reevaluated.
  """

  alias Favn.Dev
  alias Favn.Dev.EnvBootstrap
  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    opts = parse_args(args)

    case EnvBootstrap.exec(:reload, args, opts) do
      {:ok, 0} ->
        :ok

      {:ok, status} ->
        System.halt(status)

      {:error, reason} ->
        Mix.raise("reload failed: #{inspect(reason)}")
    end
  end

  @doc false
  @spec run_configured([String.t()]) :: :ok | no_return()
  def run_configured(args) do
    opts = parse_args(args)

    with {:ok, opts} <- EnvBootstrap.consume(:reload, opts) do
      run_reload(opts)
    else
      {:error, :env_bootstrap_required} ->
        Mix.raise("favn.reload.configured is an internal task; run mix favn.reload")

      {:error, reason} ->
        Mix.raise(
          "invalid favn.reload environment bootstrap: #{inspect(reason)}; run mix favn.reload"
        )
    end
  end

  @doc false
  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) when is_list(args),
    do: CLIArgs.parse_no_args!("favn.reload", args, root_dir: :string)

  defp run_reload(opts) do
    case Dev.reload(opts) do
      :ok ->
        :ok

      {:error, :stack_not_running} ->
        Mix.raise("stack not running; use mix favn.dev")

      {:error, :stack_not_healthy} ->
        Mix.raise("stack not healthy; use mix favn.stop then mix favn.dev")

      {:error, {:in_flight_runs, run_ids}} ->
        Mix.raise(in_flight_runs_message(run_ids))

      {:error, reason} ->
        Mix.raise("reload failed: #{inspect(reason)}")
    end
  end

  @doc false
  def in_flight_runs_message(run_ids) do
    "reload blocked: in-flight runs exist #{inspect(run_ids)}\n" <>
      "wait for the runs to finish, or cancel them with mix favn.runs cancel RUN_ID or from the Favn UI before retrying.\n" <>
      "if these runs are stale after a crashed local stack, run mix favn.stop then mix favn.dev; " <>
      "if they still remain stale, reset local state with mix favn.reset and restart with mix favn.dev."
  end
end
