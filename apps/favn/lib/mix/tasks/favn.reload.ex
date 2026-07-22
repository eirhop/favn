defmodule Mix.Tasks.Favn.Reload do
  use Mix.Task

  @shortdoc "Rebuilds and reloads manifest into running local stack"

  @moduledoc """
  Rebuilds the customer release contract and applies one Docker Compose change.

  Manifest-only changes publish and activate without restarting containers.
  Runner changes first persist the verified runner/manifest pair, acquire a
  recoverable maintenance lease, drain admitted work, replace and verify only
  the runner, and then activate the aligned manifest. Rollback restores
  admission only after both the previous runner and active manifest are
  verified.
  The project's `.env` is loaded before the consumer `config/runtime.exs` is
  evaluated for the production runner build.
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

      {:error, :install_required} ->
        Mix.raise("reload failed: install required; run mix favn.install then mix favn.dev")

      {:error, {:lock_failed, :timeout}} ->
        Mix.raise("reload failed: another Favn lifecycle command is active; retry after it exits")

      {:error, {:in_flight_runs, run_ids}} ->
        Mix.raise(in_flight_runs_message(run_ids))

      {:error, {:runner_replacement_rollback_failed, reason, rollback_reason}} ->
        Mix.raise(
          "reload failed and the previous runner could not be verified after rollback; " <>
            "maintenance remains active so fix the Docker runner failure and retry: " <>
            "#{inspect({reason, rollback_reason})}"
        )

      {:error, {:runner_replacement_finish_failed, reason, finish_reason}} ->
        Mix.raise(
          "reload changed or restored the runner but maintenance completion was not confirmed; " <>
            "retry to resume the persisted lease: #{inspect({reason, finish_reason})}"
        )

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
