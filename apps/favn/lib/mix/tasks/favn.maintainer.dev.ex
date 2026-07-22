defmodule Mix.Tasks.Favn.Maintainer.Dev do
  use Mix.Task

  @shortdoc "Runs Favn locally from the checkout selected by FAVN_CHECKOUT"

  @moduledoc """
  Runs the explicit non-production maintainer development workflow.

  The consuming project's `mix.exs` must select every Favn dependency from the
  checkout named by `FAVN_CHECKOUT`. This task builds or reuses an unpublished
  local control-plane image, selects it by immutable Docker image ID, and then
  starts the normal consumer-owned local Compose deployment. If that exact
  control plane is already running, it applies the normal manifest/runner
  reload instead.

  A running different control plane is never replaced implicitly. Stop the
  foreground `mix favn.dev` process and rerun this task. Official images remain
  available through `mix favn.install` and `mix favn.dev`.
  """

  alias Favn.Dev
  alias Favn.Dev.EnvBootstrap
  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    case EnvBootstrap.exec(:maintainer_dev, args, opts) do
      {:ok, 0} -> :ok
      {:ok, status} -> System.halt(status)
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec run_configured([String.t()]) :: :ok | no_return()
  def run_configured(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    with {:ok, opts} <- EnvBootstrap.consume(:maintainer_dev, opts) do
      run_maintainer(opts)
    else
      {:error, :env_bootstrap_required} ->
        Mix.raise("favn.maintainer.dev.configured is internal; run mix favn.maintainer.dev")

      {:error, reason} ->
        Mix.raise(
          "invalid favn.maintainer.dev environment bootstrap: #{inspect(reason)}; " <>
            "run mix favn.maintainer.dev"
        )
    end
  end

  @doc false
  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) when is_list(args) do
    CLIArgs.parse_no_args!("favn.maintainer.dev", args,
      root_dir: :string,
      scheduler: :boolean,
      compose_file: :string
    )
  end

  defp run_maintainer(opts) do
    case Dev.maintainer_dev(opts) do
      :ok -> :ok
      {:error, reason} -> Mix.raise(error_message(reason))
    end
  end

  defp error_message(:maintainer_checkout_required) do
    "maintainer development requires FAVN_CHECKOUT to select the local Favn repository; " <>
      "load it before Mix starts (for example with direnv and .env.local)"
  end

  defp error_message({:maintainer_dependency_mismatch, app, expected, actual}) do
    "maintainer dependency #{inspect(app)} does not come from FAVN_CHECKOUT; " <>
      "expected #{expected}, got #{inspect(actual)}. Update mix.exs and run mix deps.get"
  end

  defp error_message({:maintainer_environment_forbidden, environment}) do
    "maintainer development is available only in MIX_ENV=dev, not #{inspect(environment)}"
  end

  defp error_message({:maintainer_restart_required, _images}) do
    "the running stack uses a different control-plane image; stop the foreground " <>
      "mix favn.dev or mix favn.maintainer.dev process, then rerun mix favn.maintainer.dev"
  end

  defp error_message({:lock_failed, :timeout}) do
    "another Favn lifecycle command is active for this project; retry after it exits"
  end

  defp error_message(reason),
    do: "maintainer development failed: #{inspect(reason)}"
end
