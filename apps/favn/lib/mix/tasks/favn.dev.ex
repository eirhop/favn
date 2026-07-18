defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Starts local Favn dev stack"

  @moduledoc """
  Starts local `favn_view + favn_orchestrator + favn_runner` in foreground mode.

  The project's `.env` is loaded before the consumer project's
  `config/runtime.exs` is evaluated. Local runtime configuration is then
  collected for the runner.
  """

  alias Favn.Dev
  alias Favn.Dev.EnvBootstrap
  alias Mix.Tasks.Favn.CLIArgs

  @requirements ["loadpaths"]

  @impl Mix.Task
  def run(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    case EnvBootstrap.exec(:dev, args, opts) do
      {:ok, 0} ->
        :ok

      {:ok, status} ->
        System.halt(status)

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec run_configured([String.t()]) :: :ok | no_return()
  def run_configured(args) do
    opts = args |> parse_args() |> Keyword.put(:progress_fun, &IO.puts/1)

    with {:ok, opts} <- EnvBootstrap.consume(:dev, opts) do
      run_dev(opts)
    else
      {:error, reason} -> Mix.raise(configured_error_message(reason))
    end
  end

  defp run_dev(opts) do
    case Dev.dev(opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
  end

  @doc false
  @spec parse_args([String.t()]) :: keyword()
  def parse_args(args) when is_list(args) do
    opts =
      CLIArgs.parse_no_args!("favn.dev", args,
        root_dir: :string,
        scheduler: :boolean
      )

    opts
  end

  defp error_message(:stack_already_running), do: "local stack already running"

  defp error_message(:install_required), do: "install required; run mix favn.install"

  defp error_message(:install_stale),
    do: "install stale; run mix favn.install to refresh, or mix favn.install --force to rebuild"

  defp error_message({:stack_partially_running, service_states}) do
    details =
      service_states
      |> Enum.map_join(", ", fn {service, state} -> "#{service}=#{state}" end)

    "local stack is in a partial/dead state (#{details}); run mix favn.stop to clean up before retrying"
  end

  defp error_message({:missing_tool, tool}),
    do: "missing required tool #{tool}; run mix favn.install after tool is available"

  defp error_message({:tool_check_failed, tool, status, output}),
    do: "required tool #{tool} check failed (status=#{status}): #{output}; rerun mix favn.install"

  defp error_message({:port_conflict, service, port}),
    do: "port conflict: #{service} cannot bind port #{port}; free the port and retry"

  defp error_message({:port_check_failed, service, port, reason}),
    do:
      "port check failed for #{service} on #{port}: #{inspect(reason)}; verify local networking and retry"

  defp error_message({:postgres_misconfigured, field}),
    do: "postgres configuration missing #{field}; fix config :favn, :local and retry"

  defp error_message({:postgres_unavailable, host, port, reason}),
    do:
      "postgres unavailable at #{host}:#{port} (#{inspect(reason)}); start postgres or fix config and retry"

  defp error_message({:runtime_compile_failed, app, status, output}),
    do:
      "runtime compile failed for #{app} under --root-dir (status=#{inspect(status)}): #{output}; ensure runtime root is current and compilable"

  defp error_message({:runner_manifest_register_unavailable, runner_node, attempted}) do
    details =
      attempted
      |> Enum.map_join(", ", fn %{module: module, function: function, arity: arity} ->
        "#{inspect(module)}.#{function}/#{arity}"
      end)

    "runner bootstrap contract mismatch on #{inspect(runner_node)}; none of [#{details}] are exported on the live runner node"
  end

  defp error_message({:web_build_failed, status, output}),
    do: "web build failed (status=#{status}): #{output}"

  defp error_message({:shortname_host_unavailable, reason}),
    do:
      "local Erlang shortname host is unavailable (#{inspect(reason)}); verify local hostname setup and retry"

  defp error_message(:shortname_host_not_available),
    do: "could not derive local Erlang shortname host; verify local hostname setup and retry"

  defp error_message({:invalid_shortname_host, host}),
    do:
      "local host '#{host}' is invalid for Erlang shortnames; use a short hostname or fix local host resolution and retry"

  defp error_message({:service_exit, service, status}),
    do:
      "#{service} exited during startup (status=#{inspect(status)}); inspect .favn/logs/#{service}.log and check for stale state or port conflicts"

  defp error_message(reason), do: "failed to start local stack: #{inspect(reason)}"

  defp configured_error_message(:env_bootstrap_required),
    do: "favn.dev.configured is an internal task; run mix favn.dev"

  defp configured_error_message(reason),
    do: "invalid favn.dev environment bootstrap: #{inspect(reason)}; run mix favn.dev"
end
