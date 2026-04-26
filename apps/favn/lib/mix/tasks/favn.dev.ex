defmodule Mix.Tasks.Favn.Dev do
  use Mix.Task

  @dialyzer {:nowarn_function, run: 1}

  @shortdoc "Starts local Favn dev stack"

  @moduledoc """
  Starts local `favn_web + favn_orchestrator + favn_runner` in foreground mode.
  """

  alias Favn.Dev

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args, strict: [root_dir: :string, sqlite: :boolean, postgres: :boolean])

    opts = normalize_storage_flags(opts)

    case Dev.dev(opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Mix.raise(error_message(reason))
    end
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
      "#{service} exited during startup (status=#{status}); inspect .favn/logs/#{service}.log and check for stale state or port conflicts"

  defp error_message(reason), do: "failed to start local stack: #{inspect(reason)}"

  defp normalize_storage_flags(opts) do
    sqlite? = Keyword.get(opts, :sqlite, false)
    postgres? = Keyword.get(opts, :postgres, false)

    opts = opts |> Keyword.delete(:sqlite) |> Keyword.delete(:postgres)

    cond do
      sqlite? and postgres? -> Mix.raise("choose only one storage flag: --sqlite or --postgres")
      sqlite? -> Keyword.put(opts, :storage, :sqlite)
      postgres? -> Keyword.put(opts, :storage, :postgres)
      true -> opts
    end
  end
end
