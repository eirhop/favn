defmodule Favn.Dev.Install do
  @moduledoc """
  Project-local install and install-state validation for Favn dev tooling.
  """

  alias Favn.Dev.Paths
  alias Favn.Dev.RuntimeSource
  alias Favn.Dev.RuntimeWorkspace
  alias Favn.Dev.State

  @schema_version 3

  @type root_opt :: [root_dir: Path.t()]

  @spec run(root_opt()) :: {:ok, :installed | :already_installed} | {:error, term()}
  def run(opts \\ []) when is_list(opts) do
    case do_run(opts) do
      {:ok, status} ->
        {:ok, status}

      {:error, reason} = error ->
        _ =
          State.write_last_failure(
            %{
              "command" => "install",
              "error" => inspect(reason),
              "at" => DateTime.utc_now() |> DateTime.to_iso8601()
            },
            opts
          )

        error
    end
  end

  @spec ensure_ready(root_opt()) :: :ok | {:error, term()}
  def ensure_ready(opts \\ []) when is_list(opts) do
    with {:ok, install} <- State.read_install(opts),
         {:ok, runtime} <- State.read_install_runtime(opts),
         true <- File.dir?(runtime["materialized_root"]),
         {:ok, current_fingerprint} <- fingerprint(opts),
         {:ok, stored_fingerprint} <- fetch_fingerprint(install),
         true <- stored_fingerprint == current_fingerprint do
      :ok
    else
      {:error, :not_found} ->
        {:error, :install_required}

      false ->
        {:error, :install_stale}

      {:error, :missing_fingerprint} ->
        {:error, :install_stale}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_run(opts) do
    force? = Keyword.get(opts, :force, false)

    with :ok <- State.ensure_layout(opts),
         {:ok, source} <- RuntimeSource.resolve(opts),
         {:ok, current_fingerprint} <- fingerprint(opts),
         install_decision <- ensure_install_needed(current_fingerprint, force?, opts) do
      maybe_install(install_decision, current_fingerprint, source, opts)
    end
  end

  defp ensure_install_needed(_fingerprint, true, _opts), do: :install

  defp ensure_install_needed(fingerprint, false, opts) do
    case State.read_install(opts) do
      {:ok, install} ->
        with {:ok, stored} <- fetch_fingerprint(install) do
          if stored == fingerprint do
            :already_installed
          else
            :install
          end
        end

      {:error, :not_found} ->
        :install

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp maybe_install(:already_installed, _current_fingerprint, _source, _opts),
    do: {:ok, :already_installed}

  defp maybe_install(:install, current_fingerprint, source, opts) do
    with {:ok, toolchain} <- build_toolchain(opts),
         {:ok, runtime} <- RuntimeWorkspace.materialize(source, opts),
         :ok <- install_runtime_dependencies(runtime, opts),
         :ok <- install_web_dependencies(runtime, opts) do
      with :ok <- write_install_state(current_fingerprint, toolchain, source, runtime, opts),
           do: {:ok, :installed}
    end
  end

  defp maybe_install({:error, _reason} = error, _current_fingerprint, _source, _opts), do: error

  defp build_toolchain(opts) do
    with {:ok, node_version} <- command_version(opts, :node, "node", ["--version"]),
         {:ok, npm_version} <- command_version(opts, :npm, "npm", ["--version"]) do
      {:ok,
       %{
         "schema_version" => @schema_version,
         "captured_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
         "elixir_version" => System.version(),
         "otp_release" => otp_release(),
         "node_version" => node_version,
         "npm_version" => npm_version
       }}
    end
  end

  defp install_runtime_dependencies(runtime, opts) do
    if Keyword.get(opts, :skip_runtime_deps_install, false) do
      :ok
    else
      runtime_root = runtime["materialized_root"]
      runtime_mix_exs = Path.join(runtime_root, "mix.exs")

      if File.exists?(runtime_mix_exs) do
        mix_exec = System.find_executable("mix") || "mix"

        case System.cmd(mix_exec, ["deps.get"], cd: runtime_root, stderr_to_stdout: true) do
          {_output, 0} ->
            :ok

          {output, status} ->
            {:error, {:runtime_deps_install_failed, status, String.trim(output)}}
        end
      else
        :ok
      end
    end
  end

  defp install_web_dependencies(runtime, opts) do
    if Keyword.get(opts, :skip_web_install, false) do
      :ok
    else
      web_dir = runtime["web_root"]
      npm_exec = System.find_executable("npm") || "npm"
      npm_cache = Paths.install_cache_npm_dir(Paths.root_dir(opts))
      package_lock = Path.join(web_dir, "package-lock.json")

      env = %{"npm_config_cache" => npm_cache}

      install_with_fallback(npm_exec, package_lock, npm_cache, web_dir, env)
    end
  end

  defp install_with_fallback(npm_exec, package_lock, npm_cache, web_dir, env) do
    if File.exists?(package_lock) do
      case run_npm_install(npm_exec, ["ci", "--cache", npm_cache], web_dir, env) do
        :ok ->
          :ok

        {:error, _status, _output} ->
          case run_npm_install(npm_exec, ["install", "--cache", npm_cache], web_dir, env) do
            :ok ->
              :ok

            {:error, status, retry_output} ->
              {:error, {:web_install_failed, status, String.trim(retry_output)}}
          end
      end
    else
      case run_npm_install(npm_exec, ["install", "--cache", npm_cache], web_dir, env) do
        :ok -> :ok
        {:error, status, output} -> {:error, {:web_install_failed, status, String.trim(output)}}
      end
    end
  end

  defp run_npm_install(npm_exec, args, web_dir, env) do
    case System.cmd(npm_exec, args, cd: web_dir, stderr_to_stdout: true, env: env) do
      {_output, 0} -> :ok
      {output, status} -> {:error, status, output}
    end
  end

  defp write_install_state(fingerprint, toolchain, source, runtime, opts) do
    install = %{
      "schema_version" => @schema_version,
      "installed_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "fingerprint" => fingerprint,
      "runtime" => %{
        "source_kind" => source.kind |> Atom.to_string(),
        "source_root" => source.root,
        "materialized_root" => runtime["materialized_root"],
        "web_root" => runtime["web_root"],
        "orchestrator_root" => runtime["orchestrator_root"],
        "runner_root" => runtime["runner_root"]
      },
      "toolchain_ref" => "toolchain.json"
    }

    with :ok <- State.write_toolchain(toolchain, opts),
         do: State.write_install(install, opts)
  end

  @spec fingerprint(root_opt()) :: {:ok, map()} | {:error, term()}
  defp fingerprint(opts) do
    with {:ok, source} <- RuntimeSource.resolve(opts),
         {:ok, runtime_fingerprint} <- RuntimeSource.fingerprint(source),
         {:ok, node_version} <- command_version(opts, :node, "node", ["--version"]),
         {:ok, npm_version} <- command_version(opts, :npm, "npm", ["--version"]) do
      root_dir = Paths.root_dir(opts)

      {:ok,
       %{
         "schema_version" => @schema_version,
         "elixir_version" => System.version(),
         "otp_release" => otp_release(),
         "node_version" => node_version,
         "npm_version" => npm_version,
         "consumer_mix_lock_sha256" => file_sha256(Path.join(root_dir, "mix.lock")),
         "runtime_source" => runtime_fingerprint
       }}
    end
  end

  defp command_version(opts, tool, exec, args) do
    if Keyword.get(opts, :skip_tool_checks, false) do
      {:ok, "skipped"}
    else
      command_version_checked(tool, exec, args)
    end
  end

  defp command_version_checked(tool, exec, args) do
    case System.find_executable(exec) do
      nil ->
        {:error, {:missing_tool, tool}}

      executable ->
        case System.cmd(executable, args, stderr_to_stdout: true) do
          {value, 0} -> {:ok, String.trim(value)}
          {output, status} -> {:error, {:tool_check_failed, tool, status, String.trim(output)}}
        end
    end
  end

  defp fetch_fingerprint(%{"fingerprint" => fingerprint}) when is_map(fingerprint),
    do: {:ok, fingerprint}

  defp fetch_fingerprint(_), do: {:error, :missing_fingerprint}

  defp otp_release, do: :erlang.system_info(:otp_release) |> List.to_string()

  defp file_sha256(path) do
    case File.read(path) do
      {:ok, bytes} -> :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
      {:error, :enoent} -> nil
      {:error, _reason} -> nil
    end
  end
end
