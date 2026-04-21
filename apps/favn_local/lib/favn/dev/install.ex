defmodule Favn.Dev.Install do
  @moduledoc """
  Project-local install and install-state validation for Favn dev tooling.
  """

  alias Favn.Dev.Paths
  alias Favn.Dev.State

  @schema_version 1

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
         {:ok, current_fingerprint} <- fingerprint(opts),
         install_decision <- ensure_install_needed(current_fingerprint, force?, opts) do
      maybe_install(install_decision, current_fingerprint, opts)
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

  defp maybe_install(:already_installed, _current_fingerprint, _opts),
    do: {:ok, :already_installed}

  defp maybe_install(:install, current_fingerprint, opts) do
    with {:ok, toolchain} <- build_toolchain(opts),
         :ok <- install_web_dependencies(opts) do
      with :ok <- write_install_state(current_fingerprint, toolchain, opts), do: {:ok, :installed}
    end
  end

  defp maybe_install({:error, _reason} = error, _current_fingerprint, _opts), do: error

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

  defp install_web_dependencies(opts) do
    if Keyword.get(opts, :skip_web_install, false) do
      :ok
    else
      web_dir = web_root(opts)
      npm_exec = System.find_executable("npm") || "npm"
      npm_cache = Paths.install_cache_npm_dir(Paths.root_dir(opts))
      package_lock = Path.join(web_dir, "package-lock.json")

      args =
        if File.exists?(package_lock) do
          ["ci", "--silent", "--cache", npm_cache]
        else
          ["install", "--silent", "--cache", npm_cache]
        end

      env = %{"npm_config_cache" => npm_cache}

      case System.cmd(npm_exec, args, cd: web_dir, stderr_to_stdout: true, env: env) do
        {_output, 0} ->
          :ok

        {output, status} ->
          {:error, {:web_install_failed, status, String.trim(output)}}
      end
    end
  end

  defp write_install_state(fingerprint, toolchain, opts) do
    root_dir = Paths.root_dir(opts)

    runtime_inputs = runtime_inputs(root_dir)

    install = %{
      "schema_version" => @schema_version,
      "installed_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "fingerprint" => fingerprint,
      "runtime_inputs" => runtime_inputs,
      "toolchain_ref" => "toolchain.json"
    }

    with :ok <- materialize_runtime_inputs(runtime_inputs, root_dir),
         :ok <- State.write_toolchain(toolchain, opts),
         do: State.write_install(install, opts)
  end

  defp runtime_inputs(root_dir) do
    %{
      "web" => %{
        "source_root" => Path.join(root_dir, "web/favn_web"),
        "materialized_root" => Paths.install_runtime_web_dir(root_dir)
      },
      "orchestrator" => %{
        "source_root" => Path.join(root_dir, "apps/favn_orchestrator"),
        "materialized_root" => Paths.install_runtime_orchestrator_dir(root_dir)
      },
      "runner" => %{
        "source_root" => Path.join(root_dir, "apps/favn_runner"),
        "materialized_root" => Paths.install_runtime_runner_dir(root_dir)
      }
    }
  end

  defp materialize_runtime_inputs(runtime_inputs, root_dir) do
    [
      materialize_web_input(runtime_inputs, root_dir),
      materialize_orchestrator_input(runtime_inputs, root_dir),
      materialize_runner_input(runtime_inputs, root_dir)
    ]
    |> run_materialization_steps()
  end

  defp run_materialization_steps(results) do
    Enum.reduce_while(results, :ok, fn
      :ok, :ok -> {:cont, :ok}
      {:error, reason}, :ok -> {:halt, {:error, reason}}
      other, :ok -> {:halt, {:error, {:unexpected_materialization_result, other}}}
    end)
  end

  defp materialize_web_input(runtime_inputs, root_dir) do
    source_root = get_in(runtime_inputs, ["web", "source_root"])
    materialized_root = get_in(runtime_inputs, ["web", "materialized_root"])
    source_dir = Path.join(materialized_root, "source")

    with :ok <- File.mkdir_p(source_dir),
         :ok <-
           copy_if_exists(
             Path.join(source_root, "package.json"),
             Path.join(source_dir, "package.json")
           ),
         :ok <-
           copy_if_exists(
             Path.join(source_root, "package-lock.json"),
             Path.join(source_dir, "package-lock.json")
           ),
         :ok <-
           write_json(
             Path.join(materialized_root, "runtime_input.json"),
             %{
               "source_root" => source_root,
               "materialized_at" => DateTime.utc_now() |> DateTime.to_iso8601()
             }
           ) do
      copy_if_exists(Path.join(root_dir, "mix.lock"), Path.join(source_dir, "mix.lock"))
    end
  end

  defp materialize_orchestrator_input(runtime_inputs, root_dir) do
    source_root = get_in(runtime_inputs, ["orchestrator", "source_root"])
    materialized_root = get_in(runtime_inputs, ["orchestrator", "materialized_root"])
    source_dir = Path.join(materialized_root, "source")

    with :ok <- File.mkdir_p(source_dir),
         :ok <-
           copy_if_exists(Path.join(source_root, "mix.exs"), Path.join(source_dir, "mix.exs")),
         :ok <- copy_if_exists(Path.join(root_dir, "mix.lock"), Path.join(source_dir, "mix.lock")) do
      write_json(
        Path.join(materialized_root, "runtime_input.json"),
        %{
          "source_root" => source_root,
          "materialized_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        }
      )
    end
  end

  defp materialize_runner_input(runtime_inputs, root_dir) do
    source_root = get_in(runtime_inputs, ["runner", "source_root"])
    materialized_root = get_in(runtime_inputs, ["runner", "materialized_root"])
    source_dir = Path.join(materialized_root, "source")

    with :ok <- File.mkdir_p(source_dir),
         :ok <-
           copy_if_exists(Path.join(source_root, "mix.exs"), Path.join(source_dir, "mix.exs")),
         :ok <- copy_if_exists(Path.join(root_dir, "mix.lock"), Path.join(source_dir, "mix.lock")) do
      write_json(
        Path.join(materialized_root, "runtime_input.json"),
        %{
          "source_root" => source_root,
          "materialized_at" => DateTime.utc_now() |> DateTime.to_iso8601()
        }
      )
    end
  end

  defp copy_if_exists(source, destination) do
    if File.exists?(source) do
      File.cp(source, destination)
    else
      :ok
    end
  end

  defp write_json(path, data) when is_map(data) do
    encoded = JSON.encode_to_iodata!(data)
    File.write(path, [encoded, "\n"])
  end

  @spec fingerprint(root_opt()) :: {:ok, map()} | {:error, term()}
  defp fingerprint(opts) do
    with {:ok, node_version} <- command_version(opts, :node, "node", ["--version"]),
         {:ok, npm_version} <- command_version(opts, :npm, "npm", ["--version"]) do
      root_dir = Paths.root_dir(opts)

      {:ok,
       %{
         "schema_version" => @schema_version,
         "elixir_version" => System.version(),
         "otp_release" => otp_release(),
         "node_version" => node_version,
         "npm_version" => npm_version,
         "mix_lock_sha256" => file_sha256(Path.join(root_dir, "mix.lock")),
         "web_package_json_sha256" =>
           file_sha256(Path.join(root_dir, "web/favn_web/package.json")),
         "web_package_lock_sha256" =>
           file_sha256(Path.join(root_dir, "web/favn_web/package-lock.json")),
         "runner_mix_sha256" => file_sha256(Path.join(root_dir, "apps/favn_runner/mix.exs")),
         "orchestrator_mix_sha256" =>
           file_sha256(Path.join(root_dir, "apps/favn_orchestrator/mix.exs"))
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

  defp web_root(opts), do: Path.join(Paths.root_dir(opts), "web/favn_web")

  defp file_sha256(path) do
    case File.read(path) do
      {:ok, bytes} -> :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
      {:error, :enoent} -> nil
      {:error, _reason} -> nil
    end
  end
end
