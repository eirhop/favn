defmodule Favn.Dev.Install do
  @moduledoc """
  Project-local install and install-state validation for Favn dev tooling.
  """

  alias Favn.Dev.ChildEnvironment
  alias Favn.Dev.Command
  alias Favn.Dev.Paths
  alias Favn.Dev.RuntimeSource
  alias Favn.Dev.RuntimeWorkspace
  alias Favn.Dev.State

  @schema_version 3
  @runtime_deps_timeout_ms 600_000
  @web_install_timeout_ms 300_000

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
         {:ok, source} <- RuntimeSource.resolve(opts),
         {:ok, current_fingerprint} <- fingerprint(source, opts),
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
         {:ok, current_fingerprint} <- fingerprint(source, opts),
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
         :ok <- State.clear_install(opts),
         {:ok, runtime} <- RuntimeWorkspace.materialize(source, opts),
         :ok <- install_runtime_dependencies(runtime, opts),
         :ok <- install_web_assets(runtime, opts) do
      with :ok <- write_install_state(current_fingerprint, toolchain, source, runtime, opts),
           do: {:ok, :installed}
    end
  end

  defp maybe_install({:error, _reason} = error, _current_fingerprint, _source, _opts), do: error

  defp build_toolchain(_opts) do
    {:ok,
     %{
       "schema_version" => @schema_version,
       "captured_at" => DateTime.utc_now() |> DateTime.to_iso8601(),
       "elixir_version" => System.version(),
       "otp_release" => otp_release()
     }}
  end

  defp install_runtime_dependencies(runtime, opts) do
    if Keyword.get(opts, :skip_runtime_deps_install, false) do
      :ok
    else
      runtime_root = runtime["materialized_root"]
      runtime_mix_exs = Path.join(runtime_root, "mix.exs")

      if File.exists?(runtime_mix_exs) do
        mix_exec = System.find_executable("mix") || "mix"
        runner = command_runner(opts, :runtime_deps_command_runner, @runtime_deps_timeout_ms)

        :ok = report_phase(opts, "Favn install: resolving runtime dependencies")

        case runner.(mix_exec, ["deps.get"], cd: runtime_root, stderr_to_stdout: true) do
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

  defp install_web_assets(runtime, opts) do
    if Keyword.get(opts, :skip_web_install, false) do
      :ok
    else
      installer_root = runtime |> Map.fetch!("web_root") |> Path.join("asset_installer")
      installer_mix_exs = Path.join(installer_root, "mix.exs")

      if File.exists?(installer_mix_exs) do
        mix_exec = System.find_executable("mix") || "mix"
        runner = command_runner(opts, :web_install_command_runner, @web_install_timeout_ms)

        command_opts = [
          cd: installer_root,
          stderr_to_stdout: true,
          env: Map.put(ChildEnvironment.empty_proxy_overrides(), "MIX_ENV", "prod")
        ]

        :ok = report_phase(opts, "Favn install: installing web asset binaries")

        case runner.(mix_exec, ["assets.setup"], command_opts) do
          {_output, 0} -> :ok
          {output, status} -> {:error, {:web_install_failed, status, String.trim(output)}}
        end
      else
        :ok
      end
    end
  end

  defp command_runner(opts, runner_key, default_timeout_ms) do
    case Keyword.fetch(opts, runner_key) do
      {:ok, runner} ->
        runner

      :error ->
        timeout_ms = Keyword.get(opts, :install_command_timeout_ms, default_timeout_ms)
        output_writer = Keyword.get(opts, :install_output_writer, &IO.binwrite/1)

        fn executable, args, command_opts ->
          Command.run(
            executable,
            args,
            Keyword.merge(command_opts, timeout_ms: timeout_ms, output_writer: output_writer)
          )
        end
    end
  end

  defp report_phase(opts, message) do
    writer = Keyword.get(opts, :install_progress_writer, &IO.puts/1)
    writer.(message)
    :ok
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

  defp fingerprint(source, opts) do
    with {:ok, runtime_fingerprint} <- RuntimeSource.fingerprint(source) do
      root_dir = Paths.root_dir(opts)

      {:ok,
       %{
         "schema_version" => @schema_version,
         "elixir_version" => System.version(),
         "otp_release" => otp_release(),
         "consumer_mix_lock_sha256" => file_sha256(Path.join(root_dir, "mix.lock")),
         "runtime_source" => runtime_fingerprint
       }}
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
