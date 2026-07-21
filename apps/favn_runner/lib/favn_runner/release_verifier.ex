defmodule FavnRunner.ReleaseVerifier do
  @moduledoc """
  Verifies the runner descriptor against the packaged release before startup.

  Packaged releases read `runner-release.json` from the `favn_runner` private
  directory. Verification is required when the release script supplies
  `RELEASE_NAME`, and may be required explicitly with the
  `:release_verification` application setting. Mix-based development can start
  without an installed descriptor, but runner operations remain unavailable
  until a verified descriptor is installed.

  Verification never creates atoms from descriptor values and never includes
  artifact paths or descriptor contents in returned errors. Runtime application
  fingerprints are read from packaged `.app` files, so verification does not
  depend on whether an application has already started. Release assembly must
  stamp every fingerprinted `.app` file with its
  `:favn_runner_lock_fingerprint` property.
  """

  alias Favn.Contracts.RunnerError
  alias Favn.RunnerRelease
  alias Favn.RunnerRelease.BeamDigest
  alias Favn.RunnerRelease.ModuleFingerprint
  alias FavnRunner.PluginLoader

  @persistent_key {__MODULE__, :verified_release}
  @prepared_plugins_key {__MODULE__, :prepared_plugin_children}
  @descriptor_filename "runner-release.json"
  @max_descriptor_bytes 1_048_576

  @type verification_mode :: :optional | :required
  @type error ::
          :runner_release_not_verified
          | :runner_release_descriptor_missing
          | :runner_release_descriptor_unreadable
          | :runner_release_descriptor_too_large
          | {:runner_release_descriptor_invalid, atom()}
          | {:runner_release_module_missing, String.t()}
          | {:runner_release_module_mismatch, String.t()}
          | {:runner_release_application_missing, String.t()}
          | {:runner_release_dependency_mismatch, String.t()}
          | {:runner_release_plugin_mismatch, String.t() | :invalid_configuration}
          | {:runner_release_runtime_mismatch, atom()}
          | PluginLoader.reason()

  @doc "Returns the fixed descriptor location inside the runner application."
  @spec descriptor_path() :: String.t()
  def descriptor_path do
    Application.app_dir(:favn_runner, Path.join("priv", @descriptor_filename))
  end

  @doc "Verifies and installs only the descriptor at the fixed packaged-release path."
  @spec verify_startup() :: :ok | {:error, error()}
  def verify_startup do
    case verified_release() do
      {:ok, %RunnerRelease{}} ->
        ensure_prepared_plugins()

      {:error, :runner_release_not_verified} ->
        verify_and_install(startup_mode(), descriptor_path())
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec verify_test_startup(keyword()) :: :ok | {:error, error()}
    def verify_test_startup(opts) when is_list(opts) do
      :persistent_term.erase(@persistent_key)
      :persistent_term.erase(@prepared_plugins_key)

      verify_and_install(
        Keyword.fetch!(opts, :mode),
        Keyword.fetch!(opts, :path)
      )
    end

    @doc false
    @spec verify_test_file(Path.t(), keyword()) ::
            {:ok, RunnerRelease.t()} | {:error, error()}
    def verify_test_file(path, runtime_overrides)
        when is_binary(path) and is_list(runtime_overrides) do
      verify_file(path, Map.merge(runtime_facts(), Map.new(runtime_overrides)))
    end
  end

  defp verify_and_install(mode, path) do
    case {mode, File.exists?(path)} do
      {:optional, false} ->
        :ok

      {mode, _exists?} when mode in [:optional, :required] ->
        with {:ok, descriptor} <- verify_file(path),
             {:ok, children} <- prepare_plugins(descriptor) do
          :persistent_term.put(@persistent_key, descriptor)
          :persistent_term.put(@prepared_plugins_key, children)
          :ok
        end

      _invalid ->
        {:error, {:runner_release_descriptor_invalid, :invalid_verification_mode}}
    end
  end

  @doc "Reads and verifies one descriptor file against the current release."
  @spec verify_file(Path.t()) :: {:ok, RunnerRelease.t()} | {:error, error()}
  def verify_file(path) when is_binary(path) do
    verify_file(path, runtime_facts())
  end

  defp verify_file(path, runtime_facts) do
    with {:ok, bytes} <- read_descriptor(path),
         {:ok, descriptor} <- decode_descriptor(bytes),
         :ok <- verify_runtime(descriptor, runtime_facts),
         :ok <- verify_modules(descriptor),
         :ok <- verify_applications(descriptor),
         :ok <- verify_plugin_descriptor(descriptor) do
      {:ok, descriptor}
    end
  end

  @doc false
  @spec prepared_plugin_children() :: {:ok, [Supervisor.child_spec()]} | :not_prepared
  def prepared_plugin_children do
    case :persistent_term.get(@prepared_plugins_key, nil) do
      children when is_list(children) -> {:ok, children}
      nil -> :not_prepared
    end
  end

  @doc "Returns the verified descriptor installed for this runner node."
  @spec verified_release() :: {:ok, RunnerRelease.t()} | {:error, :runner_release_not_verified}
  def verified_release do
    case :persistent_term.get(@persistent_key, nil) do
      %RunnerRelease{} = descriptor -> {:ok, descriptor}
      nil -> {:error, :runner_release_not_verified}
    end
  end

  @doc "Returns bounded operational identity from the verified descriptor."
  @spec release_info() :: {:ok, map()} | {:error, :runner_release_not_verified}
  def release_info do
    with {:ok, descriptor} <- verified_release() do
      {:ok,
       %{
         runner_release_id: descriptor.runner_release_id,
         favn_version: descriptor.favn_version,
         runner_contract_version: descriptor.runner_contract_version,
         elixir_version: descriptor.elixir_version,
         otp_release: descriptor.otp_release,
         target: descriptor.target,
         build_profile: descriptor.build_profile
       }}
    end
  end

  @doc "Checks one manifest/work requirement against the installed release."
  @spec verify_required_release(term()) :: :ok | {:error, RunnerError.t()}
  def verify_required_release(required) do
    case verified_release() do
      {:ok, %RunnerRelease{runner_release_id: ^required}} ->
        :ok

      {:ok, %RunnerRelease{runner_release_id: actual}} ->
        {:error, release_mismatch_error(required, actual)}

      {:error, :runner_release_not_verified} ->
        {:error,
         RunnerError.new(
           kind: :boundary,
           type: :runner_release_not_verified,
           phase: :runner_release,
           message: "Runner release is not verified",
           reason: :runner_release_not_verified,
           retryable?: false,
           outcome: :safe_failure
         )}
    end
  end

  defp startup_mode do
    if present_env?("RELEASE_NAME"), do: :required, else: :optional
  end

  defp present_env?(name) do
    case System.get_env(name) do
      value when is_binary(value) -> String.trim(value) != ""
      _missing -> false
    end
  end

  defp read_descriptor(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} when size <= @max_descriptor_bytes ->
        case File.read(path) do
          {:ok, bytes} -> {:ok, bytes}
          {:error, _reason} -> {:error, :runner_release_descriptor_unreadable}
        end

      {:ok, %{type: :regular}} ->
        {:error, :runner_release_descriptor_too_large}

      {:ok, _other} ->
        {:error, :runner_release_descriptor_unreadable}

      {:error, :enoent} ->
        {:error, :runner_release_descriptor_missing}

      {:error, _reason} ->
        {:error, :runner_release_descriptor_unreadable}
    end
  end

  defp decode_descriptor(bytes) do
    case RunnerRelease.decode(bytes) do
      {:ok, descriptor} ->
        {:ok, descriptor}

      {:error, reason} ->
        {:error, {:runner_release_descriptor_invalid, descriptor_reason(reason)}}
    end
  end

  defp descriptor_reason({:runner_release_id_mismatch, _expected, _actual}),
    do: :self_hash_mismatch

  defp descriptor_reason({:runtime_code_digest_mismatch, _expected, _actual}),
    do: :code_digest_mismatch

  defp descriptor_reason({:runtime_dependency_digest_mismatch, _expected, _actual}),
    do: :dependency_digest_mismatch

  defp descriptor_reason({:unsupported_runner_release_schema, _actual, _expected}),
    do: :unsupported_schema

  defp descriptor_reason({:unsupported_runner_contract, _actual, _expected}),
    do: :unsupported_runner_contract

  defp descriptor_reason({:unsupported_favn_version, _actual, _expected}),
    do: :unsupported_favn_version

  defp descriptor_reason({:invalid_runner_release_json, _reason}), do: :invalid_json
  defp descriptor_reason(_reason), do: :invalid_descriptor

  defp verify_runtime(descriptor, runtime_facts) do
    cond do
      descriptor.favn_version != runtime_facts.favn_version ->
        {:error, {:runner_release_runtime_mismatch, :favn_version}}

      descriptor.elixir_version != runtime_facts.elixir_version ->
        {:error, {:runner_release_runtime_mismatch, :elixir_version}}

      descriptor.otp_release != runtime_facts.otp_release ->
        {:error, {:runner_release_runtime_mismatch, :otp_release}}

      descriptor.target != runtime_facts.target ->
        {:error, {:runner_release_runtime_mismatch, :target}}

      true ->
        :ok
    end
  end

  defp verify_modules(descriptor) do
    Enum.reduce_while(descriptor.runtime_modules, :ok, fn fingerprint, :ok ->
      case verify_module(fingerprint) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp verify_module(%ModuleFingerprint{} = fingerprint) do
    beam_filename = String.to_charlist(fingerprint.module <> ".beam")

    case :code.where_is_file(beam_filename) do
      :non_existing ->
        {:error, {:runner_release_module_missing, fingerprint.module}}

      path when is_list(path) ->
        with {:ok, beam} <- read_beam(path),
             {:ok, metadata} <- BeamDigest.metadata(beam),
             true <- metadata.module == fingerprint.module,
             true <- metadata.digest == fingerprint.digest do
          :ok
        else
          _mismatch -> {:error, {:runner_release_module_mismatch, fingerprint.module}}
        end
    end
  end

  defp read_beam(path) do
    case File.read(List.to_string(path)) do
      {:ok, beam} -> {:ok, beam}
      {:error, _reason} -> {:error, :unreadable}
    end
  end

  defp verify_applications(descriptor) do
    expected =
      Enum.map(descriptor.runtime_applications, fn fingerprint ->
        {fingerprint.application, fingerprint.version, fingerprint.lock_fingerprint}
      end)

    with :ok <- verify_expected_applications(expected),
         {:ok, actual} <- stamped_packaged_applications(),
         :ok <- verify_exact_application_set(expected, actual) do
      :ok
    end
  end

  defp verify_expected_applications(expected) do
    Enum.reduce_while(expected, :ok, fn {application, version, lock_fingerprint}, :ok ->
      case packaged_application(application) do
        {:ok, ^version, ^lock_fingerprint} ->
          {:cont, :ok}

        {:ok, _other_version, _other_lock_fingerprint} ->
          {:halt, {:error, {:runner_release_dependency_mismatch, application}}}

        :missing ->
          {:halt, {:error, {:runner_release_application_missing, application}}}

        _not_fingerprinted ->
          {:halt, {:error, {:runner_release_dependency_mismatch, application}}}
      end
    end)
  end

  defp packaged_application(application_name) do
    filename = String.to_charlist(application_name <> ".app")

    case :code.where_is_file(filename) do
      :non_existing ->
        :missing

      path when is_list(path) ->
        read_packaged_application(path, application_name)
    end
  end

  defp read_packaged_application(path, application_name) do
    case :file.consult(path) do
      {:ok, [{:application, application, properties}]}
      when is_atom(application) and is_list(properties) ->
        with true <- Atom.to_string(application) == application_name,
             {:ok, version} <- Keyword.fetch(properties, :vsn),
             {:ok, lock_fingerprint} <- Keyword.fetch(properties, :favn_runner_lock_fingerprint),
             true <- is_binary(lock_fingerprint) do
          {:ok, to_string(version), lock_fingerprint}
        else
          :error -> :unstamped
          _invalid -> :invalid
        end

      _invalid ->
        :invalid
    end
  end

  defp stamped_packaged_applications do
    application_names =
      :code.get_path()
      |> Enum.flat_map(&application_names_on_path/1)
      |> Enum.uniq()

    actual =
      Enum.reduce(application_names, [], fn application_name, acc ->
        case packaged_application(application_name) do
          {:ok, version, lock_fingerprint} ->
            [{application_name, version, lock_fingerprint} | acc]

          _not_stamped ->
            acc
        end
      end)

    {:ok, Enum.sort(actual)}
  end

  defp application_names_on_path(path) do
    case File.ls(List.to_string(path)) do
      {:ok, entries} ->
        entries
        |> Enum.filter(&String.ends_with?(&1, ".app"))
        |> Enum.map(&String.trim_trailing(&1, ".app"))

      {:error, _reason} ->
        []
    end
  end

  defp verify_exact_application_set(expected, actual) do
    expected = Enum.sort(expected)

    if expected == actual do
      :ok
    else
      expected_names = MapSet.new(expected, &elem(&1, 0))

      unexpected =
        Enum.find(actual, fn {application, _version, _lock_fingerprint} ->
          not MapSet.member?(expected_names, application)
        end)

      case unexpected do
        {application, _version, _lock_fingerprint} ->
          {:error, {:runner_release_dependency_mismatch, application}}

        nil ->
          {:error, {:runner_release_dependency_mismatch, "application_set"}}
      end
    end
  end

  defp verify_plugin_descriptor(descriptor) do
    runtime_modules = MapSet.new(descriptor.runtime_modules, & &1.module)

    Enum.reduce_while(descriptor.plugins, :ok, fn plugin, :ok ->
      missing =
        cond do
          plugin.plugin not in plugin.modules ->
            plugin.plugin

          true ->
            Enum.find(plugin.modules, &(not MapSet.member?(runtime_modules, &1)))
        end

      if missing do
        {:halt, {:error, {:runner_release_module_missing, missing}}}
      else
        {:cont, :ok}
      end
    end)
  end

  defp prepare_plugins(descriptor) do
    entries = Application.get_env(:favn, :runner_plugins, [])

    with :ok <- verify_configured_plugins(descriptor.plugins, entries) do
      PluginLoader.prepare(entries,
        application_validator: plugin_application_validator(descriptor),
        child_module_validator: plugin_child_module_validator(descriptor)
      )
    end
  end

  defp ensure_prepared_plugins do
    case prepared_plugin_children() do
      {:ok, _children} ->
        :ok

      :not_prepared ->
        with {:ok, descriptor} <- verified_release(),
             {:ok, children} <- prepare_plugins(descriptor) do
          :persistent_term.put(@prepared_plugins_key, children)
          :ok
        end
    end
  end

  defp verify_configured_plugins(descriptor_plugins, entries) do
    expected = descriptor_plugins |> Enum.map(& &1.plugin) |> Enum.sort()

    case configured_plugin_names(entries) do
      {:ok, ^expected} ->
        :ok

      {:ok, actual} ->
        mismatch = Enum.find(actual, &(not Enum.member?(expected, &1))) || "plugin_set"
        {:error, {:runner_release_plugin_mismatch, mismatch}}

      :error ->
        {:error, {:runner_release_plugin_mismatch, :invalid_configuration}}
    end
  end

  defp configured_plugin_names(entries) do
    case entries do
      entries when is_list(entries) ->
        entries
        |> Enum.reduce_while({:ok, []}, fn
          module, {:ok, acc} when is_atom(module) ->
            {:cont, {:ok, [Atom.to_string(module) | acc]}}

          {module, opts}, {:ok, acc} when is_atom(module) and is_list(opts) ->
            {:cont, {:ok, [Atom.to_string(module) | acc]}}

          _invalid, _acc ->
            {:halt, :error}
        end)
        |> case do
          {:ok, names} -> {:ok, Enum.sort(names)}
          :error -> :error
        end

      _invalid ->
        :error
    end
  end

  defp plugin_application_validator(descriptor) do
    applications = MapSet.new(descriptor.runtime_applications, & &1.application)

    fn plugin, application ->
      if MapSet.member?(applications, Atom.to_string(application)) do
        :ok
      else
        {:error, {:unverified_plugin_application, plugin, application}}
      end
    end
  end

  defp plugin_child_module_validator(descriptor) do
    runtime_modules = MapSet.new(descriptor.runtime_modules, & &1.module)
    plugins = Map.new(descriptor.plugins, &{&1.plugin, MapSet.new(&1.modules)})

    fn plugin, child_module ->
      plugin_name = Atom.to_string(plugin)
      child_module_name = Atom.to_string(child_module)
      plugin_modules = Map.get(plugins, plugin_name, MapSet.new())

      if MapSet.member?(plugin_modules, child_module_name) and
           MapSet.member?(runtime_modules, child_module_name) do
        :ok
      else
        {:error, {:unverified_plugin_child_module, plugin, child_module}}
      end
    end
  end

  defp release_mismatch_error(required, actual) do
    RunnerError.new(
      kind: :boundary,
      type: :runner_release_mismatch,
      phase: :runner_release,
      message: "Runner release does not match the requested release",
      reason: :runner_release_mismatch,
      details: %{
        required_runner_release_id: bounded_release_id(required),
        runner_release_id: actual
      },
      retryable?: false,
      outcome: :safe_failure
    )
  end

  defp bounded_release_id(value) do
    if RunnerRelease.valid_id?(value), do: value, else: :invalid
  end

  defp runtime_target do
    architecture = :erlang.system_info(:system_architecture) |> to_string() |> String.downcase()

    cond do
      String.contains?(architecture, "linux") and
          (String.starts_with?(architecture, "x86_64") or
             String.starts_with?(architecture, "amd64")) ->
        "linux/amd64"

      String.contains?(architecture, "linux") and
          (String.starts_with?(architecture, "aarch64") or
             String.starts_with?(architecture, "arm64")) ->
        "linux/arm64"

      true ->
        "unsupported"
    end
  end

  defp runtime_facts do
    %{
      favn_version: RunnerRelease.current_favn_version(),
      elixir_version: System.version(),
      otp_release: to_string(:erlang.system_info(:otp_release)),
      target: runtime_target()
    }
  end
end
