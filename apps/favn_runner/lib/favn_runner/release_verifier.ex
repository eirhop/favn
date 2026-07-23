defmodule FavnRunner.ReleaseVerifier do
  @moduledoc """
  Installs the operator-supplied runner release identity at startup.

  The customer image is built outside Favn and must set
  `FAVN_RUNNER_RELEASE_ID` to the same immutable ID used when building its
  manifests. Favn validates that boundary and runner protocol compatibility;
  it does not inspect packaged customer modules or dependency provenance.
  """

  alias Favn.Contracts.RunnerError
  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease
  alias FavnRunner.PluginLoader

  @persistent_key {__MODULE__, :verified_release}
  @prepared_plugins_key {__MODULE__, :prepared_plugin_children}

  @type error ::
          :runner_release_not_verified
          | :runner_release_id_missing
          | {:invalid_runner_release_id, term()}
          | {:unsupported_runner_target, term(), String.t()}
          | PluginLoader.reason()

  @doc "Validates and installs the runner identity from the frozen boot environment."
  @spec verify_startup(map()) :: :ok | {:error, error()}
  def verify_startup(environment) when is_map(environment) do
    case verified_release() do
      {:ok, %RunnerRelease{}} -> ensure_prepared_plugins()
      {:error, :runner_release_not_verified} -> install_from_environment(environment)
    end
  end

  @doc false
  @spec verify_test_startup(map()) :: :ok | {:error, error()}
  def verify_test_startup(environment) when is_map(environment) do
    :persistent_term.erase(@persistent_key)
    :persistent_term.erase(@prepared_plugins_key)
    install_from_environment(environment)
  end

  @doc false
  @spec prepared_plugin_children() :: {:ok, [Supervisor.child_spec()]} | :not_prepared
  def prepared_plugin_children do
    case :persistent_term.get(@prepared_plugins_key, nil) do
      children when is_list(children) -> {:ok, children}
      nil -> :not_prepared
    end
  end

  @doc "Returns the configured runner identity installed for this node."
  @spec verified_release() :: {:ok, RunnerRelease.t()} | {:error, :runner_release_not_verified}
  def verified_release do
    case :persistent_term.get(@persistent_key, nil) do
      %RunnerRelease{} = release -> {:ok, release}
      nil -> {:error, :runner_release_not_verified}
    end
  end

  @doc "Returns bounded operational identity for the running customer image."
  @spec release_info() :: {:ok, map()} | {:error, :runner_release_not_verified}
  def release_info do
    with {:ok, release} <- verified_release() do
      {:ok,
       %{
         runner_release_id: release.runner_release_id,
         favn_version: release.favn_version,
         runner_contract_version: release.runner_contract_version,
         elixir_version: release.elixir_version,
         otp_release: release.otp_release,
         target: release.target,
         build_profile: release.build_profile,
         identity_source: :operator
       }}
    end
  end

  @doc false
  @spec runtime_target(term(), term()) ::
          {:ok, String.t()} | {:error, {:unsupported_runner_target, term(), String.t()}}
  def runtime_target(
        os_type \\ :os.type(),
        architecture \\ :erlang.system_info(:system_architecture)
      )

  def runtime_target({:unix, :linux}, architecture) do
    architecture = to_string(architecture)

    if architecture == "amd64" or String.starts_with?(architecture, "x86_64"),
      do: {:ok, "linux/amd64"},
      else: {:error, {:unsupported_runner_target, {:unix, :linux}, architecture}}
  end

  def runtime_target(os_type, architecture),
    do: {:error, {:unsupported_runner_target, os_type, to_string(architecture)}}

  @doc "Checks one manifest/work requirement against the configured release."
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
           message: "Runner release is not configured",
           reason: :runner_release_not_verified,
           retryable?: false,
           outcome: :safe_failure
         )}
    end
  end

  defp install_from_environment(environment) do
    with {:ok, runner_release_id} <- release_id(environment),
         {:ok, target} <- runtime_target(),
         {:ok, release} <-
           RunnerRelease.new(%{
             runner_release_id: runner_release_id,
             favn_version: RunnerRelease.current_favn_version(),
             runner_contract_version: Compatibility.current_runner_contract_version(),
             elixir_version: System.version(),
             otp_release: to_string(:erlang.system_info(:otp_release)),
             target: target,
             build_profile: "prod"
           }),
         {:ok, children} <- load_plugins() do
      :persistent_term.put(@persistent_key, release)
      :persistent_term.put(@prepared_plugins_key, children)
      :ok
    end
  end

  defp release_id(environment) do
    case Map.get(environment, "FAVN_RUNNER_RELEASE_ID") do
      value when is_binary(value) ->
        case RunnerRelease.validate_id(value) do
          :ok -> {:ok, value}
          {:error, _reason} -> {:error, {:invalid_runner_release_id, value}}
        end

      _missing ->
        {:error, :runner_release_id_missing}
    end
  end

  defp ensure_prepared_plugins do
    case prepared_plugin_children() do
      {:ok, _children} ->
        :ok

      :not_prepared ->
        with {:ok, children} <- load_plugins() do
          :persistent_term.put(@prepared_plugins_key, children)
          :ok
        end
    end
  end

  defp load_plugins do
    :favn
    |> Application.get_env(:runner_plugins, [])
    |> PluginLoader.load()
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
end
