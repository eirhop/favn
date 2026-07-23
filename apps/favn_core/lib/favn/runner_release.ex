defmodule Favn.RunnerRelease do
  @moduledoc """
  Operator-owned identity of one customer runner build.

  Favn treats the release ID as an opaque immutable binding between a runner
  image and the manifests that image may execute. Local tooling generates the
  ID when it invokes the customer-owned Dockerfile. Production CI may choose
  the ID explicitly. Favn validates exact image/manifest alignment without
  making the ID a content claim.
  """

  alias Favn.Manifest.Compatibility

  @target "linux/amd64"
  @build_profile "prod"
  @runner_release_id ~r/\Arr_[0-9a-f]{64}\z/
  @fallback_favn_version "0.5.0-dev"

  @enforce_keys [
    :favn_version,
    :runner_contract_version,
    :elixir_version,
    :otp_release,
    :target,
    :runner_release_id
  ]
  defstruct [
    :favn_version,
    :runner_contract_version,
    :elixir_version,
    :otp_release,
    :target,
    :runner_release_id,
    build_profile: @build_profile
  ]

  @type t :: %__MODULE__{
          favn_version: String.t(),
          runner_contract_version: pos_integer(),
          elixir_version: String.t(),
          otp_release: String.t(),
          target: String.t(),
          runner_release_id: String.t(),
          build_profile: String.t()
        }

  @type error ::
          {:invalid_runner_release, :expected_map}
          | {:missing_runner_release_field, atom()}
          | {:invalid_runner_release_field, atom(), atom()}

  @doc "Builds a validated runtime identity from explicit operator input."
  @spec new(map()) :: {:ok, t()} | {:error, error()}
  def new(attrs) when is_map(attrs) do
    with {:ok, runner_release_id} <- required(attrs, :runner_release_id),
         :ok <- validate_id(runner_release_id),
         {:ok, favn_version} <- required_string(attrs, :favn_version),
         {:ok, runner_contract_version} <-
           required_positive_integer(attrs, :runner_contract_version),
         :ok <- validate_runner_contract_version(runner_contract_version),
         {:ok, elixir_version} <- required_string(attrs, :elixir_version),
         {:ok, otp_release} <- required_string(attrs, :otp_release),
         {:ok, target} <- required_string(attrs, :target),
         :ok <- validate_target(target),
         {:ok, build_profile} <- optional_build_profile(attrs) do
      {:ok,
       %__MODULE__{
         favn_version: favn_version,
         runner_contract_version: runner_contract_version,
         elixir_version: elixir_version,
         otp_release: otp_release,
         target: target,
         runner_release_id: runner_release_id,
         build_profile: build_profile
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  def new(_attrs), do: {:error, {:invalid_runner_release, :expected_map}}

  @doc "Validates an existing runtime identity."
  @spec verify(map() | t()) :: {:ok, t()} | {:error, error()}
  def verify(%__MODULE__{} = release), do: new(Map.from_struct(release))
  def verify(attrs) when is_map(attrs), do: new(attrs)
  def verify(_value), do: {:error, {:invalid_runner_release, :expected_map}}

  @doc "Validates the canonical `rr_` plus lowercase SHA-256 release ID syntax."
  @spec validate_id(term()) ::
          :ok | {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}}
  def validate_id(value) when is_binary(value) do
    if Regex.match?(@runner_release_id, value),
      do: :ok,
      else: {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}}
  end

  def validate_id(_value),
    do: {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}}

  @doc "Returns whether a value has canonical runner release ID syntax."
  @spec valid_id?(term()) :: boolean()
  def valid_id?(value), do: validate_id(value) == :ok

  @doc "Returns the Favn application version used for runtime diagnostics."
  @spec current_favn_version() :: String.t()
  def current_favn_version do
    :favn_core
    |> Application.spec(:vsn)
    |> case do
      nil -> @fallback_favn_version
      version -> to_string(version)
    end
  end

  @doc "Returns the currently supported runner target."
  @spec current_target() :: String.t()
  def current_target, do: @target

  defp required(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_runner_release_field, key}}
    end
  end

  defp required_string(attrs, key) do
    with {:ok, value} <- required(attrs, key),
         true <- is_binary(value) and String.trim(value) != "" do
      {:ok, value}
    else
      {:error, _reason} = error -> error
      false -> {:error, {:invalid_runner_release_field, key, :invalid_value}}
    end
  end

  defp required_positive_integer(attrs, key) do
    with {:ok, value} <- required(attrs, key),
         true <- is_integer(value) and value > 0 do
      {:ok, value}
    else
      {:error, _reason} = error -> error
      false -> {:error, {:invalid_runner_release_field, key, :invalid_value}}
    end
  end

  defp optional_build_profile(attrs) do
    case Map.get(attrs, :build_profile, @build_profile) do
      @build_profile -> {:ok, @build_profile}
      _value -> {:error, {:invalid_runner_release_field, :build_profile, :unsupported_value}}
    end
  end

  defp validate_runner_contract_version(value) do
    if value == Compatibility.current_runner_contract_version(),
      do: :ok,
      else:
        {:error, {:invalid_runner_release_field, :runner_contract_version, :unsupported_value}}
  end

  defp validate_target(@target), do: :ok

  defp validate_target(_value),
    do: {:error, {:invalid_runner_release_field, :target, :unsupported_value}}
end
