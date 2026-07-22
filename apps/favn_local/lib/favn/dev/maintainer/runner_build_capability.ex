defmodule Favn.Dev.Maintainer.RunnerBuildCapability do
  @moduledoc false

  alias Favn.Dev.Maintainer.Candidate
  alias Favn.Dev.Paths

  @environment_variable "FAVN_INTERNAL_MAINTAINER_RUNNER_BUILD"
  @schema_version 1
  @max_payload_bytes 16_384
  @max_path_bytes 4_096
  @revision ~r/\A[0-9a-f]{40,64}\z/
  @fingerprint ~r/\A[0-9a-f]{64}\z/
  @token ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [:consumer_root, :checkout, :revision, :dirty, :fingerprint]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          consumer_root: Path.t(),
          checkout: Path.t(),
          revision: String.t(),
          dirty: boolean(),
          fingerprint: String.t()
        }

  @spec from_candidate(Candidate.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def from_candidate(%Candidate{} = candidate, opts) when is_list(opts) do
    capability = %__MODULE__{
      consumer_root: opts |> Paths.root_dir() |> Path.expand(),
      checkout: Path.expand(candidate.checkout),
      revision: candidate.checkout_revision,
      dirty: candidate.checkout_dirty,
      fingerprint: candidate.checkout_fingerprint
    }

    if valid?(capability), do: {:ok, capability}, else: {:error, :invalid_maintainer_runner_build}
  end

  @spec environment(nil | t()) ::
          {:ok, [{String.t(), String.t() | nil}], [Path.t()]} | {:error, term()}
  def environment(nil), do: {:ok, [{@environment_variable, nil}], []}

  def environment(%__MODULE__{} = capability) do
    with true <- valid?(capability),
         token <- random_token(),
         directory <- capability_directory(capability.consumer_root),
         :ok <- ensure_capability_directory(directory),
         path <- Path.join(directory, token),
         payload <- encode(capability, token),
         true <- byte_size(payload) <= @max_payload_bytes,
         :ok <- File.write(path, payload, [:binary, :exclusive, :sync]),
         :ok <- File.chmod(path, 0o600) do
      {:ok, [{@environment_variable, token}], [path, consuming_path(path)]}
    else
      _invalid -> {:error, :invalid_maintainer_runner_build}
    end
  rescue
    _error -> {:error, :invalid_maintainer_runner_build}
  end

  def environment(_invalid), do: {:error, :invalid_maintainer_runner_build}

  @spec cleanup([Path.t()]) :: :ok
  def cleanup(paths) when is_list(paths) do
    Enum.each(paths, &File.rm/1)
    :ok
  end

  @spec consume(keyword()) :: {:ok, keyword()} | {:error, term()}
  def consume(opts) when is_list(opts) do
    token = System.get_env(@environment_variable)
    System.delete_env(@environment_variable)

    case token do
      nil -> {:ok, opts}
      token -> consume_token(token, opts)
    end
  end

  defp consume_token(token, opts) when is_binary(token) do
    root = opts |> Paths.root_dir() |> Path.expand()

    with true <- Regex.match?(@token, token),
         path <- Path.join(capability_directory(root), token),
         consumed <- consuming_path(path),
         :ok <- File.rename(path, consumed) do
      try do
        with {:ok, %{type: :regular, mode: mode}} <- File.lstat(consumed),
             true <- Bitwise.band(mode, 0o077) == 0,
             {:ok, payload} <- File.read(consumed),
             true <- byte_size(payload) <= @max_payload_bytes,
             {:ok, capability} <- decode(payload, token, root) do
          {:ok, Keyword.put(opts, :maintainer_runner_build, capability)}
        else
          _invalid -> {:error, :invalid_maintainer_runner_build}
        end
      after
        File.rm(consumed)
      end
    else
      _invalid -> {:error, :invalid_maintainer_runner_build}
    end
  rescue
    _error -> {:error, :invalid_maintainer_runner_build}
  end

  defp encode(capability, token) do
    :erlang.term_to_binary(
      %{
        "schema_version" => @schema_version,
        "token" => token,
        "consumer_root" => capability.consumer_root,
        "checkout" => capability.checkout,
        "revision" => capability.revision,
        "dirty" => capability.dirty,
        "fingerprint" => capability.fingerprint
      },
      [:deterministic]
    )
  end

  defp decode(payload, token, root) do
    false = match?(<<131, 80, _compressed::binary>>, payload)
    capability(payload |> :erlang.binary_to_term([:safe]), token, root)
  rescue
    _error -> {:error, :invalid_maintainer_runner_build}
  end

  defp capability(
         %{
           "schema_version" => @schema_version,
           "token" => token,
           "consumer_root" => consumer_root,
           "checkout" => checkout,
           "revision" => revision,
           "dirty" => dirty,
           "fingerprint" => fingerprint
         } = payload,
         token,
         root
       )
       when map_size(payload) == 7 do
    capability = %__MODULE__{
      consumer_root: consumer_root,
      checkout: checkout,
      revision: revision,
      dirty: dirty,
      fingerprint: fingerprint
    }

    if valid?(capability) and capability.consumer_root == root,
      do: {:ok, capability},
      else: {:error, :invalid_maintainer_runner_build}
  end

  defp capability(_payload, _token, _root), do: {:error, :invalid_maintainer_runner_build}

  defp ensure_capability_directory(directory) do
    with :ok <- File.mkdir_p(directory),
         {:ok, %{type: :directory}} <- File.lstat(directory),
         :ok <- File.chmod(directory, 0o700) do
      :ok
    else
      _invalid -> {:error, :invalid_maintainer_runner_build}
    end
  end

  defp capability_directory(root),
    do: root |> Paths.build_dir() |> Path.join("maintainer-runner-capabilities")

  defp consuming_path(path), do: path <> ".consuming"

  defp random_token, do: 32 |> :crypto.strong_rand_bytes() |> Base.encode16(case: :lower)

  defp valid?(%__MODULE__{} = capability) do
    valid_path?(capability.consumer_root) and valid_path?(capability.checkout) and
      is_binary(capability.revision) and Regex.match?(@revision, capability.revision) and
      is_boolean(capability.dirty) and is_binary(capability.fingerprint) and
      Regex.match?(@fingerprint, capability.fingerprint)
  end

  defp valid_path?(path) when is_binary(path) and byte_size(path) <= @max_path_bytes,
    do: Path.type(path) == :absolute and Path.expand(path) == path

  defp valid_path?(_path), do: false
end
