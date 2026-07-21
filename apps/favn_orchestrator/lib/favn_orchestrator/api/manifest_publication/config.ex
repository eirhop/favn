defmodule FavnOrchestrator.API.ManifestPublication.Config do
  @moduledoc """
  Validated request-size budgets for manifest publication.

  Compressed requests are bounded both before and after decompression. Plain
  JSON requests use the decompressed limit because their wire and decoded sizes
  are identical.
  """

  @default_compressed_limit_bytes 8 * 1024 * 1024
  @default_decompressed_limit_bytes 32 * 1024 * 1024
  @maximum_compressed_limit_bytes 32 * 1024 * 1024
  @maximum_decompressed_limit_bytes 128 * 1024 * 1024
  @allowed_keys [:compressed_limit_bytes, :decompressed_limit_bytes]

  @enforce_keys [:compressed_limit_bytes, :decompressed_limit_bytes]
  defstruct compressed_limit_bytes: @default_compressed_limit_bytes,
            decompressed_limit_bytes: @default_decompressed_limit_bytes

  @type t :: %__MODULE__{
          compressed_limit_bytes: pos_integer(),
          decompressed_limit_bytes: pos_integer()
        }

  @type error ::
          {:invalid_manifest_publication_config, term()}
          | {:unknown_manifest_publication_config, atom()}
          | {:invalid_manifest_publication_limit, atom(), term(), pos_integer()}

  @doc "Loads and validates manifest-publication limits from application configuration."
  @spec from_app_env() :: {:ok, t()} | {:error, error()}
  def from_app_env do
    :favn_orchestrator
    |> Application.get_env(:manifest_publication, [])
    |> new()
  end

  @doc "Builds validated manifest-publication limits from keyword options."
  @spec new(keyword()) :: {:ok, t()} | {:error, error()}
  def new(opts) when is_list(opts) do
    with :ok <- validate_keyword(opts),
         :ok <- validate_keys(opts),
         {:ok, compressed_limit_bytes} <-
           limit(
             opts,
             :compressed_limit_bytes,
             @default_compressed_limit_bytes,
             @maximum_compressed_limit_bytes
           ),
         {:ok, decompressed_limit_bytes} <-
           limit(
             opts,
             :decompressed_limit_bytes,
             @default_decompressed_limit_bytes,
             @maximum_decompressed_limit_bytes
           ),
         :ok <- validate_limit_relationship(compressed_limit_bytes, decompressed_limit_bytes) do
      {:ok,
       %__MODULE__{
         compressed_limit_bytes: compressed_limit_bytes,
         decompressed_limit_bytes: decompressed_limit_bytes
       }}
    end
  end

  def new(other), do: {:error, {:invalid_manifest_publication_config, other}}

  @doc "Returns the default compressed request limit in bytes."
  @spec default_compressed_limit_bytes() :: pos_integer()
  def default_compressed_limit_bytes, do: @default_compressed_limit_bytes

  @doc "Returns the default decompressed or plain JSON request limit in bytes."
  @spec default_decompressed_limit_bytes() :: pos_integer()
  def default_decompressed_limit_bytes, do: @default_decompressed_limit_bytes

  @doc "Returns the maximum configurable compressed request limit in bytes."
  @spec maximum_compressed_limit_bytes() :: pos_integer()
  def maximum_compressed_limit_bytes, do: @maximum_compressed_limit_bytes

  @doc "Returns the maximum configurable decompressed request limit in bytes."
  @spec maximum_decompressed_limit_bytes() :: pos_integer()
  def maximum_decompressed_limit_bytes, do: @maximum_decompressed_limit_bytes

  @doc "Converts the validated configuration to application-env keyword form."
  @spec to_keyword(t()) :: keyword()
  def to_keyword(%__MODULE__{} = config) do
    [
      compressed_limit_bytes: config.compressed_limit_bytes,
      decompressed_limit_bytes: config.decompressed_limit_bytes
    ]
  end

  defp validate_keyword(opts) do
    if Keyword.keyword?(opts),
      do: :ok,
      else: {:error, {:invalid_manifest_publication_config, opts}}
  end

  defp validate_keys(opts) do
    case Enum.find(Keyword.keys(opts), &(&1 not in @allowed_keys)) do
      nil -> :ok
      key -> {:error, {:unknown_manifest_publication_config, key}}
    end
  end

  defp limit(opts, key, default, maximum) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 and value <= maximum ->
        {:ok, value}

      value ->
        {:error, {:invalid_manifest_publication_limit, key, value, maximum}}
    end
  end

  defp validate_limit_relationship(compressed, decompressed)
       when decompressed >= compressed,
       do: :ok

  defp validate_limit_relationship(_compressed, _decompressed),
    do: {:error, {:invalid_manifest_publication_config, :decompressed_limit_too_small}}
end
