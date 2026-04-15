defmodule Favn.Manifest.Identity do
  @moduledoc """
  Manifest identity and hashing helpers.
  """

  alias Favn.Manifest.Serializer

  @default_algorithm :sha256

  @type algorithm :: :sha256
  @type error :: {:unsupported_algorithm, term()} | {:encode_failed, term()}

  @spec hash_manifest(map() | struct(), keyword()) :: {:ok, String.t()} | {:error, error()}
  def hash_manifest(manifest, opts \\ []) when is_list(opts) do
    algorithm = Keyword.get(opts, :algorithm, @default_algorithm)

    with :ok <- validate_algorithm(algorithm),
         {:ok, bytes} <- Serializer.encode_manifest(manifest) do
      hash = :crypto.hash(algorithm, bytes) |> Base.encode16(case: :lower)
      {:ok, hash}
    end
  end

  @spec hash_manifest!(map() | struct(), keyword()) :: String.t()
  def hash_manifest!(manifest, opts \\ []) when is_list(opts) do
    case hash_manifest(manifest, opts) do
      {:ok, hash} -> hash
      {:error, reason} -> raise ArgumentError, "cannot hash manifest: #{inspect(reason)}"
    end
  end

  @spec identity(map() | struct(), keyword()) :: {:ok, map()} | {:error, error()}
  def identity(manifest, opts \\ []) when is_list(opts) do
    with {:ok, content_hash} <- hash_manifest(manifest, opts) do
      {:ok,
       %{
         algorithm: Atom.to_string(Keyword.get(opts, :algorithm, @default_algorithm)),
         encoding: "hex",
         content_hash: content_hash
       }}
    end
  end

  defp validate_algorithm(:sha256), do: :ok
  defp validate_algorithm(other), do: {:error, {:unsupported_algorithm, other}}
end
