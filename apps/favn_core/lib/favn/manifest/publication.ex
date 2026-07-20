defmodule Favn.Manifest.Publication do
  @moduledoc """
  Complete publishable result of one manifest build.

  A publication contains a compact pinned manifest index plus the exact set of
  immutable execution packages referenced by that index.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Build
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Version

  @enforce_keys [:version, :execution_packages]
  defstruct [:version, execution_packages: []]

  @type t :: %__MODULE__{
          version: Version.t(),
          execution_packages: [ExecutionPackage.t()]
        }

  @type error ::
          :invalid_manifest_publication
          | {:duplicate_execution_package, String.t()}
          | {:missing_execution_packages, [String.t()]}
          | {:unexpected_execution_packages, [String.t()]}
          | {:execution_package_asset_mismatch, String.t(), Favn.Ref.t(), Favn.Ref.t()}
          | {:incompatible_execution_package_schema, String.t(), pos_integer(), pos_integer()}
          | ExecutionPackage.error()
          | Version.error()

  @doc "Pins a build and verifies exact package coverage."
  @spec new(Build.t(), keyword()) :: {:ok, t()} | {:error, error()}
  def new(%Build{} = build, opts \\ []) when is_list(opts) do
    with {:ok, version} <- Version.new(build.manifest, opts) do
      from_parts(version, build.execution_packages)
    end
  end

  @doc "Verifies an already-pinned index and its package set."
  @spec from_parts(Version.t(), [ExecutionPackage.t() | map()]) ::
          {:ok, t()} | {:error, error()}
  def from_parts(%Version{} = version, packages) when is_list(packages) do
    with {:ok, canonical_version} <- Version.verify(version),
         {:ok, canonical_packages} <- verify_packages(packages),
         :ok <- validate_coverage(canonical_version, canonical_packages),
         :ok <- validate_package_schemas(canonical_version, canonical_packages) do
      {:ok, %__MODULE__{version: canonical_version, execution_packages: canonical_packages}}
    end
  end

  def from_parts(_version, _packages), do: {:error, :invalid_manifest_publication}

  @doc "Returns packages keyed by canonical content hash."
  @spec packages_by_hash(t()) :: %{String.t() => ExecutionPackage.t()}
  def packages_by_hash(%__MODULE__{execution_packages: packages}) do
    Map.new(packages, &{&1.content_hash, &1})
  end

  @doc "Returns the sorted unique execution-package hashes referenced by an index."
  @spec required_package_hashes(Version.t()) :: [String.t()]
  def required_package_hashes(%Version{} = version) do
    version
    |> required_package_refs()
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
  end

  @doc "Returns package hash and asset-ref pairs referenced by the compact index."
  @spec required_package_refs(Version.t()) :: [{String.t(), Favn.Ref.t()}]
  def required_package_refs(%Version{manifest: %{assets: assets}}) do
    assets
    |> Enum.flat_map(fn
      %Asset{execution_package_hash: hash, ref: ref} when is_binary(hash) -> [{hash, ref}]
      _asset -> []
    end)
    |> Enum.sort()
  end

  defp verify_packages(packages) do
    packages
    |> Enum.reduce_while({:ok, [], MapSet.new()}, fn package, {:ok, acc, hashes} ->
      case ExecutionPackage.from_published(package) do
        {:ok, canonical} ->
          if MapSet.member?(hashes, canonical.content_hash) do
            {:halt, {:error, {:duplicate_execution_package, canonical.content_hash}}}
          else
            {:cont, {:ok, [canonical | acc], MapSet.put(hashes, canonical.content_hash)}}
          end

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, verified, _hashes} ->
        {:ok, Enum.sort_by(verified, & &1.content_hash)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_coverage(%Version{manifest: %{assets: assets}}, packages) do
    required =
      assets
      |> Enum.flat_map(fn
        %Asset{execution_package_hash: hash, ref: ref} when is_binary(hash) -> [{hash, ref}]
        %Asset{} -> []
      end)
      |> Map.new()

    provided = Map.new(packages, &{&1.content_hash, &1.asset_ref})
    missing = required |> Map.keys() |> Kernel.--(Map.keys(provided)) |> Enum.sort()
    unexpected = provided |> Map.keys() |> Kernel.--(Map.keys(required)) |> Enum.sort()

    cond do
      missing != [] ->
        {:error, {:missing_execution_packages, missing}}

      unexpected != [] ->
        {:error, {:unexpected_execution_packages, unexpected}}

      true ->
        Enum.reduce_while(required, :ok, fn {hash, expected_ref}, :ok ->
          actual_ref = Map.fetch!(provided, hash)

          if actual_ref == expected_ref do
            {:cont, :ok}
          else
            {:halt, {:error, {:execution_package_asset_mismatch, hash, expected_ref, actual_ref}}}
          end
        end)
    end
  end

  defp validate_package_schemas(%Version{runner_contract_version: 8}, packages) do
    case Enum.find(packages, &(&1.schema_version != 1)) do
      nil ->
        :ok

      package ->
        {:error,
         {:incompatible_execution_package_schema, package.content_hash, package.schema_version, 1}}
    end
  end

  defp validate_package_schemas(%Version{}, _packages), do: :ok
end
