defmodule FavnOrchestrator.ExecutionPackages do
  @moduledoc false

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.Index
  alias Favn.Manifest.Version
  alias Favn.Manifest.Serializer
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.RegisterExecutionPackages
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetExecutionPackage
  alias FavnOrchestrator.Persistence.Queries.MissingExecutionPackageHashes
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.Persistence.VerifiedExecutionPackage
  alias FavnOrchestrator.MemoryCapacity.BoundedWorker
  alias FavnOrchestrator.MemoryCapacity.Budget

  @max_packages 8
  @max_package_bytes 4 * 1_024 * 1_024
  @max_batch_bytes 4 * 1_024 * 1_024

  @doc "Registers immutable packages before their compact manifest index."
  @spec register(PlatformContext.t(), [ExecutionPackage.t()]) :: :ok | {:error, term()}
  def register(%PlatformContext{} = context, packages) when is_list(packages) do
    with {:ok, verified} <-
           BoundedWorker.run(fn -> verify_batch(packages) end, Budget.manifest_base()) do
      Persistence.stores().registry.register_execution_packages(%RegisterExecutionPackages{
        platform_context: context,
        packages: verified
      })
    end
  end

  defp verify_batch(packages) when length(packages) <= @max_packages do
    packages
    |> Enum.reduce_while({:ok, [], MapSet.new(), 0}, fn package,
                                                        {:ok, acc, hashes, total_bytes} ->
      with %ExecutionPackage{} <- package,
           {:ok, canonical} <- ExecutionPackage.verify(package),
           false <- MapSet.member?(hashes, canonical.content_hash),
           {:ok, encoded} <- Serializer.encode_manifest(canonical),
           true <- byte_size(encoded) <= @max_package_bytes,
           true <- total_bytes + byte_size(encoded) <= @max_batch_bytes do
        {module, name} = canonical.asset_ref

        envelope = %VerifiedExecutionPackage{
          content_hash: canonical.content_hash,
          asset_module: Atom.to_string(module),
          asset_name: Atom.to_string(name),
          runtime_input_resolver: runtime_input_resolver(canonical),
          canonical_json: encoded
        }

        {:cont,
         {:ok, [envelope | acc], MapSet.put(hashes, canonical.content_hash),
          total_bytes + byte_size(encoded)}}
      else
        true -> {:halt, {:error, :duplicate_execution_package}}
        false -> {:halt, {:error, :execution_package_batch_too_large}}
        {:error, reason} -> {:halt, {:error, reason}}
        _invalid -> {:halt, {:error, :invalid_execution_package}}
      end
    end)
    |> case do
      {:ok, envelopes, _hashes, _bytes} -> {:ok, Enum.reverse(envelopes)}
      {:error, _reason} = error -> error
    end
  end

  defp verify_batch(_packages), do: {:error, :too_many_execution_packages}

  defp runtime_input_resolver(%ExecutionPackage{
         sql_execution: %{runtime_inputs: %{module: module}}
       })
       when is_atom(module),
       do: Atom.to_string(module)

  defp runtime_input_resolver(%ExecutionPackage{}), do: nil

  @doc "Returns the requested package hashes that are not registered."
  @spec missing_hashes(PlatformContext.t(), [String.t()]) ::
          {:ok, [String.t()]} | {:error, term()}
  def missing_hashes(%PlatformContext{} = context, hashes) when is_list(hashes) do
    Persistence.stores().registry.missing_execution_package_hashes(%MissingExecutionPackageHashes{
      platform_context: context,
      hashes: hashes
    })
  end

  @doc "Fetches, verifies, and attaches the package required by one runtime work item."
  @spec attach(WorkspaceContext.t(), String.t(), RunnerWork.t(), Version.t(), Index.t()) ::
          {:ok, RunnerWork.t()} | {:error, term()}
  def attach(
        %WorkspaceContext{} = context,
        deployment_id,
        %RunnerWork{} = work,
        %Version{} = version,
        %Index{} = index
      ) do
    with :ok <- validate_workspace_read(context),
         {:ok, asset} <- Index.fetch_asset(index, RunnerWork.asset_ref(work)),
         {:ok, package} <- fetch_for_asset(context, deployment_id, version, asset),
         {:ok, canonical} <- ExecutionPackage.verify_for_asset(package, asset) do
      {:ok, %{work | execution_package: canonical}}
    end
  end

  @doc """
  Fetches and verifies one asset's package so its source can be read.

  Returns `{:ok, nil}` for an asset that has no package, which is every asset that is
  not SQL — the caller shows what that asset type has instead rather than treating the
  absence as a failure.
  """
  @spec fetch(WorkspaceContext.t(), String.t(), Version.t(), Asset.t()) ::
          {:ok, ExecutionPackage.t() | nil} | {:error, term()}
  def fetch(%WorkspaceContext{} = context, deployment_id, %Version{} = version, %Asset{} = asset) do
    with :ok <- validate_workspace_read(context),
         {:ok, package} <- fetch_for_asset(context, deployment_id, version, asset) do
      ExecutionPackage.verify_for_asset(package, asset)
    end
  end

  defp fetch_for_asset(
         context,
         deployment_id,
         version,
         %Asset{ref: asset_ref, type: :sql, execution_package_hash: hash}
       )
       when is_binary(deployment_id) and is_binary(hash) do
    Persistence.stores().registry.get_execution_package(%GetExecutionPackage{
      workspace_context: context,
      deployment_id: deployment_id,
      manifest_version_id: version.manifest_version_id,
      asset_ref: asset_ref,
      content_hash: hash
    })
  end

  defp fetch_for_asset(_context, deployment_id, _version, %Asset{type: :sql})
       when is_binary(deployment_id),
       do: {:error, :execution_package_required}

  defp fetch_for_asset(_context, _deployment_id, _version, %Asset{type: :sql}),
    do: {:error, :execution_package_deployment_required}

  defp fetch_for_asset(_context, _deployment_id, _version, %Asset{}), do: {:ok, nil}

  defp validate_workspace_read(%WorkspaceContext{} = context) do
    if WorkspaceContext.valid?(context) and
         Enum.any?(
           context.roles,
           &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
         ),
       do: :ok,
       else: {:error, Error.new(:forbidden, "workspace read role required")}
  end
end
