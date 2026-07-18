defmodule FavnOrchestrator.RunnerManifestRegistration do
  @moduledoc false

  alias Favn.Manifest.Version

  @spec ensure(module(), Version.t(), keyword()) :: :ok | {:error, term()}
  def ensure(runner_client, %Version{} = version, opts)
      when is_atom(runner_client) and is_list(opts) do
    case runner_client.ensure_manifest(
           version.manifest_version_id,
           version.content_hash,
           opts
         ) do
      :ok -> :ok
      :missing -> runner_client.register_manifest(version, opts)
      {:error, _reason} = error -> error
    end
  end

  @spec acquire(module(), Version.t(), String.t(), DateTime.t(), [Favn.Ref.t()], keyword()) ::
          :ok | {:error, term()}
  def acquire(
        runner_client,
        %Version{} = version,
        lease_id,
        %DateTime{} = expires_at,
        planned_asset_refs,
        opts
      )
      when is_atom(runner_client) and is_binary(lease_id) and is_list(planned_asset_refs) and
             is_list(opts) do
    runner_client.acquire_manifest(version, lease_id, expires_at, planned_asset_refs, opts)
  end

  @spec renew(module(), String.t() | nil, DateTime.t(), keyword()) :: :ok | {:error, term()}
  def renew(_runner_client, nil, %DateTime{}, _opts), do: :ok

  def renew(runner_client, lease_id, %DateTime{} = expires_at, opts)
      when is_atom(runner_client) and is_binary(lease_id) and is_list(opts) do
    runner_client.renew_manifest(lease_id, expires_at, opts)
  end

  @spec release(module(), String.t() | nil, keyword()) :: :ok
  def release(_runner_client, nil, _opts), do: :ok

  def release(runner_client, lease_id, opts)
      when is_atom(runner_client) and is_binary(lease_id) and is_list(opts) do
    runner_client.release_manifest(lease_id, opts)
  end
end
