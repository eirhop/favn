defmodule FavnOrchestrator.WorkspaceProvisioning do
  @moduledoc """
  Provisions one workspace together with exactly one initial administrator.

  This trusted one-off use case is provider-neutral and never runs during
  resident application startup. The caller chooses either an Entra identity,
  keyed by immutable tenant and object IDs, or a local password supplied over a
  protected channel. The persistence implementation commits the complete
  readiness state and operation receipt atomically.
  """

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Idempotency
  alias FavnOrchestrator.Persistence.Commands.ProvisionWorkspaceAdministrator
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetWorkspaceProvisioning
  alias FavnOrchestrator.Persistence.Results.WorkspaceProvisioning

  @principal_id "release:workspace-provisioning"
  @slug_pattern ~r/\A[a-z0-9][a-z0-9-]{0,62}\z/
  @uuid_pattern ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/

  @doc """
  Provisions or exactly replays one workspace-administrator request.

  Required options are `:store`, implementing `WorkspaceProvisioningStore`,
  and `:fingerprint_key`, a deployment-stable secret key of at least 32 bytes.
  """
  @spec provision(map(), keyword()) :: {:ok, WorkspaceProvisioning.t()} | {:error, term()}
  def provision(input, opts) when is_map(input) and is_list(opts) do
    with {:ok, store} <- required_store(opts),
         {:ok, fingerprint_key} <- fingerprint_key(opts),
         {:ok, normalized} <- normalize(input),
         {:ok, context} <- platform_context(normalized.operation_id),
         request_fingerprint <- Idempotency.request_hmac(normalized, fingerprint_key),
         command <- command(context, normalized, request_fingerprint),
         {:ok, result} <- store.provision(command) do
      {:ok, result}
    end
  end

  def provision(_input, _opts), do: {:error, :invalid_workspace_provisioning}

  @doc false
  @spec validate(map()) :: :ok | {:error, term()}
  def validate(input) when is_map(input) do
    case normalize(input) do
      {:ok, _normalized} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def validate(_input), do: {:error, :invalid_workspace_provisioning}

  @doc "Returns the persisted, redacted readiness result for one workspace."
  @spec status(String.t(), keyword()) :: {:ok, WorkspaceProvisioning.t()} | {:error, term()}
  def status(workspace_id, opts) when is_binary(workspace_id) and is_list(opts) do
    with {:ok, store} <- required_store(opts),
         :ok <- validate_identifier(workspace_id),
         {:ok, context} <- platform_context("workspace-status:" <> workspace_id) do
      store.get(%GetWorkspaceProvisioning{
        platform_context: context,
        workspace_id: workspace_id
      })
    end
  end

  def status(_workspace_id, _opts), do: {:error, :invalid_workspace}

  defp normalize(input) do
    with {:ok, operation_id} <- required_string(input, :operation_id),
         :ok <- validate_identifier(operation_id),
         {:ok, workspace} <- required_map(input, :workspace),
         {:ok, workspace_id} <- required_string(workspace, :id),
         :ok <- validate_identifier(workspace_id),
         {:ok, slug} <- optional_string(workspace, :slug, workspace_id),
         :ok <- validate_slug(slug),
         {:ok, workspace_name} <- optional_string(workspace, :display_name, slug),
         :ok <- validate_display_value(workspace_name),
         {:ok, administrator} <- required_map(input, :administrator),
         {:ok, username} <- required_string(administrator, :username),
         {:ok, display_name} <- optional_string(administrator, :display_name, username),
         {:ok, actor} <- Credentials.normalize_actor(username, display_name, [:admin]),
         {:ok, authentication} <- normalize_authentication(administrator) do
      {:ok,
       Map.merge(authentication, %{
         operation_id: operation_id,
         workspace_id: workspace_id,
         slug: slug,
         workspace_name: workspace_name,
         actor_id: deterministic_actor_id(actor.username),
         username: actor.username,
         display_name: actor.display_name
       })}
    end
  end

  defp normalize_authentication(administrator) do
    case value(administrator, :mode) do
      mode when mode in [:entra, "entra"] ->
        with {:ok, tenant_id} <- required_string(administrator, :tenant_id),
             {:ok, tenant_id} <- normalize_uuid(tenant_id),
             {:ok, object_id} <- required_string(administrator, :object_id),
             {:ok, object_id} <- normalize_uuid(object_id),
             :ok <- reject_password(administrator) do
          {:ok,
           %{
             authentication_mode: :entra,
             tenant_id: tenant_id,
             object_id: object_id,
             password: nil
           }}
        end

      mode when mode in [:password, "password"] ->
        with {:ok, password} <- required_string(administrator, :password),
             :ok <- Credentials.validate_password(password),
             :ok <- reject_entra_identity(administrator) do
          {:ok,
           %{
             authentication_mode: :password,
             tenant_id: nil,
             object_id: nil,
             password: password
           }}
        end

      _other ->
        {:error, :exactly_one_administrator_mode_required}
    end
  end

  defp command(context, normalized, request_fingerprint) do
    %ProvisionWorkspaceAdministrator{
      platform_context: context,
      operation_id: normalized.operation_id,
      request_fingerprint: request_fingerprint,
      workspace_id: normalized.workspace_id,
      slug: normalized.slug,
      workspace_name: normalized.workspace_name,
      actor_id: normalized.actor_id,
      username: normalized.username,
      display_name: normalized.display_name,
      authentication_mode: normalized.authentication_mode,
      password_hash: password_hash(normalized.password),
      tenant_id: normalized.tenant_id,
      object_id: normalized.object_id,
      occurred_at: DateTime.utc_now()
    }
  end

  defp platform_context(request_id) do
    bounded_request_id =
      "workspace-provisioning:" <> String.slice(Idempotency.key_hash(request_id), 0, 32)

    PlatformContext.new(@principal_id, @principal_id, [:platform_admin],
      request_id: bounded_request_id
    )
  end

  defp required_store(opts) do
    case Keyword.get(opts, :store) do
      store when is_atom(store) -> {:ok, store}
      _missing -> {:error, :workspace_provisioning_store_required}
    end
  end

  defp fingerprint_key(opts) do
    case Keyword.get(opts, :fingerprint_key) do
      key when is_binary(key) and byte_size(key) >= 32 -> {:ok, key}
      _missing -> {:error, :workspace_provisioning_fingerprint_key_required}
    end
  end

  defp password_hash(nil), do: nil
  defp password_hash(password), do: Credentials.hash_password(password).password_hash

  defp deterministic_actor_id(username) do
    digest =
      username
      |> String.normalize(:nfkc)
      |> String.downcase()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "act_admin_" <> String.slice(digest, 0, 32)
  end

  defp reject_password(map) do
    if is_nil(value(map, :password)), do: :ok, else: {:error, :multiple_administrator_modes}
  end

  defp reject_entra_identity(map) do
    if is_nil(value(map, :tenant_id)) and is_nil(value(map, :object_id)),
      do: :ok,
      else: {:error, :multiple_administrator_modes}
  end

  defp normalize_uuid(value) do
    normalized = value |> String.trim() |> String.downcase()

    if Regex.match?(@uuid_pattern, normalized),
      do: {:ok, normalized},
      else: {:error, :invalid_entra_identity}
  end

  defp validate_identifier(value) when byte_size(value) in 1..255, do: :ok
  defp validate_identifier(_value), do: {:error, :invalid_identifier}

  defp validate_slug(value) do
    if Regex.match?(@slug_pattern, value), do: :ok, else: {:error, :invalid_workspace_slug}
  end

  defp validate_display_value(value) when byte_size(value) in 1..255, do: :ok
  defp validate_display_value(_value), do: {:error, :invalid_workspace_name}

  defp required_map(map, key) do
    case value(map, key) do
      child when is_map(child) -> {:ok, child}
      _missing -> {:error, {:required_map, key}}
    end
  end

  defp required_string(map, key) do
    case value(map, key) do
      child when is_binary(child) ->
        child = String.trim(child)
        if child == "", do: {:error, {:required_string, key}}, else: {:ok, child}

      _missing ->
        {:error, {:required_string, key}}
    end
  end

  defp optional_string(map, key, default) do
    case value(map, key) do
      nil ->
        {:ok, default}

      child when is_binary(child) ->
        child = String.trim(child)
        if child == "", do: {:error, {:invalid_string, key}}, else: {:ok, child}

      _invalid ->
        {:error, {:invalid_string, key}}
    end
  end

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end
