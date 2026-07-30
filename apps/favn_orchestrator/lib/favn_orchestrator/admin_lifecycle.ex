defmodule FavnOrchestrator.AdminLifecycle do
  @moduledoc """
  Explicit first-administrator bootstrap and break-glass credential recovery.

  These operations are for a trusted host/release command, not a browser or
  normal application startup. Passwords are validated and hashed before they
  cross the persistence boundary.
  """

  alias FavnOrchestrator.Auth.Credentials
  alias FavnOrchestrator.Events
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.BootstrapAdministrator
  alias FavnOrchestrator.Persistence.Commands.RecoverAdministratorCredential
  alias FavnOrchestrator.Persistence.Commands.ResetActorCredential
  alias FavnOrchestrator.Persistence.Commands.SetActorStatus
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetGlobalActor

  @principal_id "release:admin-lifecycle"
  @grant_id "release:admin-lifecycle"

  @spec bootstrap([String.t()], String.t(), String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def bootstrap(workspace_ids, username, password, display_name, opts \\ [])

  def bootstrap(workspace_ids, username, password, display_name, opts)
      when is_list(workspace_ids) and is_binary(username) and is_binary(password) and
             is_binary(display_name) and is_list(opts) do
    with {:ok, workspace_ids} <- normalize_workspace_ids(workspace_ids),
         {:ok, actor} <- Credentials.normalize_actor(username, display_name, [:admin]),
         :ok <- Credentials.validate_password(password),
         {:ok, context} <- platform_context(),
         {:ok, result} <-
           identity_store(opts).bootstrap_administrator(%BootstrapAdministrator{
             platform_context: context,
             command_id: operation_id("admin-bootstrap"),
             actor_id: deterministic_actor_id(actor.username),
             workspace_ids: workspace_ids,
             username: actor.username,
             display_name: actor.display_name,
             password_hash: Credentials.hash_password(password).password_hash,
             occurred_at: DateTime.utc_now()
           }) do
      {:ok, actor_result(result)}
    end
  end

  def bootstrap(_workspace_ids, _username, _password, _display_name, _opts),
    do: {:error, :invalid_admin_bootstrap}

  @spec recover(String.t(), String.t(), keyword()) ::
          {:ok, %{actor_id: String.t(), username: String.t()}} | {:error, term()}
  def recover(username, password, opts \\ [])

  def recover(username, password, opts)
      when is_binary(username) and is_binary(password) and is_list(opts) do
    with {:ok, actor} <- Credentials.normalize_actor(username, "Recovery", [:admin]),
         :ok <- Credentials.validate_password(password),
         {:ok, context} <- platform_context(),
         {:ok, actor_id} <-
           identity_store(opts).recover_administrator_credential(%RecoverAdministratorCredential{
             platform_context: context,
             command_id: operation_id("admin-recover"),
             username: actor.username,
             password_hash: Credentials.hash_password(password).password_hash,
             occurred_at: DateTime.utc_now()
           }) do
      Events.broadcast_actor_changed(actor_id)
      {:ok, %{actor_id: actor_id, username: actor.username}}
    end
  end

  def recover(_username, _password, _opts), do: {:error, :invalid_admin_recovery}

  @doc """
  Rotates one exact global actor password from a trusted platform command.

  Unlike administrator recovery, this preserves the actor's current status and
  grants. Every session is revoked across all workspaces.
  """
  @spec reset_actor_credential(String.t(), String.t(), keyword()) ::
          {:ok, %{actor_id: String.t(), username: String.t()}} | {:error, term()}
  def reset_actor_credential(username, password, opts \\ [])

  def reset_actor_credential(username, password, opts)
      when is_binary(username) and is_binary(password) and is_list(opts) do
    with {:ok, actor} <- Credentials.normalize_actor(username, "Credential reset", [:viewer]),
         :ok <- Credentials.validate_password(password),
         {:ok, context} <- platform_context(),
         {:ok, actor_id} <-
           identity_store(opts).reset_actor_credential(%ResetActorCredential{
             platform_context: context,
             command_id: operation_id("actor-credential-reset"),
             username: actor.username,
             password_hash: Credentials.hash_password(password).password_hash,
             occurred_at: DateTime.utc_now()
           }) do
      Events.broadcast_actor_changed(actor_id)
      {:ok, %{actor_id: actor_id, username: actor.username}}
    end
  end

  def reset_actor_credential(_username, _password, _opts),
    do: {:error, :invalid_actor_credential_reset}

  @doc """
  Enables or disables one exact global actor from a trusted release command.

  Disabling revokes every actor session in every workspace. Workspace
  administrators deliberately cannot invoke this platform-global operation.
  """
  @spec set_actor_status(String.t(), :active | :disabled, keyword()) ::
          {:ok, map()} | {:error, term()}
  def set_actor_status(username, status, opts \\ [])

  def set_actor_status(username, status, opts)
      when is_binary(username) and status in [:active, :disabled] and is_list(opts) do
    with {:ok, actor_input} <- Credentials.normalize_actor(username, "Actor status", [:admin]),
         {:ok, context} <- platform_context(),
         store = identity_store(opts),
         {:ok, actor} <-
           store.get_global_actor(%GetGlobalActor{
             platform_context: context,
             username: actor_input.username
           }),
         :ok <- update_actor_status(store, context, actor, status) do
      Events.broadcast_actor_changed(actor.actor_id)

      {:ok,
       %{
         actor_id: actor.actor_id,
         username: actor.username,
         status: status,
         sessions_revoked: status == :disabled
       }}
    end
  end

  def set_actor_status(_username, _status, _opts), do: {:error, :invalid_actor_status}

  defp platform_context do
    PlatformContext.new(@principal_id, @grant_id, [:platform_admin])
  end

  defp identity_store(opts) do
    Keyword.get_lazy(opts, :identity_store, fn -> Persistence.stores().identity end)
  end

  defp normalize_workspace_ids(workspace_ids) do
    normalized =
      workspace_ids
      |> Enum.filter(&is_binary/1)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()
      |> Enum.sort()

    if normalized != [] and length(normalized) == length(Enum.uniq(workspace_ids)),
      do: {:ok, normalized},
      else: {:error, :workspace_ids_required}
  end

  defp deterministic_actor_id(username) do
    digest =
      username
      |> String.normalize(:nfkc)
      |> String.downcase()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    "act_admin_" <> String.slice(digest, 0, 32)
  end

  defp operation_id(prefix) do
    prefix <> ":" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end

  defp update_actor_status(store, context, actor, status) do
    store.set_actor_status(%SetActorStatus{
      platform_context: context,
      command_id: operation_id("actor-status"),
      actor_id: actor.actor_id,
      status: status,
      expected_version: actor.version,
      occurred_at: DateTime.utc_now()
    })
  end

  defp actor_result(actor) do
    %{
      actor_id: actor.actor_id,
      username: actor.username,
      workspace_id: actor.workspace_id
    }
  end
end
