defmodule FavnOrchestrator.Persistence.IdentityStore do
  @moduledoc "Persistence contract for actors, memberships, grants, sessions, and audit."

  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.CompleteOperatorCommand
  alias FavnOrchestrator.Persistence.Commands.AttachActorMembership
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.RecordAudit
  alias FavnOrchestrator.Persistence.Commands.ReserveOperatorCommand
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.RotateWorkspaceSession
  alias FavnOrchestrator.Persistence.Commands.SetActorAccess
  alias FavnOrchestrator.Persistence.Commands.SetActorStatus
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.ListActorMemberships
  alias FavnOrchestrator.Persistence.Queries.PageActors
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Queries.PageSessions
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Results.AuditEntry
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Session
  alias FavnOrchestrator.Persistence.Results.WorkspaceMembership

  @callback create_actor(CreateActor.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback get_actor(GetActor.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback page_actors(PageActors.t()) ::
              {:ok, CursorPage.t(Actor.t())} | {:error, Error.t()}
  @callback list_actor_memberships(ListActorMemberships.t()) ::
              {:ok, [WorkspaceMembership.t()]} | {:error, Error.t()}
  @callback set_access(SetActorAccess.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback attach_actor_membership(AttachActorMembership.t()) ::
              {:ok, Actor.t()} | {:error, Error.t()}
  @callback set_actor_status(SetActorStatus.t()) :: :ok | {:error, Error.t()}
  @callback change_password(ChangeActorPassword.t()) :: :ok | {:error, Error.t()}
  @callback create_session(CreateSession.t()) :: {:ok, Session.t()} | {:error, Error.t()}
  @callback rotate_workspace_session(RotateWorkspaceSession.t()) ::
              {:ok, Session.t()} | {:error, Error.t()}
  @callback get_session(GetSession.t()) :: {:ok, Session.t()} | {:error, Error.t()}
  @callback page_sessions(PageSessions.t()) ::
              {:ok, CursorPage.t(Session.t())} | {:error, Error.t()}
  @callback revoke_sessions(RevokeSessions.t()) :: :ok | {:error, Error.t()}
  @callback record_audit(RecordAudit.t()) :: :ok | {:error, Error.t()}
  @callback reserve_operator_command(ReserveOperatorCommand.t()) ::
              {:ok, map()} | {:error, Error.t()}
  @callback complete_operator_command(CompleteOperatorCommand.t()) ::
              :ok | {:error, Error.t()}
  @callback page_audit(PageAudit.t()) ::
              {:ok, CursorPage.t(AuditEntry.t())} | {:error, Error.t()}
end
