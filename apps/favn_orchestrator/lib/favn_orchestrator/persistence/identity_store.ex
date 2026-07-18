defmodule FavnOrchestrator.Persistence.IdentityStore do
  @moduledoc "Persistence contract for actors, memberships, grants, sessions, and audit."

  alias FavnOrchestrator.Persistence.Commands.ChangeActorPassword
  alias FavnOrchestrator.Persistence.Commands.CreateActor
  alias FavnOrchestrator.Persistence.Commands.CreateSession
  alias FavnOrchestrator.Persistence.Commands.RecordAudit
  alias FavnOrchestrator.Persistence.Commands.RevokeSessions
  alias FavnOrchestrator.Persistence.Commands.SetActorAccess
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetActor
  alias FavnOrchestrator.Persistence.Queries.GetSession
  alias FavnOrchestrator.Persistence.Queries.PageActors
  alias FavnOrchestrator.Persistence.Queries.PageAudit
  alias FavnOrchestrator.Persistence.Results.Actor
  alias FavnOrchestrator.Persistence.Results.AuditEntry
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.Session

  @callback create_actor(CreateActor.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback get_actor(GetActor.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback page_actors(PageActors.t()) ::
              {:ok, CursorPage.t(Actor.t())} | {:error, Error.t()}
  @callback set_access(SetActorAccess.t()) :: {:ok, Actor.t()} | {:error, Error.t()}
  @callback change_password(ChangeActorPassword.t()) :: :ok | {:error, Error.t()}
  @callback create_session(CreateSession.t()) :: {:ok, Session.t()} | {:error, Error.t()}
  @callback get_session(GetSession.t()) :: {:ok, Session.t()} | {:error, Error.t()}
  @callback revoke_sessions(RevokeSessions.t()) :: :ok | {:error, Error.t()}
  @callback record_audit(RecordAudit.t()) :: :ok | {:error, Error.t()}
  @callback page_audit(PageAudit.t()) ::
              {:ok, CursorPage.t(AuditEntry.t())} | {:error, Error.t()}
end
