defmodule FavnOrchestrator.Persistence.Commands.CreateActor do
  @moduledoc "Creates a global actor, current credential, and initial workspace membership."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :actor_id,
    :username,
    :display_name,
    :password_hash,
    :roles,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :actor_id,
    :username,
    :display_name,
    :password_hash,
    :roles,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          actor_id: String.t(),
          username: String.t(),
          display_name: String.t(),
          password_hash: String.t(),
          roles: [atom()],
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Selectors.ActorById do
  @moduledoc "Selects one actor by stable global ID."
  @enforce_keys [:actor_id]
  defstruct [:actor_id]
  @type t :: %__MODULE__{actor_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Selectors.ActorByUsername do
  @moduledoc "Selects one actor by normalized global username."
  @enforce_keys [:username]
  defstruct [:username]
  @type t :: %__MODULE__{username: String.t()}
end

defmodule FavnOrchestrator.Persistence.Queries.GetActor do
  @moduledoc "Fetches global identity, current credential, and membership in one workspace."

  alias FavnOrchestrator.Persistence.Selectors.ActorById
  alias FavnOrchestrator.Persistence.Selectors.ActorByUsername
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :selector]
  defstruct [:workspace_context, :selector]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          selector: ActorById.t() | ActorByUsername.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageActors do
  @moduledoc "Keyset-pages actors with memberships in one workspace."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context]
  defstruct [:workspace_context, :status, :after, limit: 100]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          status: atom() | nil,
          after: %{actor_id: String.t()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Commands.SetActorAccess do
  @moduledoc "Changes one workspace membership or explicitly authorized platform grant."

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :authority,
    :command_id,
    :actor_id,
    :scope_kind,
    :roles,
    :status,
    :expected_version,
    :occurred_at
  ]
  defstruct [
    :authority,
    :command_id,
    :actor_id,
    :scope_kind,
    :workspace_id,
    :roles,
    :status,
    :expected_version,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          authority: WorkspaceContext.t() | PlatformContext.t(),
          command_id: String.t(),
          actor_id: String.t(),
          scope_kind: :workspace | :platform,
          workspace_id: String.t() | nil,
          roles: [atom()],
          status: :active | :suspended | :revoked,
          expected_version: non_neg_integer(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.ChangeActorPassword do
  @moduledoc "Replaces one current credential hash and optionally revokes active sessions."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :actor_id, :password_hash, :occurred_at]
  defstruct [
    :workspace_context,
    :command_id,
    :actor_id,
    :password_hash,
    :occurred_at,
    revoke_sessions?: true
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          actor_id: String.t(),
          password_hash: String.t(),
          occurred_at: DateTime.t(),
          revoke_sessions?: boolean()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.CreateSession do
  @moduledoc "Persists one opaque session token hash with an absolute expiry."

  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :workspace_context,
    :command_id,
    :session_id,
    :actor_id,
    :token_hash,
    :provider,
    :expires_at,
    :occurred_at
  ]
  defstruct [
    :workspace_context,
    :command_id,
    :session_id,
    :actor_id,
    :token_hash,
    :provider,
    :expires_at,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          session_id: String.t(),
          actor_id: String.t(),
          token_hash: binary(),
          provider: String.t(),
          expires_at: DateTime.t(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Selectors.SessionById do
  @moduledoc "Selects one session by stable ID."
  @enforce_keys [:session_id]
  defstruct [:session_id]
  @type t :: %__MODULE__{session_id: String.t()}
end

defmodule FavnOrchestrator.Persistence.Selectors.SessionByTokenHash do
  @moduledoc "Selects one session by an already-derived opaque token hash."
  @enforce_keys [:token_hash]
  defstruct [:token_hash]
  @type t :: %__MODULE__{token_hash: binary()}
end

defmodule FavnOrchestrator.Persistence.Queries.GetSession do
  @moduledoc "Resolves one session only when its actor belongs to the command workspace."

  alias FavnOrchestrator.Persistence.Selectors.SessionById
  alias FavnOrchestrator.Persistence.Selectors.SessionByTokenHash
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :selector]
  defstruct [:workspace_context, :selector]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          selector: SessionById.t() | SessionByTokenHash.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RevokeSessions do
  @moduledoc "Idempotently revokes one session or every active session for one actor."

  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:workspace_context, :command_id, :occurred_at]
  defstruct [:workspace_context, :command_id, :session_id, :actor_id, :occurred_at]

  @type t :: %__MODULE__{
          workspace_context: WorkspaceContext.t(),
          command_id: String.t(),
          session_id: String.t() | nil,
          actor_id: String.t() | nil,
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Commands.RecordAudit do
  @moduledoc "Appends one redacted workspace or platform audit record idempotently."

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :scope,
    :command_id,
    :action,
    :subject_kind,
    :subject_id,
    :detail,
    :occurred_at
  ]
  defstruct [
    :scope,
    :command_id,
    :action,
    :subject_kind,
    :subject_id,
    :detail,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          command_id: String.t(),
          action: String.t(),
          subject_kind: String.t(),
          subject_id: String.t(),
          detail: map(),
          occurred_at: DateTime.t()
        }
end

defmodule FavnOrchestrator.Persistence.Queries.PageAudit do
  @moduledoc "Keyset-pages workspace or platform authorization audit entries."

  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.WorkspaceContext
  @enforce_keys [:scope]
  defstruct [:scope, :after, limit: 100]

  @type t :: %__MODULE__{
          scope: WorkspaceContext.t() | PlatformContext.t(),
          after: %{audit_id: pos_integer()} | nil,
          limit: 1..500
        }
end

defmodule FavnOrchestrator.Persistence.Results.Actor do
  @moduledoc "Global actor plus one explicit workspace membership and current credential hash."
  @enforce_keys [:actor_id, :username, :display_name, :status, :workspace_id, :roles]
  defstruct [
    :actor_id,
    :username,
    :display_name,
    :status,
    :workspace_id,
    :membership_status,
    :roles,
    :credential_hash,
    :credential_version,
    :access_version,
    :version
  ]

  @type t :: %__MODULE__{
          actor_id: String.t(),
          username: String.t(),
          display_name: String.t(),
          status: atom(),
          workspace_id: String.t() | nil,
          membership_status: atom(),
          roles: [atom()],
          credential_hash: String.t() | nil,
          credential_version: pos_integer() | nil,
          access_version: pos_integer(),
          version: pos_integer()
        }
end

defmodule FavnOrchestrator.Persistence.Results.Session do
  @moduledoc "Opaque session metadata; raw tokens and token hashes are never returned."
  @enforce_keys [:session_id, :actor_id, :provider, :issued_at, :status, :expires_at]
  defstruct [
    :session_id,
    :actor_id,
    :provider,
    :issued_at,
    :status,
    :expires_at,
    :revoked_at,
    :last_seen_at
  ]

  @type t :: %__MODULE__{
          session_id: String.t(),
          actor_id: String.t(),
          provider: String.t(),
          issued_at: DateTime.t(),
          status: :active | :revoked | :expired,
          expires_at: DateTime.t(),
          revoked_at: DateTime.t() | nil,
          last_seen_at: DateTime.t() | nil
        }
end

defmodule FavnOrchestrator.Persistence.Results.AuditEntry do
  @moduledoc "Redacted authorization audit entry."
  @enforce_keys [:audit_id, :principal_id, :action, :subject_kind, :subject_id, :occurred_at]
  defstruct [
    :audit_id,
    :workspace_id,
    :principal_id,
    :action,
    :subject_kind,
    :subject_id,
    :detail,
    :occurred_at
  ]

  @type t :: %__MODULE__{
          audit_id: pos_integer(),
          workspace_id: String.t() | nil,
          principal_id: String.t(),
          action: String.t(),
          subject_kind: String.t(),
          subject_id: String.t(),
          detail: map(),
          occurred_at: DateTime.t()
        }
end
