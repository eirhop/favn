defmodule FavnOrchestrator.RunManager.Submission do
  @moduledoc false

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunState

  @type submit_kind :: :manual | :pipeline | :rerun

  @type t :: %__MODULE__{
          run_state: RunState.t(),
          workspace_context: WorkspaceContext.t(),
          deployment_id: String.t(),
          pipeline_refs: [{module(), atom()}],
          manifest_version: Version.t(),
          submit_kind: submit_kind(),
          transition_metadata: map(),
          event_metadata: map(),
          idempotency: CommandIdempotency.t() | nil
        }

  defstruct [
    :run_state,
    :workspace_context,
    :deployment_id,
    :manifest_version,
    :submit_kind,
    :idempotency,
    pipeline_refs: [],
    transition_metadata: %{},
    event_metadata: %{}
  ]
end
