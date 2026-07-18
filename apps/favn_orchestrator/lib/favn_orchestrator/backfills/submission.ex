defmodule FavnOrchestrator.Backfills.Submission do
  @moduledoc false

  alias Favn.Manifest.Version
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @enforce_keys [
    :context,
    :deployment_id,
    :version,
    :target_kind,
    :target,
    :target_id,
    :root_run_id,
    :backfill_id,
    :range,
    :batches,
    :batch_hashes,
    :opts
  ]
  defstruct [
    :context,
    :deployment_id,
    :version,
    :target_kind,
    :target,
    :target_id,
    :root_run_id,
    :backfill_id,
    :range,
    :batches,
    :batch_hashes,
    :resolution,
    :opts
  ]

  @type t :: %__MODULE__{
          context: WorkspaceContext.t(),
          deployment_id: String.t(),
          version: Version.t(),
          target_kind: :asset | :pipeline,
          target: struct(),
          target_id: String.t(),
          root_run_id: String.t(),
          backfill_id: String.t(),
          range: map(),
          batches: [[struct()]],
          batch_hashes: [String.t()],
          resolution: struct() | nil,
          opts: keyword()
        }
end
