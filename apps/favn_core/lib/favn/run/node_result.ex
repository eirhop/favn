defmodule Favn.Run.NodeResult do
  @moduledoc """
  Per-node execution outcome captured during a run.

  Unlike `Favn.Run.AssetResult`, a node result preserves the planned node
  identity in `node_key`. This allows execution reporting to distinguish
  multiple planned executions of the same asset reference, such as different
  windows or freshness decisions.
  """

  alias Favn.Plan
  alias Favn.Ref
  alias Favn.Window.Runtime

  @statuses [
    :running,
    :retrying,
    :ok,
    :error,
    :cancelled,
    :timed_out,
    :skipped_fresh,
    :blocked
  ]

  @typedoc """
  Execution status for one planned node.
  """
  @type status ::
          :running
          | :retrying
          | :ok
          | :error
          | :cancelled
          | :timed_out
          | :skipped_fresh
          | :blocked

  @typedoc """
  Input version metadata captured for a node execution.
  """
  @type input_versions :: list() | map()

  @typedoc """
  One recorded execution attempt for a planned node.
  """
  @type attempt_result :: map()

  @typedoc """
  Result for one planned execution node.
  """
  @type t :: %__MODULE__{
          node_key: Plan.node_key(),
          ref: Ref.t(),
          window: Runtime.t() | nil,
          stage: non_neg_integer(),
          status: status(),
          started_at: DateTime.t() | nil,
          finished_at: DateTime.t() | nil,
          duration_ms: non_neg_integer() | nil,
          reason: term() | nil,
          freshness_key: String.t() | nil,
          input_versions: input_versions(),
          attempt_count: non_neg_integer(),
          max_attempts: pos_integer(),
          runner_execution_id: term() | nil,
          meta: map(),
          error: term() | nil,
          attempts: [attempt_result()]
        }

  defstruct [
    :node_key,
    :ref,
    :window,
    :started_at,
    :finished_at,
    :duration_ms,
    :reason,
    :freshness_key,
    :runner_execution_id,
    :error,
    stage: 0,
    status: :running,
    input_versions: %{},
    attempt_count: 0,
    max_attempts: 1,
    meta: %{},
    attempts: []
  ]

  @doc """
  Builds a node result and rejects unknown statuses.

  Accepts either a map or keyword list of struct fields.
  """
  @spec new(map() | keyword()) :: t()
  def new(fields) when is_map(fields) or is_list(fields) do
    fields = Map.new(fields)
    status = Map.get(fields, :status, :running)

    if status in @statuses do
      struct!(__MODULE__, fields)
    else
      raise ArgumentError,
            "invalid node result status #{inspect(status)}; expected one of #{inspect(@statuses)}"
    end
  end
end
