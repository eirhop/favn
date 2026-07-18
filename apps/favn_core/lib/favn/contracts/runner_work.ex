defmodule Favn.Contracts.RunnerWork do
  @moduledoc """
  Runner work request contract pinned to an immutable manifest version.

  `execution_id` may be allocated by the orchestrator before submission. This
  lets a durable dispatch intent name the external work before the runner call
  is attempted. Runners idempotently return an existing queued, running, or retained
  completed execution when both the supplied identity and normalized work are exact;
  reusing an identity for different work is rejected. A runner must never silently
  replace the supplied identity.

  `node_identity` carries the manifest/planning identity for the current planned
  node. Attempt, retry, admission, and cancellation fields are explicit runner
  lifecycle fields and are not encoded in `metadata`.
  """

  alias Favn.Plan.NodeIdentity
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Ref
  alias Favn.Run.PipelineContext

  @type t :: %__MODULE__{
          execution_id: String.t() | nil,
          run_id: String.t() | nil,
          run_started_at: DateTime.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          manifest_lease_id: String.t() | nil,
          node_identity: NodeIdentity.t() | nil,
          asset_ref: Ref.t() | nil,
          asset_refs: [Ref.t()],
          planned_asset_refs: [Ref.t()],
          attempt: pos_integer(),
          max_attempts: pos_integer(),
          asset_step_id: String.t() | nil,
          stage: non_neg_integer(),
          params: map(),
          runtime_input_pin: Favn.RuntimeInput.Pin.t() | nil,
          execution_package: ExecutionPackage.t() | nil,
          pipeline: PipelineContext.t() | nil,
          deadline_at: DateTime.t() | nil,
          trigger: map(),
          metadata: map()
        }

  defstruct execution_id: nil,
            run_id: nil,
            run_started_at: nil,
            manifest_version_id: nil,
            manifest_content_hash: nil,
            manifest_lease_id: nil,
            node_identity: nil,
            asset_ref: nil,
            asset_refs: [],
            planned_asset_refs: [],
            attempt: 1,
            max_attempts: 1,
            asset_step_id: nil,
            stage: 0,
            params: %{},
            runtime_input_pin: nil,
            execution_package: nil,
            pipeline: nil,
            deadline_at: nil,
            trigger: %{},
            metadata: %{}

  @doc """
  Returns the current planned asset reference for this work request.
  """
  @spec asset_ref(t()) :: Ref.t() | nil
  def asset_ref(%__MODULE__{asset_ref: ref}) when is_tuple(ref), do: ref

  def asset_ref(%__MODULE__{node_identity: %NodeIdentity{node_key: {ref, _window_key}}}),
    do: ref

  def asset_ref(%__MODULE__{}), do: nil

  @doc """
  Returns the complete planned asset scope visible to the runner.
  """
  @spec planned_asset_refs(t()) :: [Ref.t()]
  def planned_asset_refs(%__MODULE__{node_identity: %NodeIdentity{planned_asset_refs: refs}})
      when is_list(refs) and refs != [],
      do: refs

  def planned_asset_refs(%__MODULE__{planned_asset_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  def planned_asset_refs(%__MODULE__{asset_refs: refs}) when is_list(refs) and refs != [],
    do: refs

  def planned_asset_refs(%__MODULE__{} = work) do
    case asset_ref(work) do
      nil -> []
      ref -> [ref]
    end
  end

  @doc """
  Returns the planned node key, if present.
  """
  @spec node_key(t()) :: Favn.Plan.node_key() | nil
  def node_key(%__MODULE__{node_identity: %NodeIdentity{node_key: node_key}}), do: node_key
  def node_key(%__MODULE__{metadata: %{node_key: node_key}}), do: node_key
  def node_key(%__MODULE__{}), do: nil

  @doc """
  Returns the planned runtime window, if present.
  """
  @spec window(t()) :: Favn.Window.Runtime.t() | nil
  def window(%__MODULE__{node_identity: %NodeIdentity{window: window}}), do: window
  def window(%__MODULE__{metadata: %{window: window}}), do: window
  def window(%__MODULE__{}), do: nil

  @doc """
  Returns the effective execution pool for the planned node.
  """
  @spec execution_pool(t()) :: atom() | nil
  def execution_pool(%__MODULE__{node_identity: %NodeIdentity{execution_pool: pool}}), do: pool
  def execution_pool(%__MODULE__{metadata: %{execution_pool: pool}}), do: pool
  def execution_pool(%__MODULE__{}), do: nil

  @doc """
  Derives orchestrator lifecycle metadata from explicit work fields.
  """
  @spec lifecycle_metadata(t()) :: map()
  def lifecycle_metadata(%__MODULE__{} = work) do
    work.metadata
    |> Map.put(:attempt, work.attempt)
    |> Map.put(:asset_step_id, work.asset_step_id)
    |> Map.put(:max_attempts, work.max_attempts)
    |> Map.put(:stage, work.stage)
    |> Map.put(:node_key, node_key(work))
    |> Map.put(:window, window(work))
    |> Map.put(:execution_pool, execution_pool(work))
    |> Map.put(:deadline_at, work.deadline_at)
  end

  @doc "Returns a fixed-size fingerprint for exact deterministic execution-ID replay."
  @spec replay_fingerprint(t()) :: <<_::256>>
  def replay_fingerprint(%__MODULE__{} = work) do
    work
    |> Map.from_struct()
    |> Map.update!(:execution_package, &execution_package_identity/1)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp execution_package_identity(nil), do: nil

  defp execution_package_identity(%{content_hash: content_hash, asset_ref: asset_ref})
       when is_binary(content_hash) do
    %{content_hash: content_hash, asset_ref: asset_ref}
  end

  defp execution_package_identity(package), do: package
end
