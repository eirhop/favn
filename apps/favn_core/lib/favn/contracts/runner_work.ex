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

  Persisted SQL writes set `target_operation` explicitly. `active_relation` is
  the stable readable relation, while `write_relation` is either that relation
  for ordinary work or an isolated candidate override for rebuild work.
  `upstream_generation_pins` are independent of the output operation, so a
  non-persisted output may still read exact persisted SQL generations.
  """

  alias Favn.Contracts.RunnerReleaseBinding
  alias Favn.Contracts.TargetGenerationPin
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Plan.NodeIdentity
  alias Favn.Ref
  alias Favn.RelationRef
  alias Favn.Run.PipelineContext

  @type target_operation :: :normal_materialization | :rebuild_candidate

  @type t :: %__MODULE__{
          execution_id: String.t() | nil,
          run_id: String.t() | nil,
          run_started_at: DateTime.t() | nil,
          manifest_version_id: String.t(),
          manifest_content_hash: String.t(),
          required_runner_release_id: String.t(),
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
          target_operation: target_operation() | nil,
          logical_target_id: String.t() | nil,
          target_descriptor_hash: String.t() | nil,
          target_generation_id: String.t() | nil,
          active_relation: RelationRef.t() | nil,
          write_relation: RelationRef.t() | nil,
          upstream_generation_pins: [TargetGenerationPin.t()],
          rebuild_operation_id: String.t() | nil,
          rebuild_action_id: String.t() | nil,
          rebuild_item_id: String.t() | nil,
          rebuild_empty_generation: boolean(),
          rebuild_final_item: boolean(),
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
            required_runner_release_id: nil,
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
            target_operation: nil,
            logical_target_id: nil,
            target_descriptor_hash: nil,
            target_generation_id: nil,
            active_relation: nil,
            write_relation: nil,
            upstream_generation_pins: [],
            rebuild_operation_id: nil,
            rebuild_action_id: nil,
            rebuild_item_id: nil,
            rebuild_empty_generation: false,
            rebuild_final_item: false,
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
    |> Map.put(:required_runner_release_id, work.required_runner_release_id)
    |> Map.put(:target_operation, work.target_operation)
    |> Map.put(:logical_target_id, work.logical_target_id)
    |> Map.put(:target_generation_id, work.target_generation_id)
    |> Map.put(:rebuild_operation_id, work.rebuild_operation_id)
    |> Map.put(:rebuild_action_id, work.rebuild_action_id)
    |> Map.put(:rebuild_item_id, work.rebuild_item_id)
    |> Map.put(:rebuild_empty_generation, work.rebuild_empty_generation)
    |> Map.put(:rebuild_final_item, work.rebuild_final_item)
  end

  @doc "Validates the exact runner release identity pinned into this work request."
  @spec validate_release_binding(t()) :: :ok | {:error, RunnerReleaseBinding.error()}
  def validate_release_binding(%__MODULE__{required_runner_release_id: release_id}),
    do: RunnerReleaseBinding.validate(release_id)

  @doc """
  Validates the self-contained generation and relation identity on runner work.

  A nil operation is valid only when every persisted-target field is absent.
  Dispatch code must set an explicit operation for persisted SQL writes.
  """
  @spec validate_generation_contract(t()) :: :ok | {:error, term()}
  def validate_generation_contract(%__MODULE__{target_operation: nil} = work) do
    with :ok <- validate_generation_pins(work.upstream_generation_pins) do
      if non_target_output_work?(work),
        do: :ok,
        else: {:error, :target_operation_required}
    end
  end

  def validate_generation_contract(%__MODULE__{target_operation: operation} = work)
      when operation in [:normal_materialization, :rebuild_candidate] do
    with :ok <- validate_manifest_identity(work),
         :ok <- identifier(:logical_target_id, work.logical_target_id),
         :ok <- hash(:target_descriptor_hash, work.target_descriptor_hash),
         :ok <- generation_id(work.target_generation_id),
         :ok <- relation(:active_relation, work.active_relation),
         :ok <- relation(:write_relation, work.write_relation),
         :ok <- validate_generation_pins(work.upstream_generation_pins),
         :ok <- validate_operation_relations(work),
         :ok <- validate_rebuild_identity(work),
         :ok <- validate_rebuild_flags(work) do
      :ok
    end
  end

  def validate_generation_contract(%__MODULE__{target_operation: operation}),
    do: {:error, {:invalid_target_operation, operation}}

  @doc """
  Matches persisted-target runner work to its pinned manifest descriptor.

  This check prevents a valid-looking generation request from executing with a
  package, logical target, or stable relation taken from a different manifest
  asset.
  """
  @spec validate_target_identity(t(), TargetDescriptor.t()) :: :ok | {:error, term()}
  def validate_target_identity(%__MODULE__{} = work, %TargetDescriptor{} = descriptor) do
    with :ok <- validate_generation_contract(work),
         {:ok, descriptor} <- TargetDescriptor.validate(descriptor),
         :ok <- match_identity(:logical_target_id, work.logical_target_id, descriptor.target_id),
         :ok <-
           match_identity(
             :target_descriptor_hash,
             work.target_descriptor_hash,
             descriptor.descriptor_hash
           ),
         :ok <- match_relation(work.active_relation, descriptor.relation),
         :ok <- validate_execution_package(work, descriptor) do
      :ok
    end
  end

  def validate_target_identity(%__MODULE__{}, descriptor),
    do: {:error, {:invalid_target_descriptor, descriptor}}

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

  defp non_target_output_work?(work) do
    is_nil(work.logical_target_id) and is_nil(work.target_descriptor_hash) and
      is_nil(work.target_generation_id) and is_nil(work.active_relation) and
      is_nil(work.write_relation) and is_nil(work.rebuild_operation_id) and
      is_nil(work.rebuild_action_id) and is_nil(work.rebuild_item_id)
  end

  defp validate_manifest_identity(work) do
    with :ok <- identifier(:manifest_version_id, work.manifest_version_id),
         :ok <- hash(:manifest_content_hash, work.manifest_content_hash) do
      validate_release_binding(work)
    end
  end

  defp validate_generation_pins(pins) when is_list(pins) do
    with :ok <- Enum.reduce_while(pins, :ok, &validate_generation_pin/2) do
      target_ids = Enum.map(pins, & &1.target_id)

      if length(target_ids) == MapSet.size(MapSet.new(target_ids)),
        do: :ok,
        else: {:error, :duplicate_upstream_target_generation_pin}
    end
  end

  defp validate_generation_pins(value), do: {:error, {:invalid_upstream_generation_pins, value}}

  defp validate_generation_pin(pin, :ok) do
    case TargetGenerationPin.validate(pin) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp validate_operation_relations(%__MODULE__{
         target_operation: :normal_materialization,
         active_relation: relation,
         write_relation: relation
       }),
       do: :ok

  defp validate_operation_relations(%__MODULE__{target_operation: :normal_materialization}),
    do: {:error, :normal_materialization_relation_mismatch}

  defp validate_operation_relations(%__MODULE__{
         target_operation: :rebuild_candidate,
         active_relation: active,
         write_relation: candidate
       }) do
    if same_relation_namespace?(active, candidate) and active.name != candidate.name,
      do: :ok,
      else: {:error, :invalid_candidate_relation_override}
  end

  defp validate_rebuild_identity(%__MODULE__{target_operation: :normal_materialization} = work) do
    identities = [work.rebuild_operation_id, work.rebuild_action_id, work.rebuild_item_id]

    cond do
      Enum.all?(identities, &is_nil/1) ->
        :ok

      true ->
        with :ok <- identifier(:rebuild_operation_id, work.rebuild_operation_id),
             :ok <- identifier(:rebuild_action_id, work.rebuild_action_id) do
          identifier(:rebuild_item_id, work.rebuild_item_id)
        end
    end
  end

  defp validate_rebuild_identity(%__MODULE__{target_operation: :rebuild_candidate} = work) do
    with :ok <- identifier(:rebuild_operation_id, work.rebuild_operation_id),
         :ok <- identifier(:rebuild_action_id, work.rebuild_action_id) do
      identifier(:rebuild_item_id, work.rebuild_item_id)
    end
  end

  defp validate_rebuild_flags(%__MODULE__{target_operation: :rebuild_candidate} = work)
       when is_boolean(work.rebuild_empty_generation) and is_boolean(work.rebuild_final_item),
       do: :ok

  defp validate_rebuild_flags(%__MODULE__{} = work) do
    if work.rebuild_empty_generation == false and work.rebuild_final_item == false,
      do: :ok,
      else: {:error, :rebuild_flags_require_candidate_operation}
  end

  defp validate_execution_package(
         %__MODULE__{execution_package: %ExecutionPackage{} = package} = work,
         descriptor
       ) do
    with {:ok, package} <- ExecutionPackage.verify(package),
         :ok <- match_identity(:execution_package_asset_ref, package.asset_ref, asset_ref(work)) do
      match_identity(
        :execution_package_hash,
        package.content_hash,
        descriptor.execution_package_hash
      )
    end
  end

  defp validate_execution_package(%__MODULE__{execution_package: package}, _descriptor),
    do: {:error, {:persisted_target_execution_package_required, package}}

  defp match_relation(%RelationRef{} = actual, expected) when is_map(expected) do
    if relation_identity(actual) == relation_identity(expected),
      do: :ok,
      else: {:error, {:target_identity_mismatch, :active_relation, actual, expected}}
  end

  defp same_relation_namespace?(%RelationRef{} = left, %RelationRef{} = right) do
    {left.connection, left.catalog, left.schema} ==
      {right.connection, right.catalog, right.schema}
  end

  defp relation_identity(%RelationRef{} = relation) do
    %{
      connection: identifier_value(relation.connection),
      catalog: relation.catalog,
      schema: relation.schema,
      name: relation.name
    }
  end

  defp relation_identity(relation) when is_map(relation) do
    %{
      connection:
        identifier_value(Map.get(relation, :connection, Map.get(relation, "connection"))),
      catalog: Map.get(relation, :catalog, Map.get(relation, "catalog")),
      schema: Map.get(relation, :schema, Map.get(relation, "schema")),
      name: Map.get(relation, :name, Map.get(relation, "name"))
    }
  end

  defp match_identity(_field, value, value), do: :ok

  defp match_identity(field, actual, expected),
    do: {:error, {:target_identity_mismatch, field, actual, expected}}

  defp identifier(_field, value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp identifier(field, value), do: {:error, {:invalid_runner_work_field, field, value}}

  defp identifier_value(value) when is_atom(value), do: Atom.to_string(value)
  defp identifier_value(value), do: value

  defp generation_id(value) when is_binary(value) do
    if Regex.match?(
         ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/,
         value
       ),
       do: :ok,
       else: {:error, {:invalid_target_generation_id, value}}
  end

  defp generation_id(value), do: {:error, {:invalid_target_generation_id, value}}

  defp hash(_field, value) when is_binary(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, {:invalid_runner_work_hash, value}}
  end

  defp hash(field, value), do: {:error, {:invalid_runner_work_field, field, value}}

  defp relation(_field, %RelationRef{} = relation) do
    RelationRef.validate!(relation)
    :ok
  rescue
    ArgumentError -> {:error, {:invalid_runner_work_relation, relation}}
  end

  defp relation(field, value), do: {:error, {:invalid_runner_work_field, field, value}}
end
