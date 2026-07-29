defmodule FavnOrchestrator.RunServer.Execution.PipelineTaskContinuation do
  @moduledoc """
  Task-local orchestrator continuation persisted beside one pipeline runner task.

  Shared manifest and freshness state belong to the run checkpoint. This value
  contains only facts needed to settle this task and a reference to that shared
  checkpoint.
  """

  @keys [
    :decision,
    :freshness_checkpoint,
    :freshness_key,
    :kind,
    :materialization_claim,
    :resource_circuit_permits
  ]
  @checkpoint_keys [:attempt, :payload_hash, :revision, :sequence, :stage, :version]
  @legacy_keys [
    :decision,
    :freshness_context,
    :freshness_key,
    :kind,
    :materialization_claim,
    :resource_circuit_permits
  ]

  @type t :: %{
          required(:kind) => :pipeline,
          required(:decision) => map(),
          required(:materialization_claim) => map() | nil,
          required(:resource_circuit_permits) => [term()],
          required(:freshness_checkpoint) => map(),
          required(:freshness_key) => term()
        }

  @doc "Builds an exact task-local continuation."
  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    continuation = Map.put(attrs, :kind, :pipeline)

    if valid?(continuation) do
      Map.take(continuation, @keys)
    else
      raise ArgumentError, "invalid pipeline task continuation"
    end
  end

  @doc "Validates the exact continuation shape."
  @spec valid?(term()) :: boolean()
  def valid?(continuation) when is_map(continuation) do
    Map.keys(continuation) |> Enum.sort() == Enum.sort(@keys) and
      Map.get(continuation, :kind) == :pipeline and
      is_map(Map.get(continuation, :decision)) and
      valid_claim?(Map.get(continuation, :materialization_claim)) and
      is_list(Map.get(continuation, :resource_circuit_permits)) and
      valid_checkpoint?(Map.get(continuation, :freshness_checkpoint))
  end

  def valid?(_continuation), do: false

  @doc "Returns the shared checkpoint reference from a valid continuation."
  @spec checkpoint(t()) :: map()
  def checkpoint(continuation) when is_map(continuation),
    do: Map.get(continuation, :freshness_checkpoint)

  @doc false
  @spec legacy_freshness_context(term()) :: {:ok, map()} | :error
  def legacy_freshness_context(continuation) when is_map(continuation) do
    if Map.keys(continuation) |> Enum.sort() == Enum.sort(@legacy_keys) and
         Map.get(continuation, :kind) == :pipeline and
         is_map(Map.get(continuation, :decision)) and
         valid_claim?(Map.get(continuation, :materialization_claim)) and
         is_list(Map.get(continuation, :resource_circuit_permits)) and
         is_map(Map.get(continuation, :freshness_context)) do
      {:ok, Map.fetch!(continuation, :freshness_context)}
    else
      :error
    end
  end

  def legacy_freshness_context(_continuation), do: :error

  defp valid_claim?(nil), do: true
  defp valid_claim?(claim), do: is_map(claim)

  defp valid_checkpoint?(checkpoint) when is_map(checkpoint) do
    with true <- Map.keys(checkpoint) |> Enum.sort() == @checkpoint_keys,
         %{
           version: version,
           revision: revision,
           sequence: sequence,
           stage: stage,
           attempt: attempt,
           payload_hash: payload_hash
         } <- checkpoint do
      is_integer(version) and version > 0 and is_integer(revision) and revision > 0 and
        is_integer(sequence) and sequence > 0 and is_integer(stage) and stage >= 0 and
        is_integer(attempt) and attempt > 0 and is_binary(payload_hash) and
        byte_size(payload_hash) == 32
    else
      _invalid -> false
    end
  end

  defp valid_checkpoint?(_checkpoint), do: false
end
