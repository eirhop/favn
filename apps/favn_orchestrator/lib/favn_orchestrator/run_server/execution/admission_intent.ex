defmodule FavnOrchestrator.RunServer.Execution.AdmissionIntent do
  @moduledoc """
  Immutable inputs for the one step currently waiting for admission.

  Work is rebuilt from the pinned run and manifest. This value preserves the
  original deadline and freshness decision; it does not persist executable work,
  acquired handles, process state, or application result data. It is saved with
  the run transition before acquisition and cleared with admission's decision.
  """

  alias Favn.Contracts.RunnerWork
  alias Favn.Manifest.Version
  alias FavnOrchestrator.AssetRunnerTasks
  alias FavnOrchestrator.RunnerTaskContext
  alias FavnOrchestrator.RunState

  @metadata_key "execution_admission_intent"
  @keys ~w(version task_id asset_step_id stage attempt deadline_at occurred_at context)
  @enforce_keys [:task_id, :asset_step_id, :stage, :attempt, :deadline_at, :occurred_at, :context]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          task_id: String.t(),
          asset_step_id: String.t(),
          stage: non_neg_integer(),
          attempt: pos_integer(),
          deadline_at: DateTime.t(),
          occurred_at: DateTime.t(),
          context: map()
        }

  @doc false
  @spec metadata_key() :: String.t()
  def metadata_key, do: @metadata_key

  @doc false
  @spec validate_metadata(term()) :: :ok | {:error, :invalid_admission_intent}
  def validate_metadata(metadata) when is_map(metadata) do
    case Map.fetch(metadata, @metadata_key) do
      :error -> :ok
      {:ok, encoded} -> validate_encoded(encoded)
    end
  end

  def validate_metadata(_metadata), do: :ok

  @doc false
  @spec validate_submission_metadata(map()) :: :ok | {:error, :reserved_run_metadata}
  def validate_submission_metadata(metadata) when is_map(metadata) do
    if Enum.any?(Map.keys(metadata), &(&1 in [@metadata_key, :execution_admission_intent])),
      do: {:error, :reserved_run_metadata},
      else: :ok
  end

  @doc "Creates intent from work before any capacity, circuit or claim mutation."
  @spec new(RunState.t(), RunnerWork.t(), map(), DateTime.t()) ::
          {:ok, t()} | {:error, term()}
  def new(%RunState{} = run, %RunnerWork{} = work, context, %DateTime{} = occurred_at) do
    intent = %__MODULE__{
      task_id: AssetRunnerTasks.task_id(run, work, RunnerWork.node_key(work), work.attempt),
      asset_step_id: work.asset_step_id,
      stage: work.stage,
      attempt: work.attempt,
      deadline_at: work.deadline_at,
      occurred_at: occurred_at,
      context: execution_context(context)
    }

    with true <- work.run_id == run.id,
         true <- work.manifest_version_id == run.manifest_version_id,
         true <- work.manifest_content_hash == run.manifest_content_hash,
         true <- context[:kind] == RunState.execution_mode(run),
         true <- context_matches_work?(context, work),
         {:ok, _encoded} <- encode(intent) do
      {:ok, intent}
    else
      false -> {:error, :invalid_admission_intent}
      error -> error
    end
  end

  @doc "Encodes intent for the existing bounded run snapshot."
  @spec encode(t()) :: {:ok, map()} | {:error, term()}
  def encode(%__MODULE__{} = intent) do
    with true <- valid_identity?(intent),
         true <- empty_handles?(intent.context),
         {:ok, context} <- RunnerTaskContext.encode(intent.context) do
      encoded = %{
        "version" => 1,
        "task_id" => intent.task_id,
        "asset_step_id" => intent.asset_step_id,
        "stage" => intent.stage,
        "attempt" => intent.attempt,
        "deadline_at" => DateTime.to_iso8601(intent.deadline_at),
        "occurred_at" => DateTime.to_iso8601(intent.occurred_at),
        "context" => context
      }

      {:ok, encoded}
    else
      _ -> {:error, :invalid_admission_intent}
    end
  rescue
    _ -> {:error, :invalid_admission_intent}
  end

  @doc "Loads the original intent, verifying it belongs to the same pinned attempt."
  @spec load(RunState.t(), RunnerWork.t(), Version.t()) ::
          {:ok, t() | nil} | {:error, term()}
  def load(%RunState{} = run, %RunnerWork{} = work, %Version{} = version) do
    case Map.fetch(run.metadata, @metadata_key) do
      :error -> {:ok, nil}
      {:ok, encoded} -> decode(encoded, run, work, version)
    end
  end

  @doc "Returns metadata containing this intent; the caller commits its matching event."
  @spec put(map(), t()) :: {:ok, map()} | {:error, term()}
  def put(metadata, %__MODULE__{} = intent) when is_map(metadata) do
    with {:ok, encoded} <- encode(intent) do
      case Map.fetch(metadata, @metadata_key) do
        :error -> {:ok, Map.put(metadata, @metadata_key, encoded)}
        {:ok, ^encoded} -> {:ok, metadata}
        _ -> {:error, :admission_intent_already_pending}
      end
    end
  end

  @doc "Clears only this intent alongside its committed admission decision."
  @spec clear(map(), t()) :: {:ok, map()} | {:error, term()}
  def clear(metadata, %__MODULE__{} = intent) when is_map(metadata) do
    with {:ok, encoded} <- encode(intent),
         ^encoded <- Map.get(metadata, @metadata_key) do
      {:ok, Map.delete(metadata, @metadata_key)}
    else
      _ -> {:error, :admission_intent_mismatch}
    end
  end

  defp decode(encoded, run, work, version) when is_map(encoded) do
    with :ok <- validate_encoded(encoded),
         true <- work.run_id == run.id,
         true <- version.manifest_version_id == run.manifest_version_id,
         true <- version.content_hash == run.manifest_content_hash,
         true <- work.manifest_version_id == run.manifest_version_id,
         true <- work.manifest_content_hash == run.manifest_content_hash,
         true <- encoded["asset_step_id"] == work.asset_step_id,
         true <- encoded["stage"] == work.stage and encoded["attempt"] == work.attempt,
         true <-
           encoded["task_id"] ==
             AssetRunnerTasks.task_id(run, work, RunnerWork.node_key(work), work.attempt),
         {:ok, deadline, 0} <- DateTime.from_iso8601(encoded["deadline_at"]),
         {:ok, occurred_at, 0} <- DateTime.from_iso8601(encoded["occurred_at"]),
         {:ok, context} <- RunnerTaskContext.decode(encoded["context"], version),
         {:ok, intent} <- new(run, %{work | deadline_at: deadline}, context, occurred_at) do
      {:ok, intent}
    else
      _ -> {:error, :invalid_admission_intent}
    end
  rescue
    _ -> {:error, :invalid_admission_intent}
  end

  defp decode(_encoded, _run, _work, _version), do: {:error, :invalid_admission_intent}

  defp validate_encoded(encoded) when is_map(encoded) do
    with true <- Enum.sort(Map.keys(encoded)) == Enum.sort(@keys),
         true <- encoded["version"] == 1 and is_map(encoded["context"]),
         {:ok, deadline, 0} <- DateTime.from_iso8601(encoded["deadline_at"]),
         {:ok, occurred_at, 0} <- DateTime.from_iso8601(encoded["occurred_at"]),
         true <-
           valid_identity?(%{
             task_id: encoded["task_id"],
             asset_step_id: encoded["asset_step_id"],
             stage: encoded["stage"],
             attempt: encoded["attempt"],
             deadline_at: deadline,
             occurred_at: occurred_at
           }) do
      :ok
    else
      _ -> {:error, :invalid_admission_intent}
    end
  rescue
    _ -> {:error, :invalid_admission_intent}
  end

  defp validate_encoded(_), do: {:error, :invalid_admission_intent}

  defp valid_identity?(intent) do
    Enum.all?(
      [intent.task_id, intent.asset_step_id],
      &(is_binary(&1) and byte_size(&1) in 1..255)
    ) and
      is_integer(intent.stage) and intent.stage >= 0 and
      is_integer(intent.attempt) and intent.attempt > 0 and
      utc?(intent.deadline_at) and utc?(intent.occurred_at)
  end

  defp utc?(%DateTime{utc_offset: 0, std_offset: 0}), do: true
  defp utc?(_), do: false

  # Diagnostic stale reasons do not affect execution. Keep the same canonical
  # decision for live admission and recovery, without copying its diagnostic tree.
  defp execution_context(%{kind: :pipeline, decision: decision} = context)
       when is_map(decision) do
    if is_list(Map.get(decision, :stale_reasons, [])),
      do: %{context | decision: Map.delete(decision, :stale_reasons)},
      else: context
  end

  defp execution_context(context), do: context

  defp context_matches_work?(%{kind: :sequential}, _work), do: true

  defp context_matches_work?(
         %{kind: :pipeline, decision: decision, freshness_checkpoint: checkpoint} = context,
         work
       )
       when is_map(decision) and is_map(checkpoint),
       do:
         decision[:node_key] == RunnerWork.node_key(work) and
           context[:freshness_key] == decision[:freshness_key] and
           checkpoint[:stage] == work.stage and checkpoint[:attempt] == work.attempt

  defp context_matches_work?(_context, _work), do: false

  defp empty_handles?(%{kind: :sequential, materialization_claim: nil}), do: true

  defp empty_handles?(%{
         kind: :pipeline,
         materialization_claim: nil,
         resource_circuit_permits: []
       }),
       do: true

  defp empty_handles?(_), do: false
end
