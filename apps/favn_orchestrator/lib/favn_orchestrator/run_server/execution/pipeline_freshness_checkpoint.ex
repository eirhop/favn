defmodule FavnOrchestrator.RunServer.Execution.PipelineFreshnessCheckpoint do
  @moduledoc """
  Persists and restores the single shared freshness checkpoint for a pipeline run.

  Immutable manifest asset definitions are deliberately excluded. Recovery
  rebuilds them from the run's pinned manifest index. State indexes are encoded
  as unique state lists so their node-key and asset-key aliases are not persisted
  more than once.
  """

  alias Favn.Manifest.Index
  alias FavnOrchestrator.AssetFreshnessState
  alias FavnOrchestrator.Freshness.StateLoader
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.PutRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Queries.GetRunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.Results.RunExecutionCheckpoint
  alias FavnOrchestrator.Persistence.SystemContext
  alias FavnOrchestrator.RefreshPolicy
  alias FavnOrchestrator.RunState

  @checkpoint_version 1
  @checkpoint_ref_keys [:attempt, :payload_hash, :revision, :sequence, :stage, :version]
  @max_payload_bytes 64 * 1_024 * 1_024
  @required_payload_keys [
    :completed_node_keys,
    :current_state_deltas,
    :format,
    :now,
    :prior_states,
    :refresh_policy,
    :refreshed_node_keys,
    :run_id,
    :upstream_statuses
  ]
  @format :favn_pipeline_freshness_checkpoint_v1

  @type checkpoint_ref :: %{
          required(:version) => pos_integer(),
          required(:revision) => pos_integer(),
          required(:sequence) => pos_integer(),
          required(:stage) => non_neg_integer(),
          required(:attempt) => pos_integer(),
          required(:payload_hash) => binary()
        }

  @doc "Returns the current durable checkpoint, or `nil` for a new run."
  @spec load(RunState.t(), Index.t()) ::
          {:ok, {map(), checkpoint_ref()} | nil} | {:error, term()}
  def load(%RunState{} = run, %Index{} = manifest_index) do
    query = %GetRunExecutionCheckpoint{
      workspace_context: SystemContext.workspace(run.workspace_id, :run_checkpoint_read),
      run_id: run.id
    }

    case Persistence.stores().runs.get_execution_checkpoint(query) do
      {:ok, %RunExecutionCheckpoint{} = checkpoint} ->
        with :ok <- verify_hash(checkpoint.payload, checkpoint.payload_hash),
             {:ok, context} <- restore_payload(checkpoint.payload, run, manifest_index) do
          {:ok, {context, reference(checkpoint)}}
        end

      {:error, %{kind: :not_found}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, {:run_execution_checkpoint_lookup_failed, reason}}
    end
  end

  @doc "Fenced replacement of the run's single shared checkpoint."
  @spec put(RunState.t(), non_neg_integer(), pos_integer(), map(), checkpoint_ref() | nil) ::
          {:ok, checkpoint_ref()} | {:error, term()}
  def put(%RunState{} = run, stage, attempt, context, previous_reference)
      when is_integer(stage) and stage >= 0 and is_integer(attempt) and attempt > 0 and
             is_map(context) and
             (is_nil(previous_reference) or is_map(previous_reference)) do
    with {:ok, revision} <- next_revision(previous_reference),
         {:ok, payload} <- encode_payload(run.id, context),
         payload_hash <- :crypto.hash(:sha256, payload) do
      command = %PutRunExecutionCheckpoint{
        workspace_context: SystemContext.workspace(run.workspace_id, :run_checkpoint_write),
        run_id: run.id,
        owner_id: run.storage_owner_id,
        fencing_token: run.storage_fencing_token,
        checkpoint_version: @checkpoint_version,
        checkpoint_revision: revision,
        checkpoint_sequence: run.event_seq,
        stage: stage,
        attempt: attempt,
        payload: payload,
        payload_hash: payload_hash,
        occurred_at: DateTime.utc_now()
      }

      expected = %{
        version: @checkpoint_version,
        revision: revision,
        sequence: run.event_seq,
        stage: stage,
        attempt: attempt,
        payload_hash: payload_hash
      }

      case Persistence.stores().runs.put_execution_checkpoint(command) do
        {:ok, checkpoint} -> {:ok, reference(checkpoint)}
        {:error, reason} -> resolve_unknown_put(run, expected, reason)
      end
    else
      {:error, reason} -> {:error, {:run_execution_checkpoint_write_failed, reason}}
    end
  end

  @doc "Returns true when a task continuation names the loaded checkpoint."
  @spec matches?(checkpoint_ref() | nil, term()) :: boolean()
  def matches?(reference, candidate) when is_map(reference) and is_map(candidate) do
    Map.keys(reference) |> Enum.sort() == @checkpoint_ref_keys and
      Map.keys(candidate) |> Enum.sort() == @checkpoint_ref_keys and
      candidate == reference
  end

  def matches?(_reference, _candidate), do: false

  @doc false
  @spec encode_payload(String.t(), map()) :: {:ok, binary()} | {:error, term()}
  def encode_payload(run_id, context) when is_binary(run_id) and is_map(context) do
    with {:ok, prior_states} <- unique_states(Map.get(context, :prior_states, %{})),
         {:ok, current_states} <- unique_states(Map.get(context, :current_states, %{})),
         current_state_deltas <- state_deltas(prior_states, current_states),
         {:ok, payload_term} <-
           payload_term(run_id, context, prior_states, current_state_deltas) do
      payload = :erlang.term_to_binary(payload_term, [:deterministic])

      if byte_size(payload) <= @max_payload_bytes do
        {:ok, payload}
      else
        {:error,
         {:pipeline_freshness_checkpoint_too_large, byte_size(payload), @max_payload_bytes}}
      end
    end
  end

  def encode_payload(_run_id, _context), do: {:error, :invalid_pipeline_freshness_checkpoint}

  @doc false
  @spec decode_payload(binary()) :: {:ok, map()} | {:error, term()}
  def decode_payload(payload)
      when is_binary(payload) and byte_size(payload) <= @max_payload_bytes do
    with {:ok, value} <- safe_binary_to_term(payload),
         :ok <- validate_payload(value) do
      {:ok, value}
    end
  end

  def decode_payload(_payload), do: {:error, :invalid_pipeline_freshness_checkpoint}

  @doc false
  @spec restore_payload(binary(), RunState.t(), Index.t()) :: {:ok, map()} | {:error, term()}
  def restore_payload(payload, %RunState{} = run, %Index{} = manifest_index) do
    with {:ok, decoded} <- decode_payload(payload) do
      restore_context(decoded, run, manifest_index)
    end
  end

  @doc false
  @spec max_payload_bytes() :: pos_integer()
  def max_payload_bytes, do: @max_payload_bytes

  defp payload_term(run_id, context, prior_states, current_state_deltas) do
    with %RefreshPolicy{} = refresh_policy <- Map.get(context, :refresh_policy),
         %DateTime{} = now <- Map.get(context, :now),
         %MapSet{} = completed <- Map.get(context, :completed_node_keys),
         %MapSet{} = refreshed <- Map.get(context, :refreshed_node_keys),
         upstream_statuses when is_map(upstream_statuses) <-
           Map.get(context, :upstream_statuses) do
      {:ok,
       %{
         format: @format,
         run_id: run_id,
         refresh_policy: refresh_policy,
         prior_states: prior_states,
         current_state_deltas: current_state_deltas,
         completed_node_keys: sorted_terms(completed),
         refreshed_node_keys: sorted_terms(refreshed),
         upstream_statuses: upstream_statuses,
         now: now
       }}
    else
      _invalid -> {:error, :invalid_pipeline_freshness_context}
    end
  end

  defp restore_context(payload, %RunState{id: run_id} = run, manifest_index) do
    with ^run_id <- Map.get(payload, :run_id),
         prior_states when is_list(prior_states) <- Map.get(payload, :prior_states),
         current_state_deltas when is_list(current_state_deltas) <-
           Map.get(payload, :current_state_deltas),
         %RefreshPolicy{} = refresh_policy <- Map.get(payload, :refresh_policy),
         %DateTime{} = now <- Map.get(payload, :now),
         completed when is_list(completed) <- Map.get(payload, :completed_node_keys),
         refreshed when is_list(refreshed) <- Map.get(payload, :refreshed_node_keys),
         upstream_statuses when is_map(upstream_statuses) <-
           Map.get(payload, :upstream_statuses) do
      prior_index = StateLoader.index(prior_states)

      {:ok,
       %{
         assets_by_ref: manifest_index.assets_by_ref,
         refresh_policy: refresh_policy,
         forced_node_keys: RefreshPolicy.expand_force_set(refresh_policy, run.plan),
         prior_states: prior_index,
         current_states: Map.merge(prior_index, StateLoader.index(current_state_deltas)),
         completed_node_keys: MapSet.new(completed),
         refreshed_node_keys: MapSet.new(refreshed),
         upstream_statuses: upstream_statuses,
         now: now
       }}
    else
      _invalid -> {:error, :invalid_pipeline_freshness_checkpoint}
    end
  end

  defp validate_payload(value) when is_map(value) do
    if Map.keys(value) |> Enum.sort() == Enum.sort(@required_payload_keys) and
         Map.get(value, :format) == @format do
      :ok
    else
      {:error, :invalid_pipeline_freshness_checkpoint}
    end
  end

  defp validate_payload(_value), do: {:error, :invalid_pipeline_freshness_checkpoint}

  defp unique_states(index) when is_map(index) do
    index
    |> Map.values()
    |> Enum.reduce_while({:ok, %{}}, fn
      %AssetFreshnessState{} = state, {:ok, acc} ->
        identity = state_identity(state)

        case Map.fetch(acc, identity) do
          :error -> {:cont, {:ok, Map.put(acc, identity, state)}}
          {:ok, ^state} -> {:cont, {:ok, acc}}
          {:ok, _different} -> {:halt, {:error, :conflicting_pipeline_freshness_state}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_pipeline_freshness_state}}
    end)
    |> then(fn
      {:ok, states} ->
        {:ok,
         states
         |> Enum.sort_by(fn {identity, _state} -> deterministic_binary(identity) end)
         |> Enum.map(&elem(&1, 1))}

      error ->
        error
    end)
  end

  defp unique_states(_index), do: {:error, :invalid_pipeline_freshness_state_index}

  defp state_deltas(prior_states, current_states) do
    prior = Map.new(prior_states, &{state_identity(&1), &1})

    Enum.reject(current_states, fn state ->
      Map.get(prior, state_identity(state)) == state
    end)
  end

  defp state_identity(%AssetFreshnessState{} = state) do
    {
      state.asset_ref_module,
      state.asset_ref_name,
      state.freshness_key,
      state.evidence_generation_id,
      state.latest_success_node_key
    }
  end

  defp reference(%RunExecutionCheckpoint{} = checkpoint) do
    %{
      version: checkpoint.checkpoint_version,
      revision: checkpoint.checkpoint_revision,
      sequence: checkpoint.checkpoint_sequence,
      stage: checkpoint.stage,
      attempt: checkpoint.attempt,
      payload_hash: checkpoint.payload_hash
    }
  end

  defp verify_hash(payload, expected_hash) when is_binary(payload) and is_binary(expected_hash) do
    if :crypto.hash(:sha256, payload) == expected_hash,
      do: :ok,
      else: {:error, :pipeline_freshness_checkpoint_hash_mismatch}
  end

  defp verify_hash(_payload, _expected_hash),
    do: {:error, :invalid_pipeline_freshness_checkpoint}

  defp next_revision(nil), do: {:ok, 1}

  defp next_revision(%{revision: revision}) when is_integer(revision) and revision > 0,
    do: {:ok, revision + 1}

  defp next_revision(_reference), do: {:error, :invalid_pipeline_freshness_checkpoint_reference}

  defp resolve_unknown_put(run, expected, write_error) do
    query = %GetRunExecutionCheckpoint{
      workspace_context: SystemContext.workspace(run.workspace_id, :run_checkpoint_read),
      run_id: run.id
    }

    case Persistence.stores().runs.get_execution_checkpoint(query) do
      {:ok, checkpoint} ->
        reference = reference(checkpoint)

        if reference == expected,
          do: {:ok, reference},
          else: {:error, {:run_execution_checkpoint_write_failed, write_error}}

      _missing_or_unavailable ->
        {:error, {:run_execution_checkpoint_write_failed, write_error}}
    end
  end

  defp safe_binary_to_term(payload) do
    {:ok, :erlang.binary_to_term(payload, [:safe])}
  rescue
    _error -> {:error, :invalid_pipeline_freshness_checkpoint}
  end

  defp sorted_terms(%MapSet{} = terms),
    do: Enum.sort_by(terms, &deterministic_binary/1)

  defp deterministic_binary(term),
    do: :erlang.term_to_binary(term, [:deterministic])
end
