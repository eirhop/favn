defmodule FavnRunner.GenerationOperations do
  @moduledoc false

  alias Favn.Contracts.GenerationActivationRequest
  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardRequest
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarker, as: ContractMarker
  alias Favn.Contracts.GenerationMarkerInitializationRequest
  alias Favn.Contracts.GenerationMarkerInitializationResult, as: ContractInitializationResult
  alias Favn.Contracts.GenerationReconciliationRequest
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RunnerError
  alias Favn.Manifest.Asset
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Client
  alias Favn.SQL.Error, as: SQLError
  alias Favn.SQL.GenerationActivation
  alias Favn.SQL.GenerationActivationResult, as: SQLActivationResult
  alias Favn.SQL.GenerationDiscard
  alias Favn.SQL.GenerationInspection
  alias Favn.SQL.GenerationMarker, as: SQLMarker
  alias Favn.SQL.GenerationMarkerInitialization
  alias Favn.SQL.GenerationMarkerInitializationResult, as: SQLInitializationResult
  alias Favn.SQL.GenerationReconciliation
  alias Favn.SQL.GenerationRelation

  @runner_registry FavnRunner.ConnectionRegistry

  @spec capabilities(Asset.t()) :: {:ok, map()} | {:error, term()}
  def capabilities(%Asset{} = asset) do
    with {:ok, relation} <- persisted_relation(asset),
         {:ok, session} <- connect(asset, relation) do
      try do
        with {:ok, capabilities} <- Client.generation_capabilities(session) do
          {:ok, Map.from_struct(capabilities)}
        end
      after
        Client.disconnect(session)
      end
    end
  end

  @spec initialize_marker(GenerationMarkerInitializationRequest.t(), Version.t()) ::
          {:ok, ContractInitializationResult.t()} | {:error, term()}
  def initialize_marker(%GenerationMarkerInitializationRequest{} = request, %Version{} = version) do
    with :ok <- GenerationMarkerInitializationRequest.validate(request),
         {:ok, asset, stable_relation} <- target_asset(version, request.target_id),
         :ok <- same_relation(request.active_relation, stable_relation),
         {:ok, session} <- connect(asset, stable_relation) do
      try do
        request
        |> sql_initialization()
        |> then(&Client.initialize_generation_marker(session, &1))
        |> initialization_result(request)
      after
        Client.disconnect(session)
      end
    end
  end

  @spec marker(Asset.t()) :: {:ok, ContractMarker.t() | nil} | {:error, term()}
  def marker(%Asset{target_descriptor: %TargetDescriptor{target_id: target_id}} = asset) do
    with {:ok, relation} <- persisted_relation(asset),
         {:ok, session} <- connect(asset, relation) do
      try do
        case Client.reconcile_generation(session, %GenerationReconciliation{
               logical_target_id: target_id,
               stable_relation: relation
             }) do
          {:ok, nil} -> {:ok, nil}
          {:ok, %SQLMarker{} = marker} -> {:ok, contract_marker(marker)}
          {:error, %SQLError{} = error} -> {:error, error}
        end
      after
        Client.disconnect(session)
      end
    end
  end

  def marker(%Asset{}), do: {:error, :generation_target_not_supported}

  @spec activate(GenerationActivationRequest.t(), Version.t()) ::
          {:ok, GenerationActivationResult.t()} | {:error, term()}
  def activate(%GenerationActivationRequest{} = request, %Version{} = version) do
    with :ok <- GenerationActivationRequest.validate(request),
         {:ok, asset, stable_relation} <- target_asset(version, request.target_id),
         :ok <- same_relation(request.active_relation, stable_relation),
         {:ok, session} <- connect(asset, stable_relation) do
      try do
        with {:ok, capabilities} <- Client.generation_capabilities(session),
             :ok <- validate_activation_relations(request, capabilities.max_identifier_bytes) do
          request
          |> sql_activation()
          |> then(&Client.activate_generation(session, &1))
          |> activation_result(request)
        end
      after
        Client.disconnect(session)
      end
    end
  end

  @spec reconcile(GenerationReconciliationRequest.t(), Version.t()) ::
          {:ok, GenerationReconciliationResult.t()} | {:error, term()}
  def reconcile(
        %GenerationReconciliationRequest{activation: activation} = request,
        %Version{} = version
      ) do
    with :ok <- GenerationReconciliationRequest.validate(request),
         {:ok, asset, stable_relation} <- target_asset(version, activation.target_id),
         :ok <- same_relation(activation.active_relation, stable_relation),
         {:ok, session} <- connect(asset, stable_relation) do
      try do
        do_reconcile(session, request)
      after
        Client.disconnect(session)
      end
    end
  end

  @spec discard(GenerationDiscardRequest.t(), Version.t()) ::
          {:ok, GenerationDiscardResult.t()} | {:error, term()}
  def discard(%GenerationDiscardRequest{} = request, %Version{} = version) do
    with :ok <- GenerationDiscardRequest.validate(request),
         {:ok, asset, stable_relation} <- target_asset(version, request.target_id),
         :ok <- same_relation(request.active_relation, stable_relation),
         {:ok, session} <- connect(asset, stable_relation) do
      try do
        with {:ok, capabilities} <- Client.generation_capabilities(session),
             :ok <-
               same_relation(
                 request.candidate_relation,
                 discard_relation(request, stable_relation, capabilities.max_identifier_bytes)
               ) do
          do_discard(session, request, stable_relation)
        end
      after
        Client.disconnect(session)
      end
    end
  end

  defp discard_relation(request, stable_relation, max_identifier_bytes) do
    case request.relation_kind do
      :candidate ->
        GenerationRelation.candidate(
          stable_relation,
          request.candidate_generation_id,
          max_identifier_bytes
        )

      :retired ->
        GenerationRelation.retired(
          stable_relation,
          request.candidate_generation_id,
          max_identifier_bytes
        )
    end
  end

  defp do_reconcile(session, request) do
    activation = request.activation

    sql_request = %GenerationReconciliation{
      logical_target_id: activation.target_id,
      stable_relation: activation.active_relation
    }

    case Client.reconcile_generation(session, sql_request) do
      {:ok, %SQLMarker{} = marker} ->
        reconcile_marker(session, request, marker)

      {:ok, nil} ->
        {:ok, unknown_reconciliation(request, nil, :generation_marker_missing)}

      {:error, %SQLError{} = error} ->
        {:ok, unknown_reconciliation(request, nil, error)}
    end
  end

  defp reconcile_marker(session, request, %SQLMarker{} = marker) do
    activation = request.activation
    observed_marker = contract_marker(marker)

    cond do
      candidate_marker?(marker, activation) ->
        with {:ok, %GenerationInspection{} = inspection} <-
               Client.inspect_generation(session, activation.active_relation),
             {:ok, :not_found} <-
               Client.inspect_generation(session, activation.candidate_relation) do
          {:ok,
           %GenerationReconciliationResult{
             required_runner_release_id: activation.required_runner_release_id,
             target_id: activation.target_id,
             candidate_generation_id: activation.candidate_generation_id,
             activation_token: activation.activation_token,
             disposition: :candidate_active,
             observed_marker: observed_marker,
             candidate_present: false,
             physical_fingerprint: inspection.physical_fingerprint.fingerprint,
             reconciled_at: DateTime.utc_now(),
             error: nil
           }}
        else
          _unknown ->
            {:ok, unknown_reconciliation(request, observed_marker, :candidate_state_not_proven)}
        end

      previous_marker?(marker, activation) ->
        with {:ok, %GenerationInspection{}} <-
               Client.inspect_generation(session, activation.active_relation),
             {:ok, candidate} <- Client.inspect_generation(session, activation.candidate_relation) do
          {:ok,
           %GenerationReconciliationResult{
             required_runner_release_id: activation.required_runner_release_id,
             target_id: activation.target_id,
             candidate_generation_id: activation.candidate_generation_id,
             activation_token: activation.activation_token,
             disposition: :previous_active,
             observed_marker: observed_marker,
             candidate_present: match?(%GenerationInspection{}, candidate),
             physical_fingerprint: nil,
             reconciled_at: DateTime.utc_now(),
             error: nil
           }}
        else
          _not_proven ->
            {:ok, unknown_reconciliation(request, observed_marker, :candidate_state_not_proven)}
        end

      true ->
        {:ok, unknown_reconciliation(request, observed_marker, :generation_marker_mismatch)}
    end
  end

  defp do_discard(session, request, stable_relation) do
    reconciliation = %GenerationReconciliation{
      logical_target_id: request.target_id,
      stable_relation: stable_relation
    }

    with {:ok, marker} <- Client.reconcile_generation(session, reconciliation),
         {:ok, candidate_before} <- Client.inspect_generation(session, request.candidate_relation) do
      discard = %GenerationDiscard{
        logical_target_id: request.target_id,
        stable_relation: stable_relation,
        candidate_generation_id: request.candidate_generation_id,
        candidate_relation: request.candidate_relation
      }

      case Client.discard_generation(session, discard) do
        {:ok, :discarded} ->
          {:ok,
           %GenerationDiscardResult{
             required_runner_release_id: request.required_runner_release_id,
             target_id: request.target_id,
             candidate_generation_id: request.candidate_generation_id,
             discard_token: request.discard_token,
             outcome: if(candidate_before == :not_found, do: :already_absent, else: :discarded),
             observed_marker: maybe_contract_marker(marker),
             candidate_present: false,
             completed_at: DateTime.utc_now(),
             error: nil
           }}

        {:error, %SQLError{} = error} ->
          {:ok, discard_failure(request, marker, candidate_before, error)}
      end
    else
      {:error, %SQLError{} = error} -> {:ok, discard_failure(request, nil, nil, error)}
    end
  end

  defp activation_result({:ok, %SQLActivationResult{} = result}, request) do
    {:ok,
     %GenerationActivationResult{
       required_runner_release_id: request.required_runner_release_id,
       target_id: request.target_id,
       candidate_generation_id: request.candidate_generation_id,
       activation_token: request.activation_token,
       outcome: :succeeded,
       observed_marker: contract_marker(result.marker),
       candidate_fingerprint: result.candidate_fingerprint,
       physical_fingerprint: result.physical_fingerprint,
       retired_relation: request.retired_relation,
       completed_at: DateTime.utc_now(),
       error: nil
     }}
  end

  defp activation_result({:error, %SQLError{} = error}, request) do
    outcome = if unknown_sql_outcome?(error), do: :outcome_unknown, else: :safe_failure

    {:ok,
     %GenerationActivationResult{
       required_runner_release_id: request.required_runner_release_id,
       target_id: request.target_id,
       candidate_generation_id: request.candidate_generation_id,
       activation_token: request.activation_token,
       outcome: outcome,
       observed_marker: nil,
       candidate_fingerprint: nil,
       physical_fingerprint: nil,
       retired_relation: nil,
       completed_at: DateTime.utc_now(),
       error: runner_error(error, outcome)
     }}
  end

  defp discard_failure(request, marker, candidate_before, error) do
    outcome =
      if unknown_sql_outcome?(error) or active_discard_forbidden?(error),
        do: :outcome_unknown,
        else: :safe_failure

    %GenerationDiscardResult{
      required_runner_release_id: request.required_runner_release_id,
      target_id: request.target_id,
      candidate_generation_id: request.candidate_generation_id,
      discard_token: request.discard_token,
      outcome: outcome,
      observed_marker: maybe_contract_marker(marker),
      candidate_present:
        if(outcome == :outcome_unknown, do: nil, else: candidate_present?(candidate_before)),
      completed_at: DateTime.utc_now(),
      error: runner_error(error, outcome)
    }
  end

  defp unknown_reconciliation(request, marker, reason) do
    activation = request.activation

    %GenerationReconciliationResult{
      required_runner_release_id: activation.required_runner_release_id,
      target_id: activation.target_id,
      candidate_generation_id: activation.candidate_generation_id,
      activation_token: activation.activation_token,
      disposition: :unknown,
      observed_marker: marker,
      candidate_present: nil,
      physical_fingerprint: nil,
      reconciled_at: DateTime.utc_now(),
      error:
        RunnerError.normalize(reason,
          type: :generation_reconciliation_unknown,
          phase: :generation_reconciliation,
          retryable?: false,
          outcome: :unknown
        )
    }
  end

  defp target_asset(%Version{} = version, target_id) do
    case Enum.find(version.manifest.assets, fn
           %Asset{target_descriptor: %TargetDescriptor{target_id: ^target_id}} -> true
           _other -> false
         end) do
      %Asset{} = asset ->
        with {:ok, relation} <- persisted_relation(asset), do: {:ok, asset, relation}

      nil ->
        {:error, :generation_target_not_found}
    end
  end

  defp persisted_relation(%Asset{
         target_descriptor: %TargetDescriptor{},
         relation: %RelationRef{} = relation
       }),
       do: {:ok, relation}

  defp persisted_relation(%Asset{}), do: {:error, :generation_target_not_supported}

  defp connect(asset, %RelationRef{} = relation) do
    opts =
      [registry_name: @runner_registry]
      |> maybe_put_catalog(relation.catalog)
      |> maybe_put_resources(asset.session_requirements.resources)

    with {:ok, session} <- Client.connect(relation.connection, opts) do
      if asset.target_descriptor.adapter == Atom.to_string(session.adapter) do
        {:ok, session}
      else
        Client.disconnect(session)
        {:error, :generation_adapter_identity_mismatch}
      end
    end
  end

  defp validate_activation_relations(request, max_identifier_bytes) do
    with :ok <-
           same_relation(
             request.candidate_relation,
             GenerationRelation.candidate(
               request.active_relation,
               request.candidate_generation_id,
               max_identifier_bytes
             )
           ) do
      same_relation(
        request.retired_relation,
        GenerationRelation.retired(
          request.active_relation,
          request.previous_generation_id,
          max_identifier_bytes
        )
      )
    end
  end

  defp sql_activation(request) do
    %GenerationActivation{
      logical_target_id: request.target_id,
      stable_relation: request.active_relation,
      candidate_relation: request.candidate_relation,
      retired_relation: request.retired_relation,
      expected_active_generation_id: request.previous_generation_id,
      expected_active_marker: sql_marker(request.expected_marker),
      candidate_generation_id: request.candidate_generation_id,
      expected_candidate_fingerprint: request.expected_candidate_fingerprint,
      activation_operation_id: request.rebuild_operation_id,
      activation_token: request.activation_token,
      activated_at: DateTime.utc_now()
    }
  end

  defp sql_initialization(request) do
    %GenerationMarkerInitialization{
      logical_target_id: request.target_id,
      stable_relation: request.active_relation,
      active_generation_id: request.target_generation_id,
      expected_physical_fingerprint: request.expected_physical_fingerprint,
      initialization_operation_id: request.initialization_operation_id,
      initialization_token: request.initialization_token,
      initialized_at: DateTime.utc_now()
    }
  end

  defp initialization_result({:ok, %SQLInitializationResult{} = result}, request) do
    {:ok,
     %ContractInitializationResult{
       required_runner_release_id: request.required_runner_release_id,
       target_id: request.target_id,
       target_generation_id: request.target_generation_id,
       initialization_token: request.initialization_token,
       outcome: :succeeded,
       observed_marker: contract_marker(result.marker),
       physical_fingerprint: result.physical_fingerprint,
       completed_at: DateTime.utc_now(),
       error: nil
     }}
  end

  defp initialization_result({:error, %SQLError{} = error}, request) do
    outcome = if unknown_sql_outcome?(error), do: :outcome_unknown, else: :safe_failure

    {:ok,
     %ContractInitializationResult{
       required_runner_release_id: request.required_runner_release_id,
       target_id: request.target_id,
       target_generation_id: request.target_generation_id,
       initialization_token: request.initialization_token,
       outcome: outcome,
       observed_marker: nil,
       physical_fingerprint: nil,
       completed_at: DateTime.utc_now(),
       error: runner_error(error, outcome)
     }}
  end

  defp candidate_marker?(marker, activation) do
    marker.logical_target_id == activation.target_id and
      marker.active_generation_id == activation.candidate_generation_id and
      marker.activation_operation_id == activation.rebuild_operation_id and
      marker.activation_token == activation.activation_token and
      marker.active_relation == activation.active_relation
  end

  defp contract_marker(%SQLMarker{} = marker) do
    %ContractMarker{
      target_id: marker.logical_target_id,
      active_relation: marker.active_relation,
      active_generation_id: marker.active_generation_id,
      activation_operation_id: marker.activation_operation_id,
      activation_token: marker.activation_token,
      activated_at: marker.activated_at
    }
  end

  defp maybe_contract_marker(nil), do: nil
  defp maybe_contract_marker(%SQLMarker{} = marker), do: contract_marker(marker)

  defp sql_marker(%ContractMarker{} = marker) do
    %SQLMarker{
      logical_target_id: marker.target_id,
      active_relation: marker.active_relation,
      active_generation_id: marker.active_generation_id,
      activation_operation_id: marker.activation_operation_id,
      activation_token: marker.activation_token,
      activated_at: marker.activated_at
    }
  end

  defp previous_marker?(%SQLMarker{} = observed, activation) do
    marker_identity(observed) == marker_identity(activation.expected_marker)
  end

  defp marker_identity(marker) do
    {
      marker.active_relation,
      marker.active_generation_id,
      marker.activation_operation_id,
      marker.activation_token
    }
  end

  defp active_discard_forbidden?(%SQLError{details: details}) when is_map(details),
    do: Map.get(details, :classification) == :active_generation_discard_forbidden

  defp active_discard_forbidden?(_error), do: false

  defp candidate_present?(:not_found), do: false
  defp candidate_present?(%GenerationInspection{}), do: true
  defp candidate_present?(_unknown), do: nil

  defp runner_error(error, :outcome_unknown) do
    RunnerError.normalize(error,
      type: :generation_operation_outcome_unknown,
      phase: :generation_operation,
      retryable?: false,
      outcome: :unknown
    )
  end

  defp runner_error(error, :safe_failure) do
    RunnerError.normalize(error,
      type: :generation_operation_failed,
      phase: :generation_operation,
      retryable?: false,
      outcome: :safe_failure
    )
  end

  defp unknown_sql_outcome?(%SQLError{details: details}) when is_map(details) do
    Map.get(details, :unknown_outcome?) == true or
      Map.get(details, :classification) in [
        :activation_outcome_unknown,
        :unknown_outcome_timeout
      ] or Map.get(details, :transaction_stage) in [:commit, :rollback]
  end

  defp unknown_sql_outcome?(_error), do: false

  defp same_relation(relation, relation), do: :ok
  defp same_relation(_actual, _expected), do: {:error, :generation_relation_mismatch}

  defp maybe_put_catalog(opts, catalog) when is_binary(catalog) and catalog != "",
    do: Keyword.put(opts, :required_catalogs, [catalog])

  defp maybe_put_catalog(opts, _catalog), do: opts

  defp maybe_put_resources(opts, []), do: opts
  defp maybe_put_resources(opts, resources), do: Keyword.put(opts, :required_resources, resources)
end
