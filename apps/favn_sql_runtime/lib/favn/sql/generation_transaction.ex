defmodule Favn.SQL.GenerationTransaction do
  @moduledoc false

  alias Favn.RelationRef

  alias Favn.SQL.{
    Error,
    GenerationActivation,
    GenerationActivationResult,
    GenerationDiscard,
    GenerationInspection,
    GenerationMarker,
    GenerationMarkerInitialization,
    GenerationMarkerInitializationResult,
    GenerationReconciliation,
    GenerationRelation,
    Relation,
    Result
  }

  alias Favn.TargetCompatibility.PhysicalFingerprint

  @spec inspect(module(), term(), module(), RelationRef.t(), keyword()) ::
          {:ok, GenerationInspection.t() | :not_found} | {:error, Error.t()}
  def inspect(adapter, conn, adapter_identity, %RelationRef{} = ref, opts) do
    with {:ok, relation} <- adapter.relation(conn, ref, opts) do
      inspect_existing(adapter, conn, adapter_identity, ref, relation, opts)
    end
  end

  @spec initialize_marker(
          module(),
          term(),
          module(),
          GenerationMarkerInitialization.t(),
          keyword()
        ) :: {:ok, GenerationMarkerInitializationResult.t()} | {:error, Error.t()}
  def initialize_marker(
        adapter,
        conn,
        adapter_identity,
        %GenerationMarkerInitialization{} = request,
        opts
      ) do
    case adapter.transaction(
           conn,
           fn tx_conn ->
             initialize_marker_transaction(adapter, tx_conn, adapter_identity, request, opts)
           end,
           preserve_body_result_on_commit_error?: true
         ) do
      {:ok, %GenerationMarkerInitializationResult{} = result} -> {:ok, result}
      {:error, %Error{} = error} -> {:error, mutation_error(error, :initialize_generation_marker)}
    end
  end

  @spec activate(module(), term(), module(), GenerationActivation.t(), keyword()) ::
          {:ok, GenerationActivationResult.t()} | {:error, Error.t()}
  def activate(adapter, conn, adapter_identity, %GenerationActivation{} = request, opts) do
    with :ok <- validate_activation(request, adapter_identity) do
      case adapter.transaction(
             conn,
             fn tx_conn ->
               activation_transaction(adapter, tx_conn, adapter_identity, request, opts)
             end,
             preserve_body_result_on_commit_error?: true
           ) do
        {:ok, %GenerationActivationResult{} = result} ->
          {:ok, result}

        {:error, %Error{} = error} ->
          {:error, activation_error(error)}
      end
    end
  end

  @spec reconcile(module(), term(), module(), GenerationReconciliation.t(), keyword()) ::
          {:ok, GenerationMarker.t() | nil} | {:error, Error.t()}
  def reconcile(adapter, conn, adapter_identity, %GenerationReconciliation{} = request, opts) do
    marker_ref = GenerationRelation.marker(request.stable_relation)

    with {:ok, relation} <- adapter.relation(conn, marker_ref, opts) do
      case relation do
        nil -> {:ok, nil}
        %Relation{type: :table} -> read_marker(adapter, conn, request, marker_ref, opts)
        %Relation{} -> {:error, marker_relation_error(adapter_identity, marker_ref)}
      end
    end
  end

  @spec discard(module(), term(), module(), GenerationDiscard.t(), keyword()) ::
          :ok | {:error, Error.t()}
  def discard(adapter, conn, adapter_identity, %GenerationDiscard{} = request, opts) do
    reconciliation = %GenerationReconciliation{
      logical_target_id: request.logical_target_id,
      stable_relation: request.stable_relation
    }

    case adapter.transaction(
           conn,
           fn tx_conn ->
             with {:ok, marker} <-
                    reconcile(adapter, tx_conn, adapter_identity, reconciliation, opts),
                  :ok <- ensure_not_active(marker, request, adapter_identity),
                  {:ok, %Result{}} <-
                    adapter.execute(
                      tx_conn,
                      ["DROP TABLE IF EXISTS ", qualified(request.candidate_relation)],
                      opts
                    ) do
               {:ok, :discarded}
             end
           end,
           []
         ) do
      {:ok, :discarded} ->
        :ok

      {:error, %Error{} = error} ->
        {:error, generation_error(error, adapter_identity, :discard_generation)}
    end
  end

  defp activation_transaction(adapter, conn, adapter_identity, request, opts) do
    marker_ref = GenerationRelation.marker(request.stable_relation)

    with {:ok, _result} <- adapter.execute(conn, create_marker_table(marker_ref), opts),
         {:ok, observed_marker} <-
           read_marker(
             adapter,
             conn,
             %GenerationReconciliation{
               logical_target_id: request.logical_target_id,
               stable_relation: request.stable_relation
             },
             marker_ref,
             opts
           ),
         {:ok, mode} <- activation_mode(observed_marker, request, adapter_identity),
         {:ok, result} <-
           perform_activation(mode, adapter, conn, adapter_identity, request, marker_ref, opts) do
      {:ok, result}
    end
  end

  defp initialize_marker_transaction(adapter, conn, adapter_identity, request, opts) do
    marker_ref = GenerationRelation.marker(request.stable_relation)

    reconciliation = %GenerationReconciliation{
      logical_target_id: request.logical_target_id,
      stable_relation: request.stable_relation
    }

    with {:ok, _result} <- adapter.execute(conn, create_marker_table(marker_ref), opts),
         {:ok, observed_marker} <- read_marker(adapter, conn, reconciliation, marker_ref, opts),
         :ok <- validate_initial_marker(observed_marker, request, adapter_identity),
         {:ok, %GenerationInspection{} = inspection} <-
           inspect(adapter, conn, adapter_identity, request.stable_relation, opts),
         :ok <-
           validate_physical_fingerprint(
             inspection,
             request.expected_physical_fingerprint,
             adapter_identity,
             :initial_generation_fingerprint_mismatch
           ),
         marker <- observed_marker || marker_from_request(request),
         :ok <-
           maybe_write_initial_marker(adapter, conn, marker_ref, observed_marker, marker, opts) do
      {:ok,
       %GenerationMarkerInitializationResult{
         marker: marker,
         physical_fingerprint: inspection.physical_fingerprint.fingerprint,
         inspection: inspection
       }}
    else
      {:ok, :not_found} ->
        {:error, missing_relation_error(adapter_identity, request.stable_relation)}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp activation_mode(
         %GenerationMarker{
           active_generation_id: generation_id,
           activation_operation_id: activation_operation_id,
           activation_token: activation_token
         } = observed,
         %GenerationActivation{
           candidate_generation_id: generation_id,
           activation_operation_id: activation_operation_id,
           activation_token: activation_token
         } = request,
         _adapter_identity
       ) do
    if marker_identity(observed) == marker_identity(marker_from_request(request)),
      do: {:ok, {:already_activated, observed}},
      else: {:error, marker_mismatch_error(:already_activated_marker_mismatch)}
  end

  defp activation_mode(nil, %GenerationActivation{expected_active_generation_id: nil}, _adapter),
    do: {:ok, :activate}

  defp activation_mode(
         %GenerationMarker{} = observed,
         %GenerationActivation{
           expected_active_generation_id: expected,
           expected_active_marker: %GenerationMarker{} = expected_marker
         },
         _adapter
       ) do
    if observed.active_generation_id == expected and
         marker_identity(observed) == marker_identity(expected_marker),
       do: {:ok, :activate},
       else: {:error, marker_mismatch_error(:unexpected_active_marker)}
  end

  defp activation_mode(_marker, _request, adapter_identity),
    do: {:error, marker_mismatch_error(:unexpected_active_generation, adapter_identity)}

  defp perform_activation(
         {:already_activated, observed_marker},
         adapter,
         conn,
         adapter_identity,
         request,
         _marker_ref,
         opts
       ) do
    with {:ok, %GenerationInspection{} = inspection} <-
           inspect(adapter, conn, adapter_identity, request.stable_relation, opts) do
      {:ok,
       %GenerationActivationResult{
         marker: observed_marker,
         candidate_fingerprint: request.expected_candidate_fingerprint,
         physical_fingerprint: inspection.physical_fingerprint.fingerprint,
         inspection: inspection
       }}
    else
      {:ok, :not_found} ->
        {:error, missing_relation_error(adapter_identity, request.stable_relation)}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp perform_activation(:activate, adapter, conn, adapter_identity, request, marker_ref, opts) do
    with {:ok, candidate} <- adapter.relation(conn, request.candidate_relation, opts),
         :ok <- require_table(candidate, adapter_identity, request.candidate_relation),
         {:ok, %GenerationInspection{} = candidate_inspection} <-
           inspect(adapter, conn, adapter_identity, request.candidate_relation, opts),
         :ok <-
           validate_candidate_fingerprint(candidate_inspection, request, adapter_identity),
         {:ok, active} <- adapter.relation(conn, request.stable_relation, opts),
         :ok <- validate_active_relation(active, request, adapter_identity),
         {:ok, retired} <- adapter.relation(conn, request.retired_relation, opts),
         :ok <- require_retired_absent(retired, adapter_identity, request.retired_relation),
         :ok <- maybe_retire_active(adapter, conn, active, request, opts),
         {:ok, _result} <-
           adapter.execute(
             conn,
             rename_statement(request.candidate_relation, request.stable_relation),
             opts
           ),
         {:ok, _result} <- adapter.execute(conn, delete_marker(marker_ref, request), opts),
         {:ok, _result} <-
           adapter.execute(conn, insert_marker(marker_ref, marker_from_request(request)), opts),
         {:ok, %GenerationInspection{} = inspection} <-
           inspect(adapter, conn, adapter_identity, request.stable_relation, opts) do
      {:ok,
       %GenerationActivationResult{
         marker: marker_from_request(request),
         candidate_fingerprint: candidate_inspection.physical_fingerprint.fingerprint,
         physical_fingerprint: inspection.physical_fingerprint.fingerprint,
         inspection: inspection
       }}
    else
      {:ok, :not_found} ->
        {:error, missing_relation_error(adapter_identity, request.stable_relation)}

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp inspect_existing(_adapter, _conn, _adapter_identity, _ref, nil, _opts),
    do: {:ok, :not_found}

  defp inspect_existing(adapter, conn, adapter_identity, ref, %Relation{} = relation, opts) do
    with {:ok, columns} <- adapter.columns(conn, ref, opts),
         {:ok, physical_fingerprint} <-
           PhysicalFingerprint.new(
             adapter: adapter_identity,
             relation: relation,
             columns: columns
           ) do
      {:ok,
       %GenerationInspection{
         relation_ref: ref,
         relation: relation,
         columns: columns,
         physical_fingerprint: physical_fingerprint
       }}
    else
      {:error, %Error{} = error} ->
        {:error, error}

      {:error, reason} ->
        {:error,
         %Error{
           type: :introspection_mismatch,
           message: "generation relation inspection could not be fingerprinted",
           retryable?: false,
           adapter: adapter_identity,
           operation: :inspect_generation,
           details: %{classification: :physical_inspection_failed, reason: reason}
         }}
    end
  end

  defp read_marker(adapter, conn, request, marker_ref, opts) do
    sql = [
      "SELECT logical_target_id, active_catalog, active_schema, active_name, ",
      "active_generation_id, activation_operation_id, activation_token, activated_at ",
      "FROM ",
      qualified(marker_ref),
      " WHERE logical_target_id = ",
      literal(request.logical_target_id),
      " LIMIT 2"
    ]

    with {:ok, %Result{rows: rows}} <- adapter.query(conn, sql, opts) do
      case rows do
        [] -> {:ok, nil}
        [row] -> decode_marker(row, request)
        [_first, _second] -> {:error, marker_mismatch_error(:duplicate_marker)}
      end
    end
  end

  defp decode_marker(row, request) when is_map(row) do
    with {:ok, activated_at} <- decode_datetime(field(row, "activated_at")),
         active_name when is_binary(active_name) and active_name != "" <-
           field(row, "active_name"),
         generation_id when is_binary(generation_id) and generation_id != "" <-
           field(row, "active_generation_id"),
         operation_id when is_binary(operation_id) and operation_id != "" <-
           field(row, "activation_operation_id"),
         token when is_binary(token) and token != "" <- field(row, "activation_token") do
      {:ok,
       %GenerationMarker{
         logical_target_id: request.logical_target_id,
         active_relation: %RelationRef{
           connection: request.stable_relation.connection,
           catalog: field(row, "active_catalog"),
           schema: field(row, "active_schema"),
           name: active_name
         },
         active_generation_id: generation_id,
         activation_operation_id: operation_id,
         activation_token: token,
         activated_at: activated_at
       }}
    else
      _reason -> {:error, marker_mismatch_error(:invalid_marker)}
    end
  end

  defp create_marker_table(marker_ref) do
    [
      "CREATE TABLE IF NOT EXISTS ",
      qualified(marker_ref),
      " (logical_target_id VARCHAR NOT NULL, active_catalog VARCHAR, ",
      "active_schema VARCHAR, active_name VARCHAR NOT NULL, ",
      "active_generation_id VARCHAR NOT NULL, activation_operation_id VARCHAR NOT NULL, ",
      "activation_token VARCHAR NOT NULL, ",
      "activated_at TIMESTAMPTZ NOT NULL)"
    ]
  end

  defp delete_marker(marker_ref, request) do
    [
      "DELETE FROM ",
      qualified(marker_ref),
      " WHERE logical_target_id = ",
      literal(request.logical_target_id)
    ]
  end

  defp insert_marker(marker_ref, %GenerationMarker{} = marker) do
    stable = marker.active_relation

    [
      "INSERT INTO ",
      qualified(marker_ref),
      " (logical_target_id, active_catalog, active_schema, active_name, ",
      "active_generation_id, activation_operation_id, activation_token, activated_at) VALUES (",
      literal(marker.logical_target_id),
      ", ",
      nullable_literal(stable.catalog),
      ", ",
      nullable_literal(stable.schema),
      ", ",
      literal(stable.name),
      ", ",
      literal(marker.active_generation_id),
      ", ",
      literal(marker.activation_operation_id),
      ", ",
      literal(marker.activation_token),
      ", CAST(",
      literal(DateTime.to_iso8601(marker.activated_at)),
      " AS TIMESTAMPTZ))"
    ]
  end

  defp maybe_retire_active(_adapter, _conn, nil, _request, _opts), do: :ok

  defp maybe_retire_active(adapter, conn, %Relation{}, request, opts) do
    case adapter.execute(
           conn,
           rename_statement(request.stable_relation, request.retired_relation),
           opts
         ) do
      {:ok, %Result{}} -> :ok
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp rename_statement(from, to),
    do: ["ALTER TABLE ", qualified(from), " RENAME TO ", ident(to.name)]

  defp validate_activation(%GenerationActivation{} = request, adapter_identity) do
    relations = [request.stable_relation, request.candidate_relation, request.retired_relation]

    cond do
      not Enum.all?(
        relations,
        &match?(%RelationRef{name: name} when is_binary(name) and name != "", &1)
      ) ->
        {:error, invalid_activation_error(adapter_identity, :invalid_relation)}

      Enum.uniq_by(relations, &namespace/1) |> length() != 1 ->
        {:error, invalid_activation_error(adapter_identity, :relation_namespace_mismatch)}

      length(Enum.uniq_by(relations, & &1.name)) != 3 ->
        {:error, invalid_activation_error(adapter_identity, :relation_name_collision)}

      not valid_text?(request.logical_target_id) or
        not valid_text?(request.candidate_generation_id) or
        not valid_text?(request.expected_candidate_fingerprint) or
        not valid_text?(request.activation_operation_id) or
        not valid_text?(request.activation_token) or
          not match?(%DateTime{}, request.activated_at) ->
        {:error, invalid_activation_error(adapter_identity, :invalid_identity)}

      not (is_nil(request.expected_active_generation_id) or
               valid_text?(request.expected_active_generation_id)) ->
        {:error, invalid_activation_error(adapter_identity, :invalid_expected_generation)}

      true ->
        validate_expected_marker(request, adapter_identity)
    end
  end

  defp validate_expected_marker(
         %GenerationActivation{expected_active_generation_id: nil, expected_active_marker: nil},
         _adapter
       ),
       do: :ok

  defp validate_expected_marker(
         %GenerationActivation{
           logical_target_id: logical_target_id,
           stable_relation: stable_relation,
           expected_active_generation_id: generation_id,
           expected_active_marker: %GenerationMarker{
             logical_target_id: logical_target_id,
             active_relation: stable_relation,
             active_generation_id: generation_id
           }
         },
         _adapter
       ),
       do: :ok

  defp validate_expected_marker(_request, adapter),
    do: {:error, invalid_activation_error(adapter, :invalid_expected_marker)}

  defp validate_active_relation(
         nil,
         %GenerationActivation{expected_active_generation_id: nil},
         _adapter
       ),
       do: :ok

  defp validate_active_relation(
         %Relation{type: :table},
         %GenerationActivation{expected_active_generation_id: expected},
         _adapter
       )
       when is_binary(expected),
       do: :ok

  defp validate_active_relation(nil, _request, adapter),
    do: {:error, marker_mismatch_error(:active_relation_missing, adapter)}

  defp validate_active_relation(%Relation{}, _request, adapter),
    do: {:error, marker_mismatch_error(:active_relation_not_table, adapter)}

  defp require_table(%Relation{type: :table}, _adapter, _relation), do: :ok

  defp require_table(nil, adapter, relation),
    do: {:error, missing_relation_error(adapter, relation)}

  defp require_table(%Relation{}, adapter, _relation),
    do: {:error, marker_mismatch_error(:candidate_relation_not_table, adapter)}

  defp require_retired_absent(nil, _adapter, _relation), do: :ok

  defp require_retired_absent(%Relation{}, adapter, _relation),
    do: {:error, marker_mismatch_error(:retired_relation_already_exists, adapter)}

  defp marker_from_request(%GenerationActivation{} = request) do
    %GenerationMarker{
      logical_target_id: request.logical_target_id,
      active_relation: request.stable_relation,
      active_generation_id: request.candidate_generation_id,
      activation_operation_id: request.activation_operation_id,
      activation_token: request.activation_token,
      activated_at: request.activated_at
    }
  end

  defp marker_from_request(%GenerationMarkerInitialization{} = request) do
    %GenerationMarker{
      logical_target_id: request.logical_target_id,
      active_relation: request.stable_relation,
      active_generation_id: request.active_generation_id,
      activation_operation_id: request.initialization_operation_id,
      activation_token: request.initialization_token,
      activated_at: request.initialized_at
    }
  end

  defp validate_candidate_fingerprint(
         %GenerationInspection{physical_fingerprint: %{fingerprint: fingerprint}},
         %GenerationActivation{expected_candidate_fingerprint: fingerprint},
         _adapter
       ),
       do: :ok

  defp validate_candidate_fingerprint(
         %GenerationInspection{physical_fingerprint: %{fingerprint: observed}},
         %GenerationActivation{expected_candidate_fingerprint: expected},
         adapter
       ) do
    {:error,
     %Error{
       type: :introspection_mismatch,
       message: "candidate generation physical fingerprint does not match activation intent",
       retryable?: false,
       adapter: adapter,
       operation: :activate_generation,
       details: %{
         classification: :candidate_fingerprint_mismatch,
         expected: expected,
         observed: observed
       }
     }}
  end

  defp validate_physical_fingerprint(
         %GenerationInspection{physical_fingerprint: %{fingerprint: fingerprint}},
         fingerprint,
         _adapter,
         _classification
       ),
       do: :ok

  defp validate_physical_fingerprint(
         %GenerationInspection{physical_fingerprint: %{fingerprint: observed}},
         expected,
         adapter,
         classification
       ) do
    {:error,
     %Error{
       type: :introspection_mismatch,
       message: "generation relation physical fingerprint does not match initialization intent",
       retryable?: false,
       adapter: adapter,
       operation: :initialize_generation_marker,
       details: %{classification: classification, expected: expected, observed: observed}
     }}
  end

  defp validate_initial_marker(nil, %GenerationMarkerInitialization{}, _adapter), do: :ok

  defp validate_initial_marker(
         %GenerationMarker{} = observed,
         %GenerationMarkerInitialization{} = request,
         adapter
       ) do
    if marker_identity(observed) == marker_identity(marker_from_request(request)),
      do: :ok,
      else: {:error, marker_mismatch_error(:initial_generation_marker_mismatch, adapter)}
  end

  defp maybe_write_initial_marker(
         _adapter,
         _conn,
         _marker_ref,
         %GenerationMarker{},
         _marker,
         _opts
       ),
       do: :ok

  defp maybe_write_initial_marker(adapter, conn, marker_ref, nil, marker, opts) do
    case adapter.execute(conn, insert_marker(marker_ref, marker), opts) do
      {:ok, %Result{}} -> :ok
      {:error, %Error{} = error} -> {:error, error}
    end
  end

  defp ensure_not_active(
         %GenerationMarker{active_generation_id: generation_id},
         %GenerationDiscard{candidate_generation_id: generation_id},
         adapter
       ) do
    {:error,
     %Error{
       type: :introspection_mismatch,
       message: "cannot discard the active target generation",
       retryable?: false,
       adapter: adapter,
       operation: :discard_generation,
       details: %{
         classification: :active_generation_discard_forbidden,
         generation_id: generation_id
       }
     }}
  end

  defp ensure_not_active(_marker, %GenerationDiscard{}, _adapter), do: :ok

  defp decode_datetime(%DateTime{} = value), do: {:ok, value}

  defp decode_datetime(%NaiveDateTime{} = value),
    do: DateTime.from_naive(value, "Etc/UTC")

  defp decode_datetime({{year, month, day}, {hour, minute, second, microsecond}}) do
    with {:ok, date} <- Date.new(year, month, day),
         {:ok, time} <- Time.new(hour, minute, second, {microsecond, 6}),
         {:ok, naive} <- NaiveDateTime.new(date, time) do
      DateTime.from_naive(naive, "Etc/UTC")
    end
  end

  defp decode_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> {:error, :invalid_datetime}
    end
  end

  defp decode_datetime(_value), do: {:error, :invalid_datetime}

  defp activation_error(%Error{} = error) do
    if activation_outcome_unknown?(error) do
      details =
        error.details
        |> Map.new()
        |> Map.put(:classification, :activation_outcome_unknown)
        |> Map.put(:unknown_outcome?, true)

      %Error{
        error
        | message: "target generation activation commit outcome is unknown",
          retryable?: false,
          operation: :activate_generation,
          details: details
      }
    else
      %Error{error | retryable?: false, operation: :activate_generation}
    end
  end

  defp mutation_error(%Error{} = error, operation) do
    if activation_outcome_unknown?(error) do
      details =
        error.details
        |> Map.new()
        |> Map.put(:classification, :generation_mutation_outcome_unknown)
        |> Map.put(:unknown_outcome?, true)

      %Error{error | retryable?: false, operation: operation, details: details}
    else
      %Error{error | retryable?: false, operation: operation}
    end
  end

  defp activation_outcome_unknown?(%Error{details: details}) when is_map(details) do
    Map.get(details, :transaction_stage) in [:commit, :rollback] or
      Map.has_key?(details, :transaction_body_result) or
      nested_commit_error?(Map.get(details, :original_error))
  end

  defp activation_outcome_unknown?(_error), do: false

  defp nested_commit_error?(%{details: details}) when is_map(details) do
    Map.get(details, :transaction_stage) in [:commit, :rollback] or
      Map.has_key?(details, :transaction_body_result)
  end

  defp nested_commit_error?(_error), do: false

  defp generation_error(%Error{} = error, adapter, operation),
    do: %Error{error | adapter: error.adapter || adapter, operation: operation, retryable?: false}

  defp invalid_activation_error(adapter, reason) do
    %Error{
      type: :introspection_mismatch,
      message: "invalid target generation activation request",
      retryable?: false,
      adapter: adapter,
      operation: :activate_generation,
      details: %{classification: :invalid_generation_activation, reason: reason}
    }
  end

  defp marker_mismatch_error(reason, adapter \\ nil) do
    %Error{
      type: :introspection_mismatch,
      message: "target generation marker does not match activation intent",
      retryable?: false,
      adapter: adapter,
      operation: :activate_generation,
      details: %{classification: :generation_marker_mismatch, reason: reason}
    }
  end

  defp marker_relation_error(adapter, marker_ref) do
    %Error{
      type: :introspection_mismatch,
      message: "target generation marker relation is not a table",
      retryable?: false,
      adapter: adapter,
      operation: :reconcile_generation,
      details: %{classification: :generation_marker_mismatch, relation: relation_map(marker_ref)}
    }
  end

  defp missing_relation_error(adapter, relation) do
    %Error{
      type: :missing_relation,
      message: "target generation relation does not exist",
      retryable?: false,
      adapter: adapter,
      operation: :activate_generation,
      details: %{classification: :missing_relation, relation: relation_map(relation)}
    }
  end

  defp relation_map(ref), do: Map.take(ref, [:catalog, :schema, :name])

  defp marker_identity(marker) do
    {
      marker.logical_target_id,
      marker.active_relation,
      marker.active_generation_id,
      marker.activation_operation_id,
      marker.activation_token
    }
  end

  defp namespace(ref), do: {ref.connection, ref.catalog, ref.schema}

  defp valid_text?(value), do: is_binary(value) and value != ""

  defp field(row, key), do: Map.get(row, key, Map.get(row, String.to_atom(key)))

  defp qualified(%RelationRef{catalog: nil, schema: nil, name: name}), do: ident(name)

  defp qualified(%RelationRef{catalog: nil, schema: schema, name: name}),
    do: [ident(schema), ".", ident(name)]

  defp qualified(%RelationRef{catalog: catalog, schema: nil, name: name}),
    do: [ident(catalog), ".", ident(name)]

  defp qualified(%RelationRef{catalog: catalog, schema: schema, name: name}),
    do: [ident(catalog), ".", ident(schema), ".", ident(name)]

  defp ident(identifier), do: ["\"", String.replace(to_string(identifier), "\"", "\"\""), "\""]
  defp literal(value), do: ["'", String.replace(value, "'", "''"), "'"]
  defp nullable_literal(nil), do: "NULL"
  defp nullable_literal(value), do: literal(value)
end
