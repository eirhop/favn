defmodule FavnStoragePostgres.Idempotency.Transaction do
  @moduledoc """
  Commits an API idempotency record and its database-local mutation atomically.

  Stores normally reserve and commit inside one transaction. A command whose
  planning phase performs durable work may reserve first, then present the
  returned generation when committing its authoritative result.
  """

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.CommandIdempotency
  alias FavnOrchestrator.Persistence.Error
  alias FavnStoragePostgres.Repo

  @max_response_bytes 65_536
  @preparation_lease_seconds 60

  @type encoded_result :: %{
          required(:response) => map(),
          optional(:response_status) => non_neg_integer() | nil,
          optional(:resource_kind) => String.t() | nil,
          optional(:resource_id) => String.t() | nil
        }

  @doc "Executes or atomically replays one command inside the caller's transaction."
  @spec execute!(
          String.t(),
          CommandIdempotency.t() | nil,
          (-> result),
          (result -> {:ok, encoded_result()} | {:error, term()}),
          (encoded_result() -> {:ok, result} | {:error, term()})
        ) :: result
        when result: term()
  def execute!(_workspace_id, nil, mutation, _encode, _decode) when is_function(mutation, 0),
    do: mutation.()

  def execute!(workspace_id, %CommandIdempotency{} = context, mutation, encode, decode)
      when is_binary(workspace_id) and is_function(mutation, 0) and is_function(encode, 1) and
             is_function(decode, 1) do
    {_outcome, result} =
      execute_with_outcome!(workspace_id, context, mutation, encode, decode)

    result
  end

  @doc "Executes or replays a command and reports which path produced the result."
  @spec execute_with_outcome!(
          String.t(),
          CommandIdempotency.t() | nil,
          (-> result),
          (result -> {:ok, encoded_result()} | {:error, term()}),
          (encoded_result() -> {:ok, result} | {:error, term()})
        ) :: {:new | :replay, result}
        when result: term()
  def execute_with_outcome!(_workspace_id, nil, mutation, _encode, _decode)
      when is_function(mutation, 0),
      do: {:new, mutation.()}

  def execute_with_outcome!(
        workspace_id,
        %CommandIdempotency{} = context,
        mutation,
        encode,
        decode
      )
      when is_binary(workspace_id) and is_function(mutation, 0) and is_function(encode, 1) and
             is_function(decode, 1) do
    validate_context!(workspace_id, context)

    if is_integer(context.reservation_generation) do
      {:new, commit_prepared!(context, workspace_id, mutation, encode)}
    else
      execute_unprepared!(workspace_id, context, mutation, encode, decode)
    end
  end

  @doc "Reserves a multi-stage command or returns its exact committed result."
  @spec prepare!(
          String.t(),
          CommandIdempotency.t(),
          (encoded_result() -> {:ok, result} | {:error, term()})
        ) :: {:new, pos_integer()} | {:replay, result}
        when result: term()
  def prepare!(workspace_id, %CommandIdempotency{} = context, decode)
      when is_binary(workspace_id) and is_function(decode, 1) do
    validate_context!(workspace_id, context)

    if reserve(context, workspace_id) do
      {:new, 1}
    else
      prepare_existing!(context, workspace_id, decode)
    end
  end

  @doc "Renews one current generation-fenced multi-stage command reservation."
  @spec heartbeat!(String.t(), CommandIdempotency.t()) :: :ok
  def heartbeat!(workspace_id, %CommandIdempotency{reservation_generation: generation} = context)
      when is_binary(workspace_id) and is_integer(generation) and generation > 0 do
    validate_context!(workspace_id, context)

    result =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
          AND reservation_generation = $7 AND status = 'started'
        """,
        identity_params(workspace_id, context) ++
          [context.request_fingerprint, generation]
      )

    if result.num_rows != 1 do
      Repo.rollback(Error.new(:conflict, "deployment-planning reservation is no longer current"))
    end

    :ok
  end

  @doc "Releases one current uncommitted multi-stage command reservation."
  @spec abandon!(String.t(), CommandIdempotency.t()) :: :ok
  def abandon!(workspace_id, %CommandIdempotency{reservation_generation: generation} = context)
      when is_binary(workspace_id) and is_integer(generation) and generation > 0 do
    validate_context!(workspace_id, context)

    SQL.query!(
      Repo,
      """
      DELETE FROM favn_control.idempotency_records
      WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
        AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
        AND reservation_generation = $7 AND status = 'started'
      """,
      identity_params(workspace_id, context) ++
        [context.request_fingerprint, generation]
    )

    :ok
  end

  defp execute_unprepared!(workspace_id, context, mutation, encode, decode) do
    if reserve(context, workspace_id),
      do: {:new, commit_new!(context, workspace_id, mutation, encode)},
      else: replay_or_replace_expired!(context, workspace_id, mutation, encode, decode)
  end

  defp prepare_existing!(context, workspace_id, decode) do
    row = lock_record!(workspace_id, context)

    cond do
      row.expired? ->
        generation = row.reservation_generation + 1
        replace_expired!(workspace_id, context, generation)
        {:new, generation}

      row.request_fingerprint != context.request_fingerprint ->
        Repo.rollback(
          Error.new(:conflict, "idempotency key was reused with different command content")
        )

      row.status == "committed" ->
        decode_replay!(row, decode)

      row.stale? ->
        generation = row.reservation_generation + 1
        take_over_stale!(workspace_id, context, row.reservation_generation, generation)
        {:new, generation}

      true ->
        Repo.rollback(
          Error.new(:conflict, "deployment command is already in progress",
            retryable?: true,
            details: %{reason: :command_in_progress}
          )
        )
    end
  end

  defp reserve(context, workspace_id) do
    result =
      SQL.query!(
        Repo,
        """
        INSERT INTO favn_control.idempotency_records (
          workspace_id, operation, principal_kind, principal_id, key_hash,
          request_fingerprint, status, reservation_generation, expires_at,
          inserted_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, 'started', 1, $7, clock_timestamp(), clock_timestamp())
        ON CONFLICT (workspace_id, operation, principal_kind, principal_id, key_hash)
        DO NOTHING
        RETURNING reservation_generation
        """,
        identity_params(workspace_id, context) ++
          [context.request_fingerprint, context.expires_at]
      )

    result.num_rows == 1
  end

  defp replay_or_replace_expired!(context, workspace_id, mutation, encode, decode) do
    row = lock_record!(workspace_id, context)

    cond do
      row.expired? ->
        replace_expired!(workspace_id, context, row.reservation_generation + 1)
        {:new, commit_new!(context, workspace_id, mutation, encode)}

      row.request_fingerprint != context.request_fingerprint ->
        Repo.rollback(
          Error.new(:conflict, "idempotency key was reused with different command content")
        )

      row.status == "committed" ->
        decode_replay!(row, decode)

      true ->
        Repo.rollback(Error.new(:internal, "committed idempotency record is incomplete"))
    end
  end

  defp decode_replay!(row, decode) do
    encoded = %{
      response: row.response || %{},
      response_status: row.response_status,
      resource_kind: row.resource_kind,
      resource_id: row.resource_id
    }

    case decode.(encoded) do
      {:ok, result} -> {:replay, result}
      {:error, %Error{} = error} -> Repo.rollback(error)
      {:error, reason} -> Repo.rollback(internal_error("decode", reason))
    end
  end

  defp commit_new!(context, workspace_id, mutation, encode) do
    result = mutation.()

    encoded =
      case encode.(result) do
        {:ok, encoded} when is_map(encoded) -> validate_encoded!(encoded)
        {:error, %Error{} = error} -> Repo.rollback(error)
        {:error, reason} -> Repo.rollback(internal_error("encode", reason))
        unexpected -> Repo.rollback(internal_error("encode", unexpected))
      end

    update =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET status = 'committed', response = $7, response_status = $8,
            resource_kind = $9, resource_id = $10, updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
          AND status = 'started'
        """,
        identity_params(workspace_id, context) ++
          [
            context.request_fingerprint,
            encoded.response,
            Map.get(encoded, :response_status),
            Map.get(encoded, :resource_kind),
            Map.get(encoded, :resource_id)
          ]
      )

    if update.num_rows != 1 do
      Repo.rollback(Error.new(:internal, "idempotency result was not committed"))
    end

    result
  end

  defp commit_prepared!(context, workspace_id, mutation, encode) do
    row = lock_record!(workspace_id, context)

    unless row.status == "started" and
             row.request_fingerprint == context.request_fingerprint and
             row.reservation_generation == context.reservation_generation do
      Repo.rollback(Error.new(:conflict, "prepared idempotency reservation is no longer current"))
    end

    result = mutation.()

    encoded =
      case encode.(result) do
        {:ok, encoded} when is_map(encoded) -> validate_encoded!(encoded)
        {:error, %Error{} = error} -> Repo.rollback(error)
        {:error, reason} -> Repo.rollback(internal_error("encode", reason))
        unexpected -> Repo.rollback(internal_error("encode", unexpected))
      end

    update =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET status = 'committed', response = $8, response_status = $9,
            resource_kind = $10, resource_id = $11, updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
          AND reservation_generation = $7 AND status = 'started'
        """,
        identity_params(workspace_id, context) ++
          [
            context.request_fingerprint,
            context.reservation_generation,
            encoded.response,
            Map.get(encoded, :response_status),
            Map.get(encoded, :resource_kind),
            Map.get(encoded, :resource_id)
          ]
      )

    if update.num_rows != 1,
      do: Repo.rollback(Error.new(:internal, "prepared idempotency result was not committed"))

    result
  end

  defp lock_record!(workspace_id, context) do
    result =
      SQL.query!(
        Repo,
        """
        SELECT request_fingerprint, status, response, response_status,
               resource_kind, resource_id, expires_at, reservation_generation,
               expires_at <= clock_timestamp() AS expired,
               updated_at <= clock_timestamp() - interval '#{@preparation_lease_seconds} seconds'
                 AS stale
        FROM favn_control.idempotency_records
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5
        FOR UPDATE
        """,
        identity_params(workspace_id, context)
      )

    case result.rows do
      [
        [
          fingerprint,
          status,
          response,
          response_status,
          resource_kind,
          resource_id,
          expires_at,
          generation,
          expired?,
          stale?
        ]
      ] ->
        %{
          request_fingerprint: fingerprint,
          status: status,
          response: response,
          response_status: response_status,
          resource_kind: resource_kind,
          resource_id: resource_id,
          expires_at: expires_at,
          reservation_generation: generation,
          expired?: expired?,
          stale?: stale?
        }

      [] ->
        Repo.rollback(Error.new(:internal, "idempotency record disappeared during command"))
    end
  end

  defp replace_expired!(workspace_id, context, next_generation) do
    result =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET request_fingerprint = $6, status = 'started', response = NULL,
            response_status = NULL, resource_kind = NULL, resource_id = NULL,
            reservation_generation = $7, expires_at = $8, updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5
          AND expires_at <= clock_timestamp()
        """,
        identity_params(workspace_id, context) ++
          [context.request_fingerprint, next_generation, context.expires_at]
      )

    if result.num_rows != 1,
      do: Repo.rollback(Error.new(:internal, "expired idempotency record was not replaced"))
  end

  defp take_over_stale!(workspace_id, context, previous_generation, next_generation) do
    result =
      SQL.query!(
        Repo,
        """
        UPDATE favn_control.idempotency_records
        SET reservation_generation = $7, expires_at = $8, updated_at = clock_timestamp()
        WHERE workspace_id = $1 AND operation = $2 AND principal_kind = $3
          AND principal_id = $4 AND key_hash = $5 AND request_fingerprint = $6
          AND reservation_generation = $9 AND status = 'started'
          AND updated_at <= clock_timestamp() - interval '#{@preparation_lease_seconds} seconds'
        """,
        identity_params(workspace_id, context) ++
          [
            context.request_fingerprint,
            next_generation,
            context.expires_at,
            previous_generation
          ]
      )

    if result.num_rows != 1 do
      Repo.rollback(Error.new(:conflict, "deployment-planning reservation was renewed"))
    end
  end

  defp validate_context!(workspace_id, context) do
    valid? =
      workspace_id != "" and byte_size(workspace_id) <= 255 and context.operation != "" and
        byte_size(context.operation) <= 128 and context.principal_kind in [:actor, :service] and
        context.principal_id != "" and byte_size(context.principal_id) <= 255 and
        byte_size(context.key_hash) in 16..64 and
        byte_size(context.request_fingerprint) in 16..64 and
        match?(%DateTime{}, context.expires_at) and future_in_database?(context.expires_at)

    unless valid?, do: Repo.rollback(Error.new(:invalid, "invalid command idempotency context"))
  end

  defp future_in_database?(expires_at) do
    case SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [expires_at]).rows do
      [[future?]] -> future?
    end
  end

  defp validate_encoded!(encoded) do
    response = Map.get(encoded, :response)
    response_status = Map.get(encoded, :response_status)
    resource_kind = Map.get(encoded, :resource_kind)
    resource_id = Map.get(encoded, :resource_id)

    valid? =
      is_map(response) and byte_size(Jason.encode!(response)) <= @max_response_bytes and
        (is_nil(response_status) or
           (is_integer(response_status) and response_status in 100..599)) and
        bounded_optional?(resource_kind, 64) and bounded_optional?(resource_id, 512)

    if valid? do
      %{
        response: response,
        response_status: response_status,
        resource_kind: resource_kind,
        resource_id: resource_id
      }
    else
      Repo.rollback(Error.new(:invalid, "idempotency replay result is invalid or too large"))
    end
  rescue
    _error -> Repo.rollback(Error.new(:invalid, "idempotency replay result is not JSON safe"))
  end

  defp bounded_optional?(nil, _maximum), do: true
  defp bounded_optional?(value, maximum), do: is_binary(value) and byte_size(value) <= maximum

  defp identity_params(workspace_id, context) do
    [
      workspace_id,
      context.operation,
      Atom.to_string(context.principal_kind),
      context.principal_id,
      context.key_hash
    ]
  end

  defp internal_error(stage, reason) do
    Error.new(:internal, "idempotency result #{stage} failed",
      details: %{reason: inspect(reason)}
    )
  end
end
