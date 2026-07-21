defmodule FavnStoragePostgres.Runs.Store do
  @moduledoc false

  @behaviour FavnOrchestrator.Persistence.RunStore

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Favn.RuntimeInput.Pin
  alias FavnOrchestrator.Persistence.CapacityIdentity
  alias FavnOrchestrator.Persistence.Commands.CommitRunTransition
  alias FavnOrchestrator.Persistence.Commands.CreateRun
  alias FavnOrchestrator.Persistence.Commands.PinRuntimeInputs
  alias FavnOrchestrator.Persistence.Commands.RequestRunCancellation
  alias FavnOrchestrator.Persistence.Commands.RunTarget, as: RunTargetCommand
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Persistence.Queries.GetRun
  alias FavnOrchestrator.Persistence.Queries.GetRuntimeInputs
  alias FavnOrchestrator.Persistence.Queries.PagePublishedRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRunEvents
  alias FavnOrchestrator.Persistence.Queries.PageRuns
  alias FavnOrchestrator.Persistence.Results.CursorPage
  alias FavnOrchestrator.Persistence.Results.RunCommitted
  alias FavnOrchestrator.Persistence.Results.RunSummary
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.RunCancellation
  alias FavnOrchestrator.RunState
  alias FavnOrchestrator.Storage.RunEventCodec
  alias FavnOrchestrator.Storage.RunSnapshotCodec
  alias FavnStoragePostgres.CanonicalJSON
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Idempotency.Transaction, as: IdempotencyTransaction
  alias FavnStoragePostgres.Outbox.Writer, as: OutboxWriter
  alias FavnStoragePostgres.Payload
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Runs.RuntimeInputPinCodec
  alias FavnStoragePostgres.Runs.Decoder
  alias FavnStoragePostgres.RuntimeInputKeys
  alias FavnStoragePostgres.Schemas.CapacityScope
  alias FavnStoragePostgres.Schemas.ManifestVersion
  alias FavnStoragePostgres.Schemas.OutboxEvent
  alias FavnStoragePostgres.Schemas.Run
  alias FavnStoragePostgres.Schemas.RunEvent
  alias FavnStoragePostgres.Schemas.RunOwnership
  alias FavnStoragePostgres.Schemas.RunPlan
  alias FavnStoragePostgres.Schemas.RunTarget
  alias FavnStoragePostgres.Schemas.RuntimeInputPin
  alias FavnStoragePostgres.Schemas.WorkspaceDeployment

  @max_targets 10_000
  @bulk_insert_size 500
  @max_snapshot_bytes 4 * 1_024 * 1_024
  @max_plan_bytes 64 * 1_024 * 1_024
  @max_event_bytes 512 * 1_024
  @max_event_types 32
  @max_runtime_input_pins 1_000
  @runtime_input_package_batch_size 500
  @max_cancel_reason_bytes 32 * 1_024
  @snapshot_version 2

  @impl true
  def create_run(%CreateRun{} = command) do
    with :ok <- validate_create(command),
         {:ok, encoded} <- encode_write(command.run, command.event, persist_plan?: true),
         {:ok, result} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.execute!(
               command.workspace_context.workspace_id,
               command.idempotency,
               fn -> create_or_replay!(command, encoded) end,
               &encode_idempotent_run_result/1,
               &decode_idempotent_run_result(&1, command)
             )
           end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def commit_transition(%CommitRunTransition{} = command) do
    with :ok <- validate_transition(command),
         {:ok, encoded} <- encode_write(command.run, command.event),
         {:ok, result} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.execute!(
               command.workspace_context.workspace_id,
               command.idempotency,
               fn -> commit_or_replay!(command, encoded) end,
               &encode_idempotent_run_result/1,
               &decode_idempotent_run_result(&1, command)
             )
           end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def request_cancellation(%RequestRunCancellation{} = command) do
    with :ok <- validate_cancellation(command),
         {:ok, result} <-
           Repo.transaction(fn ->
             IdempotencyTransaction.execute!(
               command.workspace_context.workspace_id,
               command.idempotency,
               fn -> request_cancellation!(command) end,
               &encode_idempotent_run_result/1,
               &decode_idempotent_cancellation_result(&1, command)
             )
           end) do
      {:ok, result}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_run(%GetRun{workspace_context: context, run_id: run_id}) do
    with :ok <- validate_workspace_read(context),
         true <- valid_identity?(run_id) do
      case Repo.get_by(Run, workspace_id: context.workspace_id, run_id: run_id) do
        nil -> {:error, Error.new(:not_found, "run not found")}
        %Run{} = row -> decode_run(row)
      end
    else
      false -> {:error, Error.new(:invalid, "invalid run identity")}
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_runs(%PageRuns{} = query) do
    with :ok <- validate_page_runs(query),
         ecto_query <- runs_query(query),
         rows <- Repo.all(ecto_query),
         page_rows <- Enum.take(rows, query.limit),
         {:ok, runs} <- Decoder.decode_many(page_rows),
         has_more? <- length(rows) > query.limit do
      {:ok,
       %CursorPage{
         items: runs,
         limit: query.limit,
         has_more?: has_more?,
         next_cursor: next_run_cursor(List.last(page_rows), query.scope, has_more?)
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_run_summaries(%PageRuns{} = query) do
    with :ok <- validate_page_runs(query),
         ecto_query <- run_summaries_query(query),
         rows <- Repo.all(ecto_query),
         page_rows <- Enum.take(rows, query.limit),
         runner_releases <- runner_releases(page_rows),
         runs <- Enum.map(page_rows, &run_summary(&1, runner_releases)),
         has_more? <- length(rows) > query.limit do
      {:ok,
       %CursorPage{
         items: runs,
         limit: query.limit,
         has_more?: has_more?,
         next_cursor: next_run_cursor(List.last(page_rows), query.scope, has_more?)
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def page_events(%PageRunEvents{} = query) do
    with :ok <- validate_page_events(query),
         rows <- Repo.all(events_query(query)),
         page_rows <- Enum.take(rows, query.limit),
         {:ok, events} <- decode_events(page_rows),
         has_more? <- length(rows) > query.limit do
      {:ok,
       %CursorPage{
         items: events,
         limit: query.limit,
         has_more?: has_more?,
         next_cursor: next_event_cursor(query, page_rows, has_more?)
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  def page_events(%PagePublishedRunEvents{} = query) do
    with :ok <- validate_published_events(query),
         :ok <- validate_publication_cursor(query),
         rows <- Repo.all(published_events_query(query)),
         page_rows <- Enum.take(rows, query.limit),
         {:ok, events} <- decode_events(page_rows),
         has_more? <- length(rows) > query.limit do
      {:ok,
       %CursorPage{
         items: events,
         limit: query.limit,
         has_more?: has_more?,
         next_cursor:
           if(has_more? and page_rows != [],
             do: %{publication_id: elem(List.last(page_rows), 1)}
           )
       }}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def pin_runtime_inputs(%PinRuntimeInputs{} = command) do
    with :ok <- validate_runtime_input_command(command),
         {:ok, {key_version, key}} <- RuntimeInputKeys.current(),
         {:ok, pins} <-
           Repo.transaction(fn -> persist_runtime_input_pins!(command, key_version, key) end) do
      {:ok, pins}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  @impl true
  def get_runtime_inputs(%GetRuntimeInputs{} = query) do
    with :ok <- validate_runtime_input_query(query),
         {:ok, _run} <- fetch_run(query.workspace_context, query.run_id),
         {:ok, hashes} <- requested_node_hashes(query.node_keys),
         {:ok, rows} <-
           runtime_input_rows(query.workspace_context.workspace_id, query.run_id, hashes),
         {:ok, pins} <- decode_runtime_input_rows(rows, stored_resolvers(rows)) do
      {:ok, pins}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, ErrorMapper.map(reason)}
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  defp persist_runtime_input_pins!(command, key_version, key) do
    with {:ok, run} <- fetch_run(command.workspace_context, command.run_id),
         {:ok, bindings} <-
           runtime_input_bindings(
             run,
             Enum.map(command.pins, & &1.node_key)
           ),
         :ok <- validate_pin_resolvers(command.pins, bindings),
         {:ok, candidates} <-
           encode_runtime_input_pins(command, key_version, key),
         candidates <-
           Enum.map(
             candidates,
             &Map.put(&1, :binding, Map.fetch!(bindings, pin_asset_ref(&1.pin)))
           ) do
      now = DateTime.utc_now()

      rows =
        Enum.map(candidates, fn candidate ->
          %{
            workspace_id: command.workspace_context.workspace_id,
            run_id: command.run_id,
            node_key_hash: candidate.node_key_hash,
            payload_fingerprint: candidate.payload_fingerprint,
            execution_package_hash: candidate.binding.package_hash,
            resolver_module: candidate.binding.resolver_module,
            encryption_key_version: key_version,
            payload: candidate.payload,
            inserted_at: now
          }
        end)

      {inserted_count, _rows} =
        Repo.insert_all(RuntimeInputPin, rows,
          on_conflict: :nothing,
          conflict_target: [:workspace_id, :run_id, :node_key_hash]
        )

      if inserted_count > 0 do
        SQL.query!(
          Repo,
          """
          INSERT INTO favn_control.runtime_input_key_versions (key_version, first_used_at)
          VALUES ($1, clock_timestamp())
          ON CONFLICT (key_version) DO NOTHING
          """,
          [key_version]
        )
      end

      hashes = Enum.map(candidates, & &1.node_key_hash)

      with {:ok, persisted_rows} <-
             runtime_input_rows(
               command.workspace_context.workspace_id,
               command.run_id,
               hashes
             ) do
        persisted = Map.new(persisted_rows, &{&1.node_key_hash, &1})
        verify_runtime_input_replay!(candidates, persisted, stored_resolvers(persisted_rows))
      end
    else
      {:error, %Error{} = error} -> Repo.rollback(error)
      {:error, reason} -> Repo.rollback(ErrorMapper.map(reason))
    end
  end

  defp encode_runtime_input_pins(command, key_version, key) do
    Enum.reduce_while(command.pins, {:ok, []}, fn pin, {:ok, acc} ->
      with {:ok, node_key_hash} <- RuntimeInputPinCodec.node_key_hash(pin.node_key),
           scope <- runtime_input_scope(command, node_key_hash, key_version),
           {:ok, encoded} <- RuntimeInputPinCodec.encode(pin, scope, key) do
        candidate =
          encoded
          |> Map.put(:node_key_hash, node_key_hash)
          |> Map.put(:pin, pin)

        {:cont, {:ok, [candidate | acc]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, candidates} -> {:ok, Enum.reverse(candidates)}
      error -> error
    end)
  end

  defp verify_runtime_input_replay!(candidates, persisted, allowed_resolvers) do
    Enum.reduce_while(candidates, {:ok, []}, fn candidate, {:ok, acc} ->
      case Map.fetch(persisted, candidate.node_key_hash) do
        {:ok, row}
        when row.execution_package_hash == candidate.binding.package_hash and
               row.resolver_module == candidate.binding.resolver_module ->
          case decode_runtime_input_row(row, allowed_resolvers) do
            {:ok, pin} ->
              if runtime_input_pin_equivalent?(candidate.pin, pin) do
                {:cont, {:ok, [pin | acc]}}
              else
                {:halt,
                 {:error,
                  Error.new(:conflict, "runtime input pin identity has different content")}}
              end

            {:error, error} ->
              {:halt, {:error, error}}
          end

        {:ok, _conflicting_row} ->
          {:halt,
           {:error, Error.new(:conflict, "runtime input pin identity has different content")}}

        :error ->
          {:halt, {:error, Error.new(:internal, "runtime input pin write did not persist")}}
      end
    end)
    |> case do
      {:ok, pins} -> Enum.reverse(pins)
      {:error, %Error{} = error} -> Repo.rollback(error)
      {:error, reason} -> Repo.rollback(ErrorMapper.map(reason))
    end
  end

  defp runtime_input_pin_equivalent?(%Pin{} = candidate, %Pin{} = persisted) do
    Pin.equivalent?(candidate, persisted) and
      candidate.run_id == persisted.run_id and
      candidate.node_key == persisted.node_key and
      candidate.params == persisted.params and
      candidate.metadata == persisted.metadata and
      candidate.sensitive_params == persisted.sensitive_params and
      candidate.source_run_id == persisted.source_run_id and
      candidate.source_node_key == persisted.source_node_key and
      candidate.source_payload_fingerprint == persisted.source_payload_fingerprint and
      candidate.schema_version == persisted.schema_version
  end

  defp runtime_input_rows(workspace_id, run_id, nil) do
    rows =
      RuntimeInputPin
      |> where([pin], pin.workspace_id == ^workspace_id and pin.run_id == ^run_id)
      |> order_by([pin], asc: pin.node_key_hash)
      |> limit(@max_runtime_input_pins + 1)
      |> Repo.all()

    if length(rows) <= @max_runtime_input_pins do
      {:ok, rows}
    else
      {:error,
       Error.new(:limit_exceeded, "runtime input pin result exceeds the bounded read limit")}
    end
  end

  defp runtime_input_rows(_workspace_id, _run_id, []), do: {:ok, []}

  defp runtime_input_rows(workspace_id, run_id, hashes) do
    RuntimeInputPin
    |> where(
      [pin],
      pin.workspace_id == ^workspace_id and pin.run_id == ^run_id and
        pin.node_key_hash in ^hashes
    )
    |> order_by([pin], asc: pin.node_key_hash)
    |> Repo.all()
    |> then(&{:ok, &1})
  end

  defp decode_runtime_input_rows(rows, allowed_resolvers) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      case decode_runtime_input_row(row, allowed_resolvers) do
        {:ok, pin} -> {:cont, {:ok, [pin | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> then(fn
      {:ok, pins} -> {:ok, Enum.reverse(pins)}
      error -> error
    end)
  end

  defp decode_runtime_input_row(row, allowed_resolvers) do
    scope = %{
      workspace_id: row.workspace_id,
      run_id: row.run_id,
      node_key_hash: row.node_key_hash,
      key_version: row.encryption_key_version
    }

    with {:ok, key} <- RuntimeInputKeys.fetch(row.encryption_key_version),
         {:ok, pin} <-
           RuntimeInputPinCodec.decode(row.payload, scope, key, allowed_resolvers) do
      {:ok, pin}
    else
      {:error, reason} ->
        {:error,
         Error.new(:internal, "runtime input pin could not be decrypted",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp fetch_run(context, run_id) do
    query =
      from(run in Run,
        where: run.workspace_id == ^context.workspace_id and run.run_id == ^run_id,
        select: %{
          workspace_id: run.workspace_id,
          run_id: run.run_id,
          manifest_version_id: run.manifest_version_id
        }
      )

    case Repo.one(query) do
      %{workspace_id: _, run_id: _, manifest_version_id: _} = run -> {:ok, run}
      nil -> {:error, Error.new(:not_found, "run not found")}
    end
  end

  defp validate_pin_resolvers(pins, bindings) do
    if Enum.all?(pins, fn pin ->
         case Map.get(bindings, pin_asset_ref(pin)) do
           %{resolver_module: resolver} -> resolver == Atom.to_string(pin.resolver)
           nil -> false
         end
       end) do
      :ok
    else
      {:error,
       Error.new(:invalid, "runtime input pin resolver is not declared by the pinned manifest")}
    end
  end

  defp runtime_input_bindings(
         %{workspace_id: workspace_id, run_id: run_id, manifest_version_id: manifest_version_id},
         node_keys
       ) do
    refs = Enum.map(node_keys, &node_asset_ref/1)

    if Enum.all?(refs, &match?({module, name} when is_atom(module) and is_atom(name), &1)) do
      refs = refs |> MapSet.new() |> Enum.sort()

      refs
      |> Enum.chunk_every(@runtime_input_package_batch_size)
      |> Enum.reduce_while({:ok, %{}}, fn batch, {:ok, bindings} ->
        case runtime_input_binding_batch(
               workspace_id,
               run_id,
               manifest_version_id,
               batch
             ) do
          {:ok, batch_bindings} -> {:cont, {:ok, Map.merge(bindings, batch_bindings)}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      {:error, Error.new(:invalid, "runtime input pin has an invalid node key")}
    end
  end

  defp runtime_input_binding_batch(workspace_id, run_id, manifest_version_id, refs) do
    modules = Enum.map(refs, &(&1 |> elem(0) |> Atom.to_string()))
    names = Enum.map(refs, &(&1 |> elem(1) |> Atom.to_string()))
    target_ids = Enum.map(refs, &FavnOrchestrator.Persistence.TargetIdentity.for_asset/1)

    %{rows: rows} =
      SQL.query!(
        Repo,
        """
        SELECT package_ref.asset_module,
               package_ref.asset_name,
               package_ref.package_hash,
               package.runtime_input_resolver
        FROM unnest($4::text[], $5::text[], $6::text[])
          AS requested(asset_module, asset_name, target_id)
        JOIN favn_control.manifest_execution_packages package_ref
          ON package_ref.manifest_version_id = $3
         AND package_ref.asset_module = requested.asset_module
         AND package_ref.asset_name = requested.asset_name
        JOIN favn_control.execution_packages package
          ON package.content_hash = package_ref.package_hash
        JOIN favn_control.run_targets target
          ON target.workspace_id = $1
         AND target.run_id = $2
         AND target.manifest_version_id = $3
         AND target.target_kind = 'asset'
         AND target.target_id = requested.target_id
        ORDER BY package_ref.asset_module, package_ref.asset_name
        """,
        [workspace_id, run_id, manifest_version_id, modules, names, target_ids]
      )

    if length(rows) <= length(refs) do
      rows =
        Enum.map(rows, fn [module, name, hash, resolver] -> {module, name, hash, resolver} end)

      refs_by_identity =
        Map.new(refs, fn {module, name} = ref ->
          {{Atom.to_string(module), Atom.to_string(name)}, ref}
        end)

      decode_runtime_input_bindings(rows, refs_by_identity)
    else
      {:error, Error.new(:internal, "runtime input package query exceeded requested assets")}
    end
  end

  defp decode_runtime_input_bindings(rows, refs_by_identity) do
    expected_count = map_size(refs_by_identity)

    rows
    |> Enum.reduce_while({:ok, %{}}, fn {module, name, hash, resolver}, {:ok, bindings} ->
      with {asset_module, asset_name} = asset_ref
           when is_atom(asset_module) and is_atom(asset_name) <-
             Map.get(refs_by_identity, {module, name}),
           true <- is_binary(resolver) and resolver != "" do
        {:cont,
         {:ok,
          Map.put(bindings, asset_ref, %{
            package_hash: hash,
            resolver_module: resolver
          })}}
      else
        _invalid ->
          {:halt, {:error, Error.new(:invalid, "runtime input package is invalid")}}
      end
    end)
    |> case do
      {:ok, bindings} ->
        if map_size(bindings) == expected_count do
          {:ok, bindings}
        else
          {:error,
           Error.new(:invalid, "runtime input pin asset is not declared by the pinned manifest")}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp stored_resolvers(rows), do: MapSet.new(rows, & &1.resolver_module)

  defp pin_asset_ref(pin), do: node_asset_ref(pin.node_key)

  defp node_asset_ref({{module, name}, _window}) when is_atom(module) and is_atom(name),
    do: {module, name}

  defp node_asset_ref(_node_key), do: nil

  defp requested_node_hashes(nil), do: {:ok, nil}

  defp requested_node_hashes(node_keys) when is_list(node_keys) do
    if length(node_keys) <= @max_runtime_input_pins do
      Enum.reduce_while(node_keys, {:ok, []}, fn node_key, {:ok, hashes} ->
        case RuntimeInputPinCodec.node_key_hash(node_key) do
          {:ok, hash} -> {:cont, {:ok, [hash | hashes]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> then(fn
        {:ok, hashes} -> {:ok, Enum.reverse(hashes)}
        error -> error
      end)
    else
      {:error, Error.new(:limit_exceeded, "runtime input pin query exceeds 1000 node keys")}
    end
  end

  defp requested_node_hashes(_node_keys),
    do: {:error, Error.new(:invalid, "runtime input pin node keys must be a list")}

  defp runtime_input_scope(command, node_key_hash, key_version) do
    %{
      workspace_id: command.workspace_context.workspace_id,
      run_id: command.run_id,
      node_key_hash: node_key_hash,
      key_version: key_version
    }
  end

  defp validate_runtime_input_command(%PinRuntimeInputs{} = command) do
    cond do
      not workspace_writer?(command.workspace_context) ->
        {:error, Error.new(:forbidden, "workspace write role required")}

      not valid_identity?(command.command_id) or not valid_identity?(command.run_id) ->
        {:error, Error.new(:invalid, "invalid runtime input pin command identity")}

      command.pins == [] or length(command.pins) > @max_runtime_input_pins ->
        {:error,
         Error.new(:limit_exceeded, "runtime input pin batch must contain 1 to 1000 pins")}

      not Enum.all?(command.pins, &valid_runtime_input_pin?(&1, command.run_id)) ->
        {:error, Error.new(:invalid, "runtime input pin batch is invalid")}

      duplicate_node_keys?(command.pins) ->
        {:error, Error.new(:invalid, "runtime input pin batch contains duplicate node keys")}

      true ->
        :ok
    end
  end

  defp validate_runtime_input_query(%GetRuntimeInputs{} = query) do
    cond do
      not workspace_reader?(query.workspace_context) ->
        {:error, Error.new(:forbidden, "workspace read role required")}

      not valid_identity?(query.run_id) ->
        {:error, Error.new(:invalid, "invalid run identity")}

      true ->
        :ok
    end
  end

  defp valid_runtime_input_pin?(%Favn.RuntimeInput.Pin{} = pin, run_id) do
    pin.run_id == run_id and is_tuple(pin.node_key) and is_atom(pin.resolver) and
      is_map(pin.params) and is_map(pin.metadata) and is_list(pin.sensitive_params) and
      valid_identity?(pin.input_identity) and valid_identity?(pin.payload_fingerprint) and
      pin.schema_version == 1 and match?(%DateTime{}, pin.inserted_at) and
      match?(%DateTime{}, pin.updated_at)
  end

  defp valid_runtime_input_pin?(_pin, _run_id), do: false

  defp duplicate_node_keys?(pins) do
    keys = Enum.map(pins, & &1.node_key)
    length(keys) != length(Enum.uniq(keys))
  end

  defp validate_create(%CreateRun{} = command) do
    run = command.run

    cond do
      not workspace_writer?(command.workspace_context) ->
        {:error, :invalid}

      not valid_identity?(command.command_id) or not valid_identity?(command.deployment_id) ->
        {:error, :invalid}

      not match?(%RunState{}, run) ->
        {:error, :invalid}

      run.workspace_id != command.workspace_context.workspace_id or
          run.deployment_id != command.deployment_id ->
        {:error, :invalid}

      run.event_seq != 1 ->
        {:error, :invalid}

      event_sequence(command.event) != run.event_seq ->
        {:error, :invalid}

      command.targets == [] or length(command.targets) > @max_targets ->
        {:error, :invalid}

      not valid_targets?(command.targets) ->
        {:error, :invalid}

      true ->
        :ok
    end
  end

  defp validate_transition(%CommitRunTransition{} = command) do
    cond do
      not workspace_writer?(command.workspace_context) ->
        {:error, :invalid}

      not valid_identity?(command.command_id) ->
        {:error, :invalid}

      not is_integer(command.expected_sequence) or command.expected_sequence < 1 ->
        {:error, :invalid}

      not match?(%RunState{}, command.run) ->
        {:error, :invalid}

      command.run.workspace_id != command.workspace_context.workspace_id ->
        {:error, :invalid}

      command.run.event_seq != command.expected_sequence + 1 ->
        {:error, :invalid}

      event_sequence(command.event) != command.run.event_seq ->
        {:error, :invalid}

      not valid_owner_fence?(command.owner_id, command.fencing_token) ->
        {:error, :invalid}

      true ->
        :ok
    end
  end

  defp valid_owner_fence?(nil, nil), do: true

  defp valid_owner_fence?(owner_id, fencing_token),
    do: valid_identity?(owner_id) and is_integer(fencing_token) and fencing_token > 0

  defp validate_cancellation(%RequestRunCancellation{} = command) do
    cond do
      not workspace_writer?(command.workspace_context) ->
        {:error, :invalid}

      not valid_identity?(command.command_id) or not valid_identity?(command.run_id) ->
        {:error, :invalid}

      not is_map(command.reason) or not is_struct(command.occurred_at, DateTime) ->
        {:error, :invalid}

      true ->
        Payload.validate(command.reason, @max_cancel_reason_bytes)
    end
  end

  defp validate_page_runs(%PageRuns{} = query) do
    cond do
      not valid_read_scope?(query.scope) ->
        {:error, Error.new(:invalid, "invalid run query scope")}

      not is_integer(query.limit) or query.limit < 1 or query.limit > 200 ->
        {:error, Error.new(:invalid, "run page limit must be between 1 and 200")}

      not is_nil(query.after) and not is_map(query.after) ->
        {:error, Error.new(:invalid, "invalid run cursor")}

      not is_nil(query.root_execution_group_id) and
          not valid_identity?(query.root_execution_group_id) ->
        {:error, Error.new(:invalid, "invalid execution group identity")}

      true ->
        :ok
    end
  end

  defp validate_page_events(%PageRunEvents{} = query) do
    if workspace_reader?(query.workspace_context) and exactly_one_event_identity?(query) and
         valid_event_page_limit?(query.limit) and valid_event_cursors?(query) and
         valid_event_cursor_combination?(query) and query.order in [nil, :asc, :desc] and
         valid_event_types?(query.event_types),
       do: :ok,
       else: {:error, :invalid}
  end

  defp valid_event_page_limit?(limit), do: is_integer(limit) and limit >= 1 and limit <= 200

  defp valid_event_cursors?(query) do
    valid_non_negative_cursor?(query.after_sequence) and
      valid_non_negative_cursor?(query.after_event_id) and
      valid_positive_cursor?(query.before_event_id)
  end

  defp valid_non_negative_cursor?(nil), do: true
  defp valid_non_negative_cursor?(value), do: is_integer(value) and value >= 0
  defp valid_positive_cursor?(nil), do: true
  defp valid_positive_cursor?(value), do: is_integer(value) and value >= 1

  defp valid_event_cursor_combination?(query) do
    (is_nil(query.after_event_id) or is_nil(query.before_event_id)) and
      (is_nil(query.run_id) or is_nil(query.after_event_id)) and
      (is_nil(query.run_id) or is_nil(query.before_event_id)) and
      (is_nil(query.root_execution_group_id) or is_nil(query.after_sequence))
  end

  defp valid_event_types?(nil), do: true

  defp valid_event_types?(event_types),
    do: is_list(event_types) and length(event_types) <= @max_event_types

  defp validate_published_events(%PagePublishedRunEvents{} = query) do
    cond do
      not valid_read_scope?(query.scope) ->
        {:error, Error.new(:invalid, "invalid published event scope")}

      not is_integer(query.limit) or query.limit < 1 or query.limit > 200 ->
        {:error, Error.new(:invalid, "published event page limit must be between 1 and 200")}

      not is_nil(query.after_publication_id) and
          (not is_integer(query.after_publication_id) or query.after_publication_id < 0) ->
        {:error, Error.new(:invalid, "invalid publication cursor")}

      is_list(query.event_types) and length(query.event_types) > @max_event_types ->
        {:error, Error.new(:invalid, "too many published event filters")}

      not is_nil(query.event_types) and not is_list(query.event_types) ->
        {:error, Error.new(:invalid, "invalid published event filters")}

      true ->
        :ok
    end
  end

  defp validate_publication_cursor(%PagePublishedRunEvents{after_publication_id: nil}), do: :ok

  defp validate_publication_cursor(%PagePublishedRunEvents{after_publication_id: cursor}) do
    %{rows: [[last_publication_id]]} =
      SQL.query!(
        Repo,
        """
        SELECT last_publication_id
        FROM favn_control.outbox_publication_state
        WHERE singleton_id = 1
        """,
        []
      )

    if cursor <= last_publication_id,
      do: :ok,
      else: {:error, Error.new(:invalid, "publication cursor is ahead of durable state")}
  end

  defp create_or_replay!(command, encoded) do
    workspace_id = command.workspace_context.workspace_id
    lock_run_identity!(workspace_id, command.run.id)

    case lock_run(workspace_id, command.run.id) do
      %Run{} = existing ->
        replay_create!(existing, command, encoded)

      nil ->
        deployment =
          Repo.get_by(WorkspaceDeployment,
            workspace_id: workspace_id,
            deployment_id: command.deployment_id,
            manifest_version_id: command.run.manifest_version_id
          ) || Repo.rollback(Error.new(:constraint, "run deployment is not available"))

        manifest = Repo.get!(ManifestVersion, deployment.manifest_version_id)
        manifest_content_hash = Base.encode16(manifest.content_hash, case: :lower)

        cond do
          command.run.manifest_content_hash != manifest_content_hash ->
            Repo.rollback(
              Error.new(:constraint, "run manifest content does not match its deployment",
                details: %{reason: :run_manifest_content_hash_mismatch}
              )
            )

          not is_binary(command.run.required_runner_release_id) or
              command.run.required_runner_release_id != manifest.required_runner_release_id ->
            Repo.rollback(
              Error.new(:constraint, "run runner release identity does not match its deployment",
                details: %{reason: :run_manifest_runner_release_mismatch}
              )
            )

          true ->
            :ok
        end

        _deployment = deployment
        SQL.query!(Repo, "SET CONSTRAINTS ALL DEFERRED", [])
        event_id = next_event_id!()

        outbox =
          write_run_outbox!(command.command_id, workspace_id, command.run, encoded.event, nil)

        run_row = run_row(command, encoded, event_id, creation_hash!(command, encoded))
        Repo.insert!(run_row)
        insert_run_plan!(command.run, encoded)
        Repo.insert!(event_row(workspace_id, event_id, outbox.outbox_event_id, encoded))
        insert_targets!(command, event_id)

        Repo.insert!(%RunOwnership{
          workspace_id: workspace_id,
          run_id: command.run.id,
          fencing_token: 0,
          updated_at: encoded.occurred_at
        })

        maybe_insert_run_capacity_scope!(command, encoded.occurred_at)

        committed(command.run, encoded.event, event_id, outbox.outbox_event_id, false)
    end
  end

  defp maybe_insert_run_capacity_scope!(command, occurred_at) do
    case pipeline_max_concurrency(command.run) do
      limit when is_integer(limit) and limit > 0 ->
        Repo.insert!(%CapacityScope{
          scope_id:
            CapacityIdentity.scope_id(
              command.workspace_context.workspace_id,
              :run,
              command.run.id
            ),
          workspace_id: command.workspace_context.workspace_id,
          scope_kind: "run",
          scope_key: command.run.id,
          capacity_limit: limit,
          active_count: 0,
          version: 1,
          inserted_at: occurred_at,
          updated_at: occurred_at
        })

      _unlimited ->
        :ok
    end
  end

  defp pipeline_max_concurrency(%RunState{metadata: metadata}) when is_map(metadata) do
    policy =
      Map.get(metadata, :pipeline_execution_policy) ||
        Map.get(metadata, "pipeline_execution_policy")

    if is_map(policy),
      do: Map.get(policy, :max_concurrency) || Map.get(policy, "max_concurrency")
  end

  defp lock_run_identity!(workspace_id, run_id) do
    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text, $2::text)::text, 0))",
      [workspace_id, run_id]
    )

    :ok
  end

  defp commit_or_replay!(command, encoded) do
    workspace_id = command.workspace_context.workspace_id

    case lock_run(workspace_id, command.run.id) do
      nil ->
        Repo.rollback(Error.new(:not_found, "run not found"))

      %Run{event_sequence: sequence} = existing when sequence == command.run.event_seq ->
        exact_replay!(existing, encoded)

      %Run{event_sequence: sequence} when sequence != command.expected_sequence ->
        Repo.rollback(
          Error.new(:conflict, "run sequence changed", details: %{actual_sequence: sequence})
        )

      %Run{} = existing ->
        ensure_same_run_identity!(existing, command.run)
        validate_fence!(command)

        event_id = next_event_id!()

        outbox =
          write_run_outbox!(
            command.command_id,
            workspace_id,
            command.run,
            encoded.event,
            existing.status
          )

        Repo.insert!(event_row(workspace_id, event_id, outbox.outbox_event_id, encoded))

        {updated, _rows} =
          from(run in Run,
            where:
              run.workspace_id == ^workspace_id and run.run_id == ^command.run.id and
                run.event_sequence == ^command.expected_sequence
          )
          |> Repo.update_all(
            set: [
              status: Atom.to_string(command.run.status),
              event_sequence: command.run.event_seq,
              latest_event_id: event_id,
              snapshot_version: @snapshot_version,
              snapshot_hash: encoded.snapshot_hash,
              snapshot: encoded.snapshot,
              updated_at: command.run.updated_at || encoded.occurred_at,
              terminal_at: terminal_at(command.run, encoded.occurred_at)
            ]
          )

        if updated != 1, do: Repo.rollback(Error.new(:conflict, "run sequence changed"))
        committed(command.run, encoded.event, event_id, outbox.outbox_event_id, false)
    end
  end

  defp request_cancellation!(%RequestRunCancellation{} = command) do
    workspace_id = command.workspace_context.workspace_id

    case lock_run(workspace_id, command.run_id) do
      nil ->
        Repo.rollback(Error.new(:not_found, "run not found"))

      %Run{} = row ->
        with {:ok, run} <- decode_run(row),
             {:ok, requested, event} <-
               RunCancellation.request(run, command.reason, command.occurred_at),
             {:ok, encoded} <- encode_write(requested, event) do
          commit_or_replay!(
            %CommitRunTransition{
              workspace_context: command.workspace_context,
              command_id: command.command_id,
              expected_sequence: run.event_seq,
              run: requested,
              event: event,
              idempotency: nil
            },
            encoded
          )
        else
          {:error, :run_already_terminal} ->
            Repo.rollback(
              Error.new(:conflict, "run is already terminal",
                details: %{reason: :run_already_terminal}
              )
            )

          {:error, :backfill_parent_cancel_not_supported} ->
            Repo.rollback(
              Error.new(:conflict, "backfill parent cancellation is not supported",
                details: %{reason: :backfill_parent_cancel_not_supported}
              )
            )

          {:error, %Error{} = error} ->
            Repo.rollback(error)

          {:error, reason} ->
            Repo.rollback(ErrorMapper.map(reason))
        end
    end
  end

  defp exact_replay!(%Run{} = existing, encoded) do
    event =
      Repo.get_by!(RunEvent,
        workspace_id: existing.workspace_id,
        run_id: existing.run_id,
        sequence: encoded.event.sequence
      )

    if existing.snapshot_hash == encoded.snapshot_hash and event.event_hash == encoded.event_hash do
      {:ok, run} = decode_run(existing)
      {:ok, decoded_event} = decode_event(event, nil)
      committed(run, decoded_event, event.event_id, event.outbox_event_id, true)
    else
      Repo.rollback(Error.new(:conflict, "run write identity has different canonical content"))
    end
  end

  defp replay_create!(%Run{} = existing, command, encoded) do
    event =
      Repo.get_by!(RunEvent,
        workspace_id: existing.workspace_id,
        run_id: existing.run_id,
        sequence: 1
      )

    outbox = Repo.get!(OutboxEvent, event.outbox_event_id)

    if existing.creation_hash == creation_hash!(command, encoded) and
         event.event_hash == encoded.event_hash and outbox.command_id == command.command_id do
      {:ok, run} = decode_run(existing)
      {:ok, decoded_event} = decode_event(event, outbox.publication_id)
      committed(run, decoded_event, event.event_id, event.outbox_event_id, true)
    else
      Repo.rollback(Error.new(:conflict, "run creation identity has different canonical content"))
    end
  end

  defp validate_fence!(%CommitRunTransition{owner_id: nil}), do: :ok

  defp validate_fence!(%CommitRunTransition{} = command) do
    workspace_id = command.workspace_context.workspace_id

    query =
      from(ownership in RunOwnership,
        where: ownership.workspace_id == ^workspace_id and ownership.run_id == ^command.run.id,
        lock: "FOR UPDATE"
      )

    case Repo.one(query) do
      %RunOwnership{
        owner_id: owner_id,
        fencing_token: token,
        released_at: nil,
        expires_at: %DateTime{} = expires_at
      }
      when owner_id == command.owner_id and token == command.fencing_token ->
        case SQL.query!(Repo, "SELECT $1::timestamptz > clock_timestamp()", [expires_at]) do
          %{rows: [[true]]} -> :ok
          _result -> Repo.rollback(Error.new(:fenced, "run ownership lease has expired"))
        end

      _ownership ->
        Repo.rollback(Error.new(:fenced, "run ownership fencing token is stale"))
    end
  end

  defp encode_write(%RunState{} = run, event, opts \\ []) do
    with {:ok, snapshot_json} <- RunSnapshotCodec.encode_run(run, plan: :reference),
         :ok <- Payload.validate_encoded(snapshot_json, @max_snapshot_bytes),
         {:ok, snapshot} <- Jason.decode(snapshot_json),
         {:ok, plan} <- encode_plan(run, Keyword.get(opts, :persist_plan?, false)),
         {:ok, normalized_event} <- RunEventCodec.normalize(run.id, event),
         {:ok, event_json} <- RunEventCodec.encode(normalized_event),
         :ok <- Payload.validate_encoded(event_json, @max_event_bytes),
         {:ok, event_payload} <- Jason.decode(event_json),
         {:ok, snapshot_hash} <- CanonicalJSON.hash(snapshot),
         {:ok, event_hash} <- CanonicalJSON.hash(event_payload) do
      {:ok,
       %{
         snapshot: snapshot,
         snapshot_hash: snapshot_hash,
         plan: plan,
         event: normalized_event,
         event_payload: event_payload,
         event_hash: event_hash,
         occurred_at: normalized_event.occurred_at
       }}
    end
  end

  defp encode_plan(%RunState{plan: nil}, _persist?), do: {:ok, nil}
  defp encode_plan(%RunState{}, false), do: {:ok, nil}

  defp encode_plan(%RunState{} = run, true) do
    with {:ok, plan_json} <- RunSnapshotCodec.encode_plan(run.plan),
         :ok <- Payload.validate_encoded(plan_json, @max_plan_bytes),
         {:ok, plan} <- Jason.decode(plan_json) do
      {:ok, plan}
    end
  end

  defp run_row(command, encoded, event_id, creation_hash) do
    run = command.run

    %Run{
      workspace_id: command.workspace_context.workspace_id,
      run_id: run.id,
      deployment_id: command.deployment_id,
      manifest_version_id: run.manifest_version_id,
      root_execution_group_id: run.root_run_id || run.id,
      parent_run_id: run.parent_run_id,
      rerun_of_run_id: run.rerun_of_run_id,
      submit_kind: Atom.to_string(run.submit_kind),
      trigger_type: trigger_type(run.trigger),
      status: Atom.to_string(run.status),
      event_sequence: run.event_seq,
      submitted_event_id: event_id,
      latest_event_id: event_id,
      snapshot_version: @snapshot_version,
      creation_hash: creation_hash,
      snapshot_hash: encoded.snapshot_hash,
      snapshot: encoded.snapshot,
      inserted_at: run.inserted_at || encoded.occurred_at,
      updated_at: run.updated_at || encoded.occurred_at,
      terminal_at: terminal_at(run, encoded.occurred_at)
    }
  end

  defp insert_run_plan!(%RunState{plan: nil}, _encoded), do: :ok

  defp insert_run_plan!(%RunState{} = run, %{plan: plan}) when is_map(plan) do
    plan_hash = run.plan_hash || RunState.plan_hash(run.plan)

    Repo.insert!(%RunPlan{
      workspace_id: run.workspace_id,
      run_id: run.id,
      manifest_version_id: run.manifest_version_id,
      plan_version: 1,
      plan_hash: Base.decode16!(plan_hash, case: :lower),
      plan: plan,
      inserted_at: run.inserted_at || DateTime.utc_now()
    })

    :ok
  end

  defp event_row(workspace_id, event_id, outbox_event_id, encoded) do
    event = encoded.event

    %RunEvent{
      event_id: event_id,
      workspace_id: workspace_id,
      run_id: event.run_id,
      sequence: event.sequence,
      event_type: to_string(event.event_type),
      entity_type: to_string(event.entity),
      asset_step_id: event.data[:asset_step_id] || event.data["asset_step_id"],
      status: stringify(event.status),
      stage: event.stage,
      occurred_at: event.occurred_at,
      payload_version: 1,
      event: encoded.event_payload,
      event_hash: encoded.event_hash,
      outbox_event_id: outbox_event_id,
      inserted_at: encoded.occurred_at
    }
  end

  defp write_run_outbox!(command_id, workspace_id, run, event, previous_status) do
    OutboxWriter.insert!(%{
      workspace_id: workspace_id,
      command_id: command_id,
      event_kind: "run." <> to_string(event.event_type),
      aggregate_kind: "run",
      aggregate_id: run.id,
      aggregate_version: run.event_seq,
      occurred_at: event.occurred_at,
      payload: %{
        "run_id" => run.id,
        "sequence" => run.event_seq,
        "event_type" => to_string(event.event_type),
        "status" => Atom.to_string(run.status),
        "previous_status" => previous_status
      }
    })
  end

  defp insert_targets!(command, event_id) do
    run = command.run
    workspace_id = command.workspace_context.workspace_id

    rows =
      Enum.map(command.targets, fn target ->
        %{
          workspace_id: workspace_id,
          run_id: run.id,
          deployment_id: command.deployment_id,
          manifest_version_id: run.manifest_version_id,
          target_kind: Atom.to_string(target.target_kind),
          target_id: target.target_id,
          target_module: target.target_module,
          target_name: target.target_name,
          is_primary: target.is_primary,
          submitted_event_id: event_id,
          inserted_at: run.inserted_at || DateTime.utc_now()
        }
      end)

    count =
      rows
      |> Enum.chunk_every(@bulk_insert_size)
      |> Enum.reduce(0, fn chunk, inserted ->
        {count, _rows} = Repo.insert_all(RunTarget, chunk)
        inserted + count
      end)

    if count != length(rows),
      do: Repo.rollback(Error.new(:internal, "run target insert was incomplete"))
  end

  defp creation_hash!(command, encoded) do
    {:ok, hash} =
      CanonicalJSON.hash(%{
        "deployment_id" => command.deployment_id,
        "snapshot" => encoded.snapshot,
        "event" => encoded.event_payload,
        "targets" => command.targets
      })

    hash
  end

  defp ensure_same_run_identity!(existing, run) do
    cond do
      existing.manifest_version_id != run.manifest_version_id ->
        Repo.rollback(Error.new(:conflict, "run manifest identity cannot change"))

      existing.deployment_id != run.deployment_id ->
        Repo.rollback(Error.new(:conflict, "run deployment identity cannot change"))

      Map.get(existing.snapshot, "manifest_content_hash") != run.manifest_content_hash ->
        Repo.rollback(Error.new(:conflict, "run manifest content identity cannot change"))

      Map.get(existing.snapshot, "required_runner_release_id") !=
          run.required_runner_release_id ->
        Repo.rollback(Error.new(:conflict, "run runner release identity cannot change"))

      true ->
        :ok
    end
  end

  defp lock_run(workspace_id, run_id) do
    from(run in Run,
      where: run.workspace_id == ^workspace_id and run.run_id == ^run_id,
      lock: "FOR UPDATE"
    )
    |> Repo.one()
  end

  defp next_event_id! do
    %{rows: [[event_id]]} =
      SQL.query!(
        Repo,
        "SELECT nextval(pg_get_serial_sequence('favn_control.run_events', 'event_id'))",
        []
      )

    event_id
  end

  defp runs_query(%PageRuns{} = query) do
    Run
    |> scope_runs(query.scope)
    |> filter_runs(query)
    |> cursor_runs(query.after, query.scope)
    |> order_runs(query.scope)
    |> limit(^(query.limit + 1))
  end

  defp run_summaries_query(%PageRuns{} = query) do
    query
    |> runs_query()
    |> select([run], %{
      workspace_id: run.workspace_id,
      run_id: run.run_id,
      deployment_id: run.deployment_id,
      manifest_version_id: run.manifest_version_id,
      submit_kind: run.submit_kind,
      trigger_type: run.trigger_type,
      status: run.status,
      event_sequence: run.event_sequence,
      submitted_event_id: run.submitted_event_id,
      latest_event_id: run.latest_event_id,
      inserted_at: run.inserted_at,
      updated_at: run.updated_at,
      terminal_at: run.terminal_at,
      parent_run_id: run.parent_run_id,
      rerun_of_run_id: run.rerun_of_run_id,
      root_execution_group_id: run.root_execution_group_id
    })
  end

  defp scope_runs(query, %WorkspaceContext{workspace_id: workspace_id}),
    do: where(query, [run], run.workspace_id == ^workspace_id)

  defp scope_runs(query, %PlatformContext{}), do: query

  defp filter_runs(query, %PageRuns{} = page) do
    query
    |> then(fn query ->
      if is_binary(page.manifest_version_id),
        do: where(query, [run], run.manifest_version_id == ^page.manifest_version_id),
        else: query
    end)
    |> then(fn query ->
      if is_binary(page.root_execution_group_id),
        do:
          where(
            query,
            [run],
            run.root_execution_group_id == ^page.root_execution_group_id
          ),
        else: query
    end)
    |> then(fn query ->
      if is_atom(page.status) and not is_nil(page.status),
        do: where(query, [run], run.status == ^Atom.to_string(page.status)),
        else: query
    end)
  end

  defp cursor_runs(query, nil, _scope), do: query

  defp cursor_runs(query, %{latest_event_id: event_id, run_id: run_id}, %WorkspaceContext{})
       when is_integer(event_id) and is_binary(run_id) do
    where(
      query,
      [run],
      run.latest_event_id < ^event_id or
        (run.latest_event_id == ^event_id and run.run_id < ^run_id)
    )
  end

  defp cursor_runs(
         query,
         %{latest_event_id: event_id, workspace_id: workspace_id, run_id: run_id},
         %PlatformContext{}
       )
       when is_integer(event_id) and is_binary(workspace_id) and is_binary(run_id) do
    where(
      query,
      [run],
      run.latest_event_id < ^event_id or
        (run.latest_event_id == ^event_id and run.workspace_id > ^workspace_id) or
        (run.latest_event_id == ^event_id and run.workspace_id == ^workspace_id and
           run.run_id < ^run_id)
    )
  end

  defp cursor_runs(_query, _cursor, _scope),
    do: raise(ArgumentError, "invalid run cursor shape")

  defp order_runs(query, %WorkspaceContext{}),
    do: order_by(query, [run], desc: run.latest_event_id, desc: run.run_id)

  defp order_runs(query, %PlatformContext{}),
    do: order_by(query, [run], desc: run.latest_event_id, asc: run.workspace_id, desc: run.run_id)

  defp events_query(%PageRunEvents{run_id: run_id} = page) when is_binary(run_id) do
    RunEvent
    |> join(:left, [event], outbox in OutboxEvent,
      on: outbox.outbox_event_id == event.outbox_event_id
    )
    |> where(
      [event, _outbox],
      event.workspace_id == ^page.workspace_context.workspace_id and event.run_id == ^page.run_id
    )
    |> then(fn query ->
      if is_integer(page.after_sequence),
        do: where(query, [event, _outbox], event.sequence > ^page.after_sequence),
        else: query
    end)
    |> then(fn query ->
      if is_list(page.event_types) and page.event_types != [] do
        types = Enum.map(page.event_types, &to_string/1)
        where(query, [event, _outbox], event.event_type in ^types)
      else
        query
      end
    end)
    |> order_by([event, _outbox], asc: event.sequence)
    |> limit(^(page.limit + 1))
    |> select([event, outbox], {event, outbox.publication_id})
  end

  defp events_query(%PageRunEvents{root_execution_group_id: root_run_id} = page)
       when is_binary(root_run_id) do
    RunEvent
    |> join(:inner, [event], run in Run,
      on: run.workspace_id == event.workspace_id and run.run_id == event.run_id
    )
    |> where(
      [event, run],
      event.workspace_id == ^page.workspace_context.workspace_id and
        run.root_execution_group_id == ^root_run_id
    )
    |> then(fn query ->
      if is_integer(page.after_event_id),
        do: where(query, [event, _run], event.event_id > ^page.after_event_id),
        else: query
    end)
    |> then(fn query ->
      if is_integer(page.before_event_id),
        do: where(query, [event, _run], event.event_id < ^page.before_event_id),
        else: query
    end)
    |> then(fn query ->
      if is_list(page.event_types) and page.event_types != [] do
        types = Enum.map(page.event_types, &to_string/1)
        where(query, [event, _run], event.event_type in ^types)
      else
        query
      end
    end)
    |> group_event_order(page.order)
    |> limit(^(page.limit + 1))
    |> select([event, _run], {event, event.event_id})
  end

  defp published_events_query(%PagePublishedRunEvents{} = page) do
    RunEvent
    |> join(:inner, [event], outbox in OutboxEvent,
      on: outbox.outbox_event_id == event.outbox_event_id
    )
    |> published_event_scope(page.scope)
    |> where([_event, outbox], not is_nil(outbox.publication_id))
    |> then(fn query ->
      if is_integer(page.after_publication_id),
        do: where(query, [_event, outbox], outbox.publication_id > ^page.after_publication_id),
        else: query
    end)
    |> then(fn query ->
      if is_list(page.event_types) and page.event_types != [] do
        types = Enum.map(page.event_types, &to_string/1)
        where(query, [event, _outbox], event.event_type in ^types)
      else
        query
      end
    end)
    |> order_by([_event, outbox], asc: outbox.publication_id)
    |> limit(^(page.limit + 1))
    |> select([event, outbox], {event, outbox.publication_id})
  end

  defp published_event_scope(query, %WorkspaceContext{workspace_id: workspace_id}),
    do: where(query, [event, _outbox], event.workspace_id == ^workspace_id)

  defp published_event_scope(query, %PlatformContext{}), do: query

  defp run_summary(row, runner_releases) do
    %RunSummary{
      workspace_id: row.workspace_id,
      run_id: row.run_id,
      manifest_version_id: row.manifest_version_id,
      required_runner_release_id: Map.get(runner_releases, row.manifest_version_id),
      submit_kind: String.to_existing_atom(row.submit_kind),
      status: String.to_existing_atom(row.status),
      event_sequence: row.event_sequence,
      inserted_at: row.inserted_at,
      updated_at: row.updated_at,
      terminal_at: row.terminal_at,
      parent_run_id: row.parent_run_id,
      rerun_of_run_id: row.rerun_of_run_id,
      root_run_id: row.root_execution_group_id,
      deployment_id: row.deployment_id,
      trigger_type: String.to_existing_atom(row.trigger_type),
      submitted_event_id: row.submitted_event_id,
      latest_event_id: row.latest_event_id
    }
  end

  defp runner_releases(rows) do
    manifest_version_ids = rows |> Enum.map(& &1.manifest_version_id) |> Enum.uniq()

    from(manifest in ManifestVersion,
      where: manifest.manifest_version_id in ^manifest_version_ids,
      select: {manifest.manifest_version_id, manifest.required_runner_release_id}
    )
    |> Repo.all()
    |> Map.new()
  end

  defp decode_run(%Run{} = row), do: Decoder.decode(row)

  defp decode_events(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {row, publication_id}, {:ok, acc} ->
      case decode_event(row, publication_id) do
        {:ok, event} -> {:cont, {:ok, [event | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> then(fn
      {:ok, events} -> {:ok, Enum.reverse(events)}
      error -> error
    end)
  end

  defp decode_event(%RunEvent{} = row, publication_id) do
    case RunEventCodec.decode(Jason.encode!(row.event)) do
      {:ok, event} ->
        {:ok, Map.put(event, :global_sequence, publication_id)}

      {:error, reason} ->
        {:error,
         Error.new(:internal, "persisted run event is invalid",
           details: %{reason: inspect(reason)}
         )}
    end
  end

  defp next_event_cursor(_query, _rows, false), do: nil
  defp next_event_cursor(_query, [], _has_more?), do: nil

  defp next_event_cursor(%PageRunEvents{run_id: run_id}, rows, true)
       when is_binary(run_id),
       do: %{sequence: elem(List.last(rows), 0).sequence}

  defp next_event_cursor(%PageRunEvents{root_execution_group_id: root_run_id}, rows, true)
       when is_binary(root_run_id),
       do: %{event_id: elem(List.last(rows), 0).event_id}

  defp group_event_order(query, :desc), do: order_by(query, [event, _run], desc: event.event_id)
  defp group_event_order(query, _order), do: order_by(query, [event, _run], asc: event.event_id)

  defp exactly_one_event_identity?(%PageRunEvents{} = query) do
    run? = valid_identity?(query.run_id)
    group? = valid_identity?(query.root_execution_group_id)
    run? != group?
  end

  defp next_run_cursor(_row, _scope, false), do: nil
  defp next_run_cursor(nil, _scope, _has_more), do: nil

  defp next_run_cursor(row, %WorkspaceContext{}, true),
    do: %{latest_event_id: row.latest_event_id, run_id: row.run_id}

  defp next_run_cursor(row, %PlatformContext{}, true),
    do: %{
      latest_event_id: row.latest_event_id,
      workspace_id: row.workspace_id,
      run_id: row.run_id
    }

  defp committed(run, event, event_id, outbox_event_id, replayed?) do
    %RunCommitted{
      run: run,
      event: event,
      event_id: event_id,
      outbox_event_id: outbox_event_id,
      replayed?: replayed?
    }
  end

  defp encode_idempotent_run_result(%RunCommitted{} = result) do
    {:ok,
     %{
       response: %{
         "run_id" => result.run.id,
         "sequence" => result.event.sequence,
         "event_id" => result.event_id,
         "outbox_event_id" => result.outbox_event_id
       },
       response_status: 200,
       resource_kind: "run",
       resource_id: result.run.id
     }}
  end

  defp decode_idempotent_run_result(
         %{response: response, resource_kind: "run", resource_id: run_id},
         command
       )
       when is_map(response) do
    expected_run = command.run
    sequence = Map.get(response, "sequence")
    event_id = Map.get(response, "event_id")
    outbox_event_id = Map.get(response, "outbox_event_id")

    event =
      Repo.get_by(RunEvent,
        workspace_id: command.workspace_context.workspace_id,
        run_id: run_id,
        sequence: sequence
      )

    with true <- run_id == expected_run.id,
         %RunEvent{event_id: ^event_id, outbox_event_id: ^outbox_event_id} = event <- event,
         {:ok, decoded_event} <- decode_event(event, nil) do
      {:ok, committed(expected_run, decoded_event, event_id, outbox_event_id, true)}
    else
      {:error, %Error{} = error} -> {:error, error}
      _other -> {:error, Error.new(:internal, "idempotent run replay record is inconsistent")}
    end
  end

  defp decode_idempotent_run_result(_encoded, _command),
    do: {:error, Error.new(:internal, "idempotent run replay record is invalid")}

  defp decode_idempotent_cancellation_result(
         %{response: response, resource_kind: "run", resource_id: run_id},
         command
       )
       when is_map(response) do
    sequence = Map.get(response, "sequence")
    event_id = Map.get(response, "event_id")
    outbox_event_id = Map.get(response, "outbox_event_id")
    workspace_id = command.workspace_context.workspace_id

    event =
      Repo.get_by(RunEvent,
        workspace_id: workspace_id,
        run_id: run_id,
        sequence: sequence
      )

    run = Repo.get_by(Run, workspace_id: workspace_id, run_id: run_id)

    with true <- run_id == command.run_id,
         %RunEvent{event_id: ^event_id, outbox_event_id: ^outbox_event_id} = event <- event,
         %Run{} = run <- run,
         {:ok, decoded_run} <- decode_run(run),
         {:ok, decoded_event} <- decode_event(event, nil) do
      {:ok, committed(decoded_run, decoded_event, event_id, outbox_event_id, true)}
    else
      {:error, %Error{} = error} -> {:error, error}
      _other -> {:error, Error.new(:internal, "idempotent cancellation replay is inconsistent")}
    end
  end

  defp decode_idempotent_cancellation_result(_encoded, _command),
    do: {:error, Error.new(:internal, "idempotent cancellation replay is invalid")}

  defp valid_targets?(targets) do
    Enum.all?(targets, fn
      %RunTargetCommand{} = target ->
        target.target_kind in [:asset, :pipeline] and valid_identity?(target.target_id) and
          valid_identity?(target.target_module) and
          (is_nil(target.target_name) or valid_identity?(target.target_name)) and
          is_boolean(target.is_primary)

      _target ->
        false
    end) and
      targets
      |> Enum.map(&{&1.target_kind, &1.target_id})
      |> then(&(length(&1) == length(Enum.uniq(&1))))
  end

  defp workspace_writer?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(roles, &(&1 in [:customer_operator, :workspace_admin, :platform_operator]))

  defp workspace_writer?(_context), do: false

  defp valid_read_scope?(%WorkspaceContext{} = context), do: workspace_reader?(context)

  defp valid_read_scope?(%PlatformContext{roles: roles} = context),
    do:
      PlatformContext.valid?(context) and
        Enum.any?(roles, &(&1 in [:platform_reader, :platform_operator, :platform_admin]))

  defp valid_read_scope?(_scope), do: false

  defp validate_workspace_read(context) do
    if workspace_reader?(context),
      do: :ok,
      else: {:error, Error.new(:forbidden, "workspace read role required")}
  end

  defp workspace_reader?(%WorkspaceContext{roles: roles} = context),
    do:
      WorkspaceContext.valid?(context) and
        Enum.any?(
          roles,
          &(&1 in [:customer_reader, :customer_operator, :workspace_admin, :platform_operator])
        )

  defp workspace_reader?(_context), do: false

  defp valid_identity?(value), do: is_binary(value) and value != "" and byte_size(value) <= 255

  defp event_sequence(event) when is_map(event),
    do: Map.get(event, :sequence) || Map.get(event, "sequence")

  defp event_sequence(_event), do: nil

  defp trigger_type(trigger) when is_map(trigger) do
    trigger
    |> Map.get(:kind, Map.get(trigger, "kind", :manual))
    |> stringify()
  end

  defp trigger_type(_trigger), do: "manual"

  defp terminal_at(%RunState{status: status}, occurred_at)
       when status in [:ok, :partial, :error, :cancelled, :timed_out],
       do: occurred_at

  defp terminal_at(_run, _occurred_at), do: nil

  defp stringify(nil), do: nil
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value) when is_binary(value), do: value
end
