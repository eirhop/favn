defmodule FavnStoragePostgres.Maintenance.Retention do
  @moduledoc false
  import Ecto.Query
  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.PlatformContext
  alias FavnOrchestrator.Retention.Policy
  alias FavnStoragePostgres.ErrorMapper
  alias FavnStoragePostgres.Maintenance.RetentionFamilies
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.Schemas.MaintenanceJob

  @job_id "retention:scheduler"
  @lock 704_202_609

  def status(context) do
    with :ok <- authorize(context) do
      case Repo.get(MaintenanceJob, @job_id) do
        nil -> {:ok, %{version: 0, policy: %Policy{}, cursor: %{}, processed_count: 0}}
        job -> {:ok, state(job)}
      end
    end
  rescue
    error -> {:error, ErrorMapper.map(error)}
  end

  def configure(command) do
    with :ok <- authorize(command.platform_context),
         {:ok, policy} <- Policy.new(command.policy),
         :ok <- version_valid(command.expected_version) do
      transaction(fn ->
        lock!()
        job = initialize!(policy)
        check_version!(job, command.expected_version)

        job
        |> Ecto.Changeset.change(
          configuration: Policy.encode(policy),
          cursor:
            Map.new(job.cursor || %{}, fn {k, v} ->
              {k, if(is_map(v), do: Map.delete(v, "cutoff"), else: v)}
            end),
          version: job.version + 1,
          updated_at: now!()
        )
        |> Repo.update!()
        |> state()
      end)
    end
  end

  def batch(command) do
    started = System.monotonic_time()

    result =
      with :ok <- authorize(command.platform_context),
           {:ok, policy} <- Policy.new(command.policy),
           :ok <- version_valid(command.expected_version) do
        transaction(fn ->
          lock!()
          job = initialize!(policy)
          check_version!(job, command.expected_version)

          if job.configuration != Policy.encode(policy),
            do: fail!(:conflict, "retention policy mismatch")

          SQL.query!(Repo, "SELECT set_config('transaction_timeout', $1, true)", [
            "#{policy.turn_budget_ms}ms"
          ])

          now = now!()
          cursor = job.cursor || %{}
          due = cursor["due_at"]

          if command.scheduled? and not is_nil(due) and
               DateTime.compare(now, timestamp!(due)) == :lt do
            Map.put(state(job), :batch_count, 0)
          else
            family =
              Enum.at(
                Policy.families(),
                rem(cursor["family_index"] || 0, length(Policy.families()))
              )

            period = Policy.period(policy, family)

            result =
              if period == :retain_forever do
                %{deleted_count: 0, cursor: cursor[Atom.to_string(family)]}
              else
                previous = cursor[Atom.to_string(family)] || %{}

                cutoff =
                  if is_binary(previous["cutoff"]) and previous["phase"] != 0,
                    do: timestamp!(previous["cutoff"]),
                    else: DateTime.add(now, -period, :second)

                result = RetentionFamilies.delete!(family, policy, cutoff, previous)

                next_cursor =
                  if result.cursor,
                    do: Map.put(result.cursor, "cutoff", DateTime.to_iso8601(cutoff)),
                    else: nil

                %{result | cursor: next_cursor}
              end

            next = rem((cursor["family_index"] || 0) + 1, length(Policy.families()))

            cursor =
              cursor
              |> Map.put("family_index", next)
              |> Map.put(Atom.to_string(family), result.cursor)
              |> Map.put(
                "due_at",
                DateTime.to_iso8601(DateTime.add(now, policy.interval_ms, :millisecond))
              )
              |> Map.put("last_check_at", DateTime.to_iso8601(now))

            cursor =
              if result.deleted_count > 0,
                do: Map.put(cursor, "last_deletion_at", DateTime.to_iso8601(now)),
                else: cursor

            updated =
              job
              |> Ecto.Changeset.change(
                cursor: cursor,
                updated_at: now,
                processed_count: job.processed_count + result.deleted_count,
                version: job.version + 1
              )
              |> Repo.update!()

            Map.merge(state(updated), %{batch_count: result.deleted_count, family: family})
          end
        end)
      end

    case result do
      {:ok, %{family: family, batch_count: count}} ->
        :telemetry.execute(
          [:favn, :retention, :batch],
          %{deleted_count: count, duration: System.monotonic_time() - started},
          %{family: family}
        )

      _ ->
        :ok
    end

    result
  end

  def preview(context, family) do
    with :ok <- authorize(context),
         true <- family in Policy.families() do
      transaction(fn ->
        SQL.query!(Repo, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", [])
        {:ok, %{policy: policy, version: version}} = status(context)
        period = Policy.period(policy, family)

        result =
          if period == :retain_forever do
            %{family: family, eligible_count: 0, complete?: true, reason: :disabled}
          else
            cutoff = DateTime.add(now!(), -period, :second)
            RetentionFamilies.preview!(family, policy, cutoff)
          end

        Map.put(result, :version, version)
      end)
    else
      false -> {:error, Error.new(:invalid, "unknown retention family")}
      error -> error
    end
  end

  # Serialize family deletion and projection repair.
  def lock! do
    %{rows: [[acquired]]} = SQL.query!(Repo, "SELECT pg_try_advisory_xact_lock($1)", [@lock])
    unless acquired, do: fail!(:conflict, "retention batch is busy")
    :ok
  end

  def now! do
    %{rows: [[now]]} = SQL.query!(Repo, "SELECT clock_timestamp()", [])
    now
  end

  defp initialize!(policy) do
    now = now!()

    Repo.insert_all(
      MaintenanceJob,
      [
        %{
          job_id: @job_id,
          job_kind: "retention",
          scope_kind: "platform",
          status: "running",
          configuration: Policy.encode(policy),
          cursor: %{},
          fencing_token: 0,
          processed_count: 0,
          version: 1,
          inserted_at: now,
          updated_at: now
        }
      ],
      on_conflict: :nothing
    )

    Repo.one!(from(j in MaintenanceJob, where: j.job_id == @job_id, lock: "FOR UPDATE"))
  end

  defp state(job) do
    case Policy.decode(job.configuration) do
      {:ok, policy} ->
        %{
          version: job.version,
          policy: policy,
          cursor: job.cursor,
          processed_count: job.processed_count
        }

      _ ->
        fail!(:invalid, "invalid persisted retention policy")
    end
  end

  defp check_version!(%{version: 1, processed_count: 0, cursor: cursor}, 0) when cursor == %{},
    do: :ok

  defp check_version!(%{version: version}, version), do: :ok

  defp check_version!(_, _),
    do: fail!(:conflict, "retention version changed; read status before retrying")

  defp version_valid(v) when is_integer(v) and v >= 0, do: :ok
  defp version_valid(_), do: {:error, Error.new(:invalid, "invalid retention version")}

  defp authorize(%PlatformContext{roles: roles} = context) do
    if PlatformContext.valid?(context) and
         Enum.any?(roles, &(&1 in [:platform_operator, :platform_admin])),
       do: :ok,
       else: {:error, Error.new(:invalid, "platform maintenance authority required")}
  end

  defp authorize(_), do: {:error, Error.new(:invalid, "platform maintenance authority required")}
  defp timestamp!(value), do: value |> DateTime.from_iso8601() |> elem(1)
  defp fail!(kind, message), do: Repo.rollback(Error.new(kind, message))

  defp transaction(fun) do
    Repo.transaction(
      fn ->
        SQL.query!(Repo, "SET LOCAL lock_timeout = '100ms'", [])
        SQL.query!(Repo, "SET LOCAL statement_timeout = '1s'", [])
        SQL.query!(Repo, "SET LOCAL transaction_timeout = '5s'", [])
        fun.()
      end,
      timeout: 6_000
    )
  rescue
    error -> {:error, ErrorMapper.map(error)}
  catch
    :exit, reason -> {:error, ErrorMapper.map(reason)}
  end
end
