Code.require_file("support.exs", __DIR__)

defmodule RegistrationStress.Load do
  @moduledoc false
  alias RegistrationStress.Support, as: S
  @target "pipeline:Elixir.CrmDemo.RegistrationStress.Pipeline:registration_stress"

  def run(opts) do
    S.ensure(
      Regex.match?(~r/^favn-763-[a-z0-9-]+$/, Map.fetch!(opts, :project)),
      "Use isolated favn-763-* project"
    )

    S.ensure(
      Regex.match?(~r/^[A-Za-z0-9-]{1,100}$/, Map.fetch!(opts, :key_prefix)),
      "Use unique alphanumeric/hyphen key prefix"
    )

    S.ensure(
      opts.runs in 1..50 and opts.max_in_flight in 1..5 and opts.timeout in 1..3600,
      "Runs <= 50, in-flight <= 5, timeout <= 3600s"
    )

    env = S.env()
    port = Map.get(env, "FAVN_API_HOST_PORT", "4101")

    ports =
      S.docker([
        "inspect",
        "--format",
        ~s|{{json (index .NetworkSettings.Ports "4101/tcp")}}|,
        opts.project <> "-control-plane-1"
      ])
      |> JSON.decode!()

    S.ensure(
      Enum.any?(ports || [], &(&1["HostIp"] == "127.0.0.1" and &1["HostPort"] == port)),
      "API port does not belong to this isolated project"
    )

    ledger =
      Path.join(
        S.root(),
        ".favn/registration-stress/cases/#{opts.project}-#{opts.key_prefix}.json"
      )

    S.ensure(
      not File.exists?(ledger),
      "Key prefix reserved; inspect original evidence before resolving any uncertain request"
    )

    io = S.evidence(Map.fetch!(opts, :output))

    try do
      reservation = S.evidence(ledger)

      S.emit(
        %{
          project: opts.project,
          key_prefix: opts.key_prefix,
          evidence: Path.expand(opts.output),
          reserved_at: S.now()
        },
        reservation
      )

      File.close(reservation)
      started = S.monotonic()

      emit(io, "load_started", %{
        project: opts.project,
        requested_runs: opts.runs,
        max_in_flight: opts.max_in_flight,
        asset_sleep_ms: 0,
        target: @target,
        workload_model: "bounded_backlog"
      })

      loop(opts, env, port, io, started, %{}, %{})
    after
      File.close(io)
    end
  end

  defp loop(opts, env, port, io, started, accepted, terminal) do
    if S.monotonic() >= started + opts.timeout * 1000 do
      emit(io, "load_timeout", %{submitted: map_size(accepted), terminal: map_size(terminal)})
      1
    else
      accepted = submit(opts, env, port, io, accepted, map_size(terminal))
      ids = Enum.map_join(accepted, ",", fn {id, _} -> "'#{id}'" end)

      states =
        S.query(opts.project, """
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
          'submission_status',s.status,'run_id',s.run_id,
          'run_status',r.status,'enqueued_at',s.enqueued_at,
          'started_at',r.inserted_at,'terminal_at',r.terminal_at,
          'failure_kind',s.failure_kind)), '[]'::jsonb)
        FROM favn_control.run_submissions s LEFT JOIN favn_control.runs r
          ON r.workspace_id=s.workspace_id AND r.run_id=s.run_id
        WHERE s.workspace_id='elastic-simulation' AND s.run_id IN (#{ids});
        """)

      terminal =
        Enum.reduce(states, terminal, fn state, acc ->
          if (state["terminal_at"] != nil or state["submission_status"] in ["failed", "cancelled"]) and
               not Map.has_key?(acc, state["run_id"]) do
            emit(
              io,
              "run_terminal",
              Map.put(state, "idempotency_key", Map.fetch!(accepted, state["run_id"]))
            )

            Map.put(acc, state["run_id"], state)
          else
            acc
          end
        end)

      if map_size(terminal) == opts.runs do
        ok = Enum.count(terminal, fn {_, state} -> state["run_status"] == "ok" end)

        emit(io, "load_finished", %{
          submitted: map_size(accepted),
          successful: ok,
          failed: map_size(terminal) - ok,
          elapsed_seconds: (S.monotonic() - started) / 1000
        })

        if ok == opts.runs, do: 0, else: 1
      else
        S.sleep(1000)
        loop(opts, env, port, io, started, accepted, terminal)
      end
    end
  end

  defp submit(opts, env, port, io, accepted, terminal_count) do
    if map_size(accepted) < opts.runs and map_size(accepted) - terminal_count < opts.max_in_flight do
      S.sleep(0)
      sequence = map_size(accepted) + 1
      key = "#{opts.key_prefix}-#{sequence}"

      payload = %{
        target: %{type: "pipeline", id: @target},
        refresh: "force_all",
        metadata: %{local_stress_sequence: sequence, local_stress_key_prefix: opts.key_prefix}
      }

      emit(io, "submission_intent", %{idempotency_key: key, payload: payload})
      began = S.monotonic()
      response = request(port, payload, env, key)

      case response do
        {:ok, status, %{"data" => %{"run" => %{"id" => id}}} = body}
        when status in 200..299 and is_binary(id) ->
          unless Regex.match?(~r/^[A-Za-z0-9_-]{1,128}$/, id) and not Map.has_key?(accepted, id) do
            unknown!(io, key)
          end

          emit(io, "submission_accepted", %{
            idempotency_key: key,
            http_status: status,
            request_ms: S.monotonic() - began,
            response: body
          })

          submit(opts, env, port, io, Map.put(accepted, id, key), terminal_count)

        {:ok, status, _} when status in 400..499 and status != 408 ->
          emit(io, "submission_rejected", %{idempotency_key: key, http_status: status})
          raise "Submission stopped; inspect original key before retrying"

        _ ->
          unknown!(io, key)
      end
    else
      accepted
    end
  end

  defp request(port, payload, env, key) do
    S.http(
      "http://127.0.0.1:#{port}/api/orchestrator/v1/runs",
      :post,
      payload,
      [
        {"Authorization", "Bearer " <> Map.fetch!(env, "FAVN_PLATFORM_TOKEN")},
        {"X-Favn-Workspace-Id", "elastic-simulation"},
        {"Idempotency-Key", key}
      ],
      30_000
    )
  rescue
    _ -> {:error, :invalid_response}
  end

  defp unknown!(io, key) do
    emit(io, "submission_outcome_unknown", %{idempotency_key: key})
    raise "Unknown submission outcome; resolve original key before retrying"
  end

  defp emit(io, event, fields), do: S.emit(Map.merge(fields, %{event: event, at: S.now()}), io)
end

alias RegistrationStress.Support, as: S

{opts, []} =
  S.options(
    System.argv(),
    [
      project: :string,
      runs: :integer,
      max_in_flight: :integer,
      timeout: :integer,
      key_prefix: :string,
      output: :string
    ],
    runs: 10,
    max_in_flight: 2,
    timeout: 900
  )

if opts[:help] do
  IO.puts(
    "elixir load.exs --project favn-763-NAME --key-prefix UNIQUE --output FILE [--runs 10 --max-in-flight 2 --timeout 900]"
  )
else
  S.trap_termination()
  System.halt(RegistrationStress.Load.run(opts))
end
