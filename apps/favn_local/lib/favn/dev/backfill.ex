defmodule Favn.Dev.Backfill do
  @moduledoc """
  Local operational-backfill workflow for a running `mix favn.dev` stack.

  This module backs `mix favn.backfill`. It submits operational backfills,
  dry-runs backfill plans, reads backfill-window projections, reruns failed or
  explicitly force-refreshed successful windows, and repairs derived backfill
  read models through the private local orchestrator HTTP boundary.

  Submit options accept explicit `:from`, `:to`, and `:kind` values or compact
  `:window` ranges such as `"month:2025-05..2026-05"`. Pass `refresh: "force"`
  to recompute windows even when freshness state is already successful. Month
  and year inputs may be provided as full dates; they are normalized to the
  anchor value expected by the orchestrator.
  """

  alias Favn.Dev.ComposeSession
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Run

  @terminal_statuses ["ok", "partial", "error", "cancelled", "timed_out"]
  @default_timeout_ms 60_000
  @default_poll_interval_ms 1_000

  @type workflow_opts :: [root_dir: Path.t()]
  @type repair_opts :: [
          root_dir: Path.t(),
          apply: boolean(),
          backfill_run_id: String.t(),
          pipeline_module: String.t() | module()
        ]
  @type submit_opts :: [
          root_dir: Path.t(),
          from: String.t(),
          to: String.t(),
          kind: String.t() | atom(),
          window: String.t(),
          dry_run: boolean(),
          timezone: String.t(),
          coverage_baseline_id: String.t(),
          wait: boolean(),
          retry_max_attempts: pos_integer(),
          retry_backoff_ms: non_neg_integer(),
          timeout_ms: pos_integer(),
          wait_timeout_ms: pos_integer(),
          run_timeout_ms: pos_integer(),
          poll_interval_ms: pos_integer(),
          refresh: String.t(),
          metadata: map()
        ]

  @spec submit_pipeline(module() | String.t(), submit_opts()) :: {:ok, map()} | {:error, term()}
  def submit_pipeline(pipeline_module, opts \\ [])

  def submit_pipeline(pipeline_module, opts)
      when (is_atom(pipeline_module) or is_binary(pipeline_module)) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, range} <- build_range(opts),
         {:ok, base_url, credentials, session_context} <- session(opts) do
      case maybe_submit_or_plan(
             pipeline_module,
             range,
             base_url,
             credentials,
             session_context,
             opts
           ) do
        {:error, reason} -> {:error, unwrap_submit_error(reason)}
        result -> result
      end
    else
      {:error, reason} -> {:error, unwrap_submit_error(reason)}
    end
  end

  def submit_pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

  defp maybe_submit_or_plan(pipeline_module, range, base_url, credentials, session_context, opts) do
    if Keyword.get(opts, :dry_run, false) do
      with {:ok, payload} <- build_plan_payload(pipeline_module, range, opts) do
        OrchestratorClient.plan_backfill(
          base_url,
          credentials.service_token,
          session_context,
          payload
        )
      end
    else
      with {:ok, active_manifest} <-
             OrchestratorClient.active_manifest(
               base_url,
               credentials.service_token,
               session_context
             ),
           {:ok, target} <- Run.resolve_pipeline_target(active_manifest, pipeline_module),
           {:ok, payload} <- build_submit_payload(target, range, opts),
           {:ok, run} <-
             OrchestratorClient.submit_backfill(
               base_url,
               credentials.service_token,
               session_context,
               payload
             ),
           {:ok, final_run} <-
             maybe_wait(run, base_url, credentials.service_token, session_context, opts),
           :ok <- ensure_success(final_run, Keyword.get(opts, :wait, true)) do
        {:ok, final_run}
      end
    end
  end

  @spec plan_pipeline(module() | String.t(), submit_opts()) :: {:ok, map()} | {:error, term()}
  def plan_pipeline(pipeline_module, opts \\ [])

  def plan_pipeline(pipeline_module, opts)
      when (is_atom(pipeline_module) or is_binary(pipeline_module)) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, range} <- build_range(opts),
         {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, payload} <- build_plan_payload(pipeline_module, range, opts) do
      OrchestratorClient.plan_backfill(
        base_url,
        credentials.service_token,
        session_context,
        payload
      )
    else
      {:error, reason} -> {:error, unwrap_submit_error(reason)}
    end
  end

  def plan_pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

  @doc "Plans exact currently missing windows for one active asset."
  @spec plan_missing_asset(module() | String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def plan_missing_asset(asset, opts \\ [])

  def plan_missing_asset(asset, opts)
      when (is_atom(asset) or is_binary(asset)) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, target_id} <- resolve_asset_target(base_url, credentials, session_context, asset) do
      OrchestratorClient.plan_missing_coverage_backfill(
        base_url,
        credentials.service_token,
        session_context,
        target_id,
        Keyword.take(opts, [:cursor, :limit])
      )
    end
  end

  def plan_missing_asset(_asset, _opts), do: {:error, :invalid_asset}

  @doc "Submits one previously reviewed exact missing-window plan."
  @spec submit_missing_asset(module() | String.t(), map(), keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def submit_missing_asset(asset, plan, opts \\ [])

  def submit_missing_asset(asset, plan, opts)
      when (is_atom(asset) or is_binary(asset)) and is_map(plan) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, target_id} <- resolve_asset_target(base_url, credentials, session_context, asset) do
      OrchestratorClient.submit_missing_coverage_backfill(
        base_url,
        credentials.service_token,
        session_context,
        target_id,
        plan
      )
    end
  end

  def submit_missing_asset(_asset, _plan, _opts), do: {:error, :invalid_asset}

  defp resolve_asset_target(base_url, credentials, session_context, asset) do
    with {:ok, active_manifest} <-
           OrchestratorClient.active_manifest(
             base_url,
             credentials.service_token,
             session_context
           ),
         {:ok, %{"target_type" => "asset", "target_id" => target_id}} <-
           Run.resolve_run_target(active_manifest, asset) do
      {:ok, target_id}
    else
      {:ok, %{"target_type" => _other}} -> {:error, :missing_coverage_requires_asset}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_asset_target}
    end
  end

  @spec list_windows(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def list_windows(backfill_run_id, opts \\ [])
      when is_binary(backfill_run_id) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.list_backfill_windows(
        base_url,
        credentials.service_token,
        session_context,
        backfill_run_id,
        filters(opts, [:pipeline_module, :window_key, :status, :limit, :offset])
      )
    end
  end

  @spec list_coverage_baselines(keyword()) :: {:ok, map()} | {:error, term()}
  def list_coverage_baselines(opts \\ []) when is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.list_coverage_baselines(
        base_url,
        credentials.service_token,
        session_context,
        filters(opts, [:pipeline_module, :source_key, :segment_key_hash, :status, :limit, :offset])
      )
    end
  end

  @spec list_asset_window_states(keyword()) :: {:ok, map()} | {:error, term()}
  def list_asset_window_states(opts \\ []) when is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.list_asset_window_states(
        base_url,
        credentials.service_token,
        session_context,
        filters(opts, [
          :asset_ref_module,
          :asset_ref_name,
          :pipeline_module,
          :window_key,
          :status,
          :limit,
          :offset
        ])
      )
    end
  end

  @spec rerun_window(String.t(), String.t(), workflow_opts() | submit_opts()) ::
          {:ok, map()} | {:error, term()}
  def rerun_window(backfill_run_id, window_key, opts \\ [])
      when is_binary(backfill_run_id) and is_binary(window_key) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.rerun_backfill_window(
        base_url,
        credentials.service_token,
        session_context,
        backfill_run_id,
        window_key,
        rerun_window_payload(opts)
      )
    end
  end

  @spec repair_projections(repair_opts()) :: {:ok, map()} | {:error, term()}
  def repair_projections(opts \\ []) when is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, payload} <- build_repair_payload(opts) do
      OrchestratorClient.repair_backfill_projections(
        base_url,
        credentials.service_token,
        session_context,
        payload
      )
    end
  end

  @doc false
  @spec build_submit_payload(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def build_submit_payload(%{"target_id" => target_id}, range, opts)
      when is_binary(target_id) and target_id != "" and is_map(range) and is_list(opts) do
    payload = %{
      target: %{type: "pipeline", id: target_id},
      manifest_selection: %{mode: "active"},
      range: range
    }

    {:ok,
     payload
     |> maybe_put(:coverage_baseline_id, Keyword.get(opts, :coverage_baseline_id))
     |> maybe_put(:metadata, Keyword.get(opts, :metadata))
     |> maybe_put(:refresh, Keyword.get(opts, :refresh))
     |> maybe_put(:retry_policy, retry_policy(opts))
     |> maybe_put(:timeout_ms, run_timeout_ms(opts))}
  end

  def build_submit_payload(_target, _range, _opts), do: {:error, :invalid_pipeline_target}

  @doc false
  @spec build_plan_payload(module() | String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def build_plan_payload(pipeline_module, range, opts)
      when (is_atom(pipeline_module) or is_binary(pipeline_module)) and is_map(range) and
             is_list(opts) do
    payload = %{
      target: %{type: "pipeline", module: module_name(pipeline_module)},
      manifest_selection: %{mode: "active"},
      range: range
    }

    {:ok,
     payload
     |> maybe_put(:coverage_baseline_id, Keyword.get(opts, :coverage_baseline_id))
     |> maybe_put(:metadata, Keyword.get(opts, :metadata))
     |> maybe_put(:refresh, Keyword.get(opts, :refresh))
     |> maybe_put(:retry_policy, retry_policy(opts))
     |> maybe_put(:timeout_ms, run_timeout_ms(opts))}
  end

  def build_plan_payload(_pipeline_module, _range, _opts), do: {:error, :invalid_pipeline}

  @doc false
  @spec build_range(keyword()) :: {:ok, map()} | {:error, term()}
  def build_range(opts) when is_list(opts) do
    with :ok <- reject_mixed_window_opts(opts),
         {:ok, opts} <- expand_window_opt(opts),
         {:ok, from} <- required_string(opts, :from),
         {:ok, to} <- required_string(opts, :to),
         {:ok, kind} <- required_value(opts, :kind),
         kind <- to_string(kind),
         from <- normalize_range_value(kind, from),
         to <- normalize_range_value(kind, to) do
      {:ok,
       %{
         from: from,
         to: to,
         kind: kind,
         timezone: Keyword.get(opts, :timezone, "Etc/UTC")
       }}
    end
  end

  defp reject_mixed_window_opts(opts) do
    if Keyword.get(opts, :window) not in [nil, ""] and
         Enum.any?([:from, :to, :kind], &(Keyword.get(opts, &1) not in [nil, ""])) do
      {:error, :mixed_window_range_options}
    else
      :ok
    end
  end

  defp expand_window_opt(opts) do
    case Keyword.get(opts, :window) do
      nil -> {:ok, opts}
      "" -> {:error, {:invalid_window_range, ""}}
      window when is_binary(window) -> parse_window_range(window, opts)
      value -> {:error, {:invalid_window_range, value}}
    end
  end

  defp parse_window_range(window, opts) do
    case String.split(window, [":", ".."], parts: 3) do
      [kind, from, to] when kind != "" and from != "" and to != "" ->
        {:ok,
         opts |> Keyword.put(:kind, kind) |> Keyword.put(:from, from) |> Keyword.put(:to, to)}

      _other ->
        {:error, {:invalid_window_range, window}}
    end
  end

  @doc false
  @spec build_repair_payload(keyword()) :: {:ok, map()} | {:error, term()}
  def build_repair_payload(opts) when is_list(opts) do
    payload = %{apply: Keyword.get(opts, :apply, false)}

    payload = maybe_put(payload, :backfill_run_id, Keyword.get(opts, :backfill_run_id))
    payload = maybe_put(payload, :pipeline_module, Keyword.get(opts, :pipeline_module))

    if Map.has_key?(payload, :backfill_run_id) and Map.has_key?(payload, :pipeline_module) do
      {:error, :invalid_repair_scope}
    else
      {:ok, payload}
    end
  end

  defp validate_opts(opts) do
    [
      :timeout_ms,
      :wait_timeout_ms,
      :run_timeout_ms,
      :retry_max_attempts,
      :poll_interval_ms
    ]
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case validate_positive_integer(opts, key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      :ok -> validate_non_negative_integer(opts, :retry_backoff_ms)
      error -> error
    end
  end

  defp rerun_window_payload(opts) do
    %{}
    |> maybe_put(:refresh, Keyword.get(opts, :refresh))
    |> maybe_put(:allow_success, Keyword.get(opts, :allow_success))
  end

  defp validate_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  defp validate_non_negative_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value >= 0 -> :ok
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  defp session(opts), do: ComposeSession.resolve(opts)

  defp filters(opts, allowed) do
    opts
    |> Keyword.take(allowed)
    |> Enum.reject(fn {_key, value} -> is_nil(value) or value == "" end)
  end

  defp required_string(opts, key) do
    case Keyword.get(opts, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_option, key}}
    end
  end

  defp required_value(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:error, {:missing_option, key}}
      "" -> {:error, {:missing_option, key}}
      value -> {:ok, value}
    end
  end

  defp maybe_put(payload, _key, nil), do: payload
  defp maybe_put(payload, _key, ""), do: payload
  defp maybe_put(payload, key, value), do: Map.put(payload, key, value)

  defp normalize_range_value(kind, value) when kind in ["month", "monthly"] do
    case Date.from_iso8601(value) do
      {:ok, date} -> date |> Date.to_iso8601() |> binary_part(0, 7)
      {:error, _reason} -> value
    end
  end

  defp normalize_range_value(kind, value) when kind in ["year", "yearly"] do
    case Date.from_iso8601(value) do
      {:ok, date} -> date |> Date.to_iso8601() |> binary_part(0, 4)
      {:error, _reason} -> value
    end
  end

  defp normalize_range_value(_kind, value), do: value

  defp module_name(value) when is_atom(value), do: Atom.to_string(value)
  defp module_name(value) when is_binary(value), do: value

  defp run_timeout_ms(opts),
    do: Keyword.get(opts, :run_timeout_ms, Keyword.get(opts, :timeout_ms))

  defp retry_policy(opts) do
    if Keyword.has_key?(opts, :retry_max_attempts) or Keyword.has_key?(opts, :retry_backoff_ms) do
      %{
        max_attempts: Keyword.get(opts, :retry_max_attempts, 1),
        backoff: Keyword.get(opts, :retry_backoff_ms, 0)
      }
    end
  end

  defp maybe_wait(run, base_url, service_token, session_context, opts) do
    case {Keyword.get(opts, :wait, true), run} do
      {false, _run} ->
        {:ok, run}

      {true, %{"id" => run_id}} when is_binary(run_id) and run_id != "" ->
        timeout_ms =
          Keyword.get(opts, :wait_timeout_ms, Keyword.get(opts, :timeout_ms, @default_timeout_ms))

        poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
        deadline = System.monotonic_time(:millisecond) + timeout_ms

        wait_for_run(
          run,
          run_id,
          base_url,
          service_token,
          session_context,
          deadline,
          poll_interval_ms
        )

      _other ->
        {:error, :invalid_run_response}
    end
  end

  defp wait_for_run(
         run,
         run_id,
         base_url,
         service_token,
         session_context,
         deadline,
         poll_interval_ms
       ) do
    if terminal_status?(run) do
      {:ok, run}
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        {:error, {:run_wait_timeout, run_id}}
      else
        Process.sleep(min(poll_interval_ms, max(deadline - now, 0)))

        with {:ok, next_run} <-
               OrchestratorClient.get_run(
                 base_url,
                 service_token,
                 session_context,
                 run_id
               ) do
          wait_for_run(
            next_run,
            run_id,
            base_url,
            service_token,
            session_context,
            deadline,
            poll_interval_ms
          )
        end
      end
    end
  end

  defp ensure_success(_run, false), do: :ok

  defp ensure_success(run, true) do
    case run_status(run) do
      "ok" ->
        :ok

      "partial" ->
        {:error, {:run_failed, "backfill parent run finished with status partial", run}}

      status when status in ["error", "cancelled", "timed_out"] ->
        {:error, {:run_failed, run}}

      _other ->
        :ok
    end
  end

  defp terminal_status?(run), do: run_status(run) in @terminal_statuses
  defp run_status(run), do: Map.get(run, "status") || Map.get(run, :status)

  defp unwrap_submit_error(%{operation: operation, reason: {:http_error, 422, payload}})
       when operation in [:submit_backfill, :plan_backfill] do
    {:orchestrator_validation_failed, validation_error_code(payload)}
  end

  defp unwrap_submit_error(reason), do: reason

  defp validation_error_code(%{error_code: code}) when is_binary(code) and code != "", do: code
  defp validation_error_code(_payload), do: "validation_failed"
end
