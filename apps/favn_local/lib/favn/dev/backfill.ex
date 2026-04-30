defmodule Favn.Dev.Backfill do
  @moduledoc """
  Local operational-backfill workflow for a running `mix favn.dev` stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.Run
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @terminal_statuses ["ok", "partial", "error", "cancelled", "timed_out"]
  @default_timeout_ms 60_000
  @default_poll_interval_ms 1_000

  @type workflow_opts :: [root_dir: Path.t()]
  @type submit_opts :: [
          root_dir: Path.t(),
          from: String.t(),
          to: String.t(),
          kind: String.t() | atom(),
          timezone: String.t(),
          coverage_baseline_id: String.t(),
          wait: boolean(),
          max_attempts: pos_integer(),
           retry_backoff_ms: non_neg_integer(),
           timeout_ms: pos_integer(),
           wait_timeout_ms: pos_integer(),
           run_timeout_ms: pos_integer(),
           poll_interval_ms: pos_integer(),
          metadata: map()
        ]

  @spec submit_pipeline(module() | String.t(), submit_opts()) :: {:ok, map()} | {:error, term()}
  def submit_pipeline(pipeline_module, opts \\ [])

  def submit_pipeline(pipeline_module, opts)
      when (is_atom(pipeline_module) or is_binary(pipeline_module)) and is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, range} <- build_range(opts),
         {:ok, base_url, credentials, session_context} <- session(opts),
         {:ok, active_manifest} <-
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
    else
      {:error, reason} -> {:error, unwrap_submit_error(reason)}
    end
  end

  def submit_pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

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

  @spec rerun_window(String.t(), String.t(), workflow_opts()) :: {:ok, map()} | {:error, term()}
  def rerun_window(backfill_run_id, window_key, opts \\ [])
      when is_binary(backfill_run_id) and is_binary(window_key) and is_list(opts) do
    with {:ok, base_url, credentials, session_context} <- session(opts) do
      OrchestratorClient.rerun_backfill_window(
        base_url,
        credentials.service_token,
        session_context,
        backfill_run_id,
        window_key
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
     |> maybe_put(:max_attempts, Keyword.get(opts, :max_attempts))
     |> maybe_put(:retry_backoff_ms, Keyword.get(opts, :retry_backoff_ms))
      |> maybe_put(:timeout_ms, run_timeout_ms(opts))}
  end

  def build_submit_payload(_target, _range, _opts), do: {:error, :invalid_pipeline_target}

  @doc false
  @spec build_range(keyword()) :: {:ok, map()} | {:error, term()}
  def build_range(opts) when is_list(opts) do
    with {:ok, from} <- required_string(opts, :from),
         {:ok, to} <- required_string(opts, :to),
         {:ok, kind} <- required_value(opts, :kind) do
      {:ok,
       %{
         from: from,
         to: to,
         kind: to_string(kind),
         timezone: Keyword.get(opts, :timezone, "Etc/UTC")
       }}
    end
  end

  defp validate_opts(opts) do
    Enum.reduce_while([:timeout_ms, :wait_timeout_ms, :run_timeout_ms, :poll_interval_ms], :ok, fn
      key, :ok ->
        case validate_positive_integer(opts, key) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp validate_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  defp session(opts) do
    with :ok <- ensure_running(opts),
         {:ok, runtime, secrets} <- read_runtime_snapshot(opts),
         {:ok, credentials} <- local_credentials(secrets),
         base_url = base_url(runtime, opts),
         {:ok, session_context} <-
           OrchestratorClient.password_login(
             base_url,
             credentials.service_token,
             credentials.username,
             credentials.password
           ) do
      {:ok, base_url, credentials, session_context}
    end
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      _other -> {:error, :stack_not_running}
    end
  end

  defp read_runtime_snapshot(opts) do
    with {:ok, runtime} <- State.read_runtime(opts),
         {:ok, secrets} <- State.read_secrets(opts) do
      {:ok, runtime, secrets}
    end
  end

  defp local_credentials(secrets) do
    with token when is_binary(token) and token != "" <- secrets["service_token"],
         username when is_binary(username) and username != "" <-
           secrets["local_operator_username"],
         password when is_binary(password) and password != "" <-
           secrets["local_operator_password"] do
      {:ok, %{service_token: token, username: username, password: password}}
    else
      _other -> {:error, :missing_local_operator_credentials}
    end
  end

  defp base_url(runtime, opts) do
    runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url
  end

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

  defp run_timeout_ms(opts), do: Keyword.get(opts, :run_timeout_ms, Keyword.get(opts, :timeout_ms))

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
      "ok" -> :ok
      "partial" -> {:error, {:run_failed, "backfill parent run finished with status partial", run}}
      status when status in ["error", "cancelled", "timed_out"] -> {:error, {:run_failed, run}}
      _other -> :ok
    end
  end

  defp terminal_status?(run), do: run_status(run) in @terminal_statuses
  defp run_status(run), do: Map.get(run, "status") || Map.get(run, :status)

  defp unwrap_submit_error(%{operation: :submit_backfill, reason: {:http_error, 422, payload}}) do
    case get_in(payload, ["error", "message"]) do
      message when is_binary(message) and message != "" ->
        {:orchestrator_validation_failed, message}

      _other ->
        {:orchestrator_validation_failed, inspect(payload)}
    end
  end

  defp unwrap_submit_error(reason), do: reason
end
