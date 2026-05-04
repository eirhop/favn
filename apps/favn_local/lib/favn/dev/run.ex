defmodule Favn.Dev.Run do
  @moduledoc """
  Local run submission workflow for a running `mix favn.dev` stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.State
  alias Favn.Dev.Status
  alias Favn.Window.Request, as: WindowRequest

  @terminal_statuses ["ok", "error", "cancelled", "timed_out"]
  @default_timeout_ms 60_000
  @default_poll_interval_ms 1_000

  @type run_opts :: [
          root_dir: Path.t(),
          wait: boolean(),
          window: String.t(),
          timezone: String.t(),
          timeout_ms: non_neg_integer(),
          poll_interval_ms: pos_integer()
        ]

  @spec pipeline(module() | String.t(), run_opts()) :: {:ok, map()} | {:error, term()}
  def pipeline(pipeline_module, opts \\ [])

  def pipeline(pipeline_module, opts)
      when is_atom(pipeline_module) or is_binary(pipeline_module) do
    with :ok <- validate_opts(opts),
         {:ok, window_request} <- parse_window_request(opts),
         :ok <- ensure_running(opts),
         {:ok, runtime} <- read_runtime_snapshot(opts),
         credentials = local_credentials(),
         session_context = local_dev_context(),
         {:ok, active_manifest} <-
           OrchestratorClient.active_manifest(
             base_url(runtime, opts),
             credentials.service_token,
             session_context
           ),
         {:ok, target} <- resolve_pipeline_target(active_manifest, pipeline_module),
         {:ok, run} <-
           submit_pipeline_run(
             base_url(runtime, opts),
             credentials.service_token,
             session_context,
             target,
             window_request
           ),
         {:ok, final_run} <-
           maybe_wait(run, runtime, credentials.service_token, session_context, opts),
         :ok <- ensure_success(final_run, Keyword.get(opts, :wait, true)) do
      {:ok, final_run}
    end
  end

  def pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

  defp validate_opts(opts) do
    case validate_timezone_without_window(opts) do
      :ok ->
        case validate_positive_integer(opts, :timeout_ms) do
          :ok -> validate_positive_integer(opts, :poll_interval_ms)
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_timezone_without_window(opts) do
    if Keyword.has_key?(opts, :timezone) and not Keyword.has_key?(opts, :window),
      do: {:error, {:invalid_option, :timezone_without_window}},
      else: :ok
  end

  defp validate_positive_integer(opts, key) do
    case Keyword.fetch(opts, key) do
      :error -> :ok
      {:ok, value} when is_integer(value) and value > 0 -> :ok
      {:ok, _value} -> {:error, {:invalid_option, key}}
    end
  end

  @doc false
  @spec resolve_pipeline_target(map(), module() | String.t()) :: {:ok, map()} | {:error, term()}
  def resolve_pipeline_target(active_manifest, pipeline_module)
      when is_map(active_manifest) and (is_atom(pipeline_module) or is_binary(pipeline_module)) do
    requested = normalize_pipeline_name(pipeline_module)
    pipelines = get_in(active_manifest, ["targets", "pipelines"]) || []

    case Enum.find(pipelines, &pipeline_target_match?(&1, requested)) do
      %{} = target -> {:ok, target}
      _other -> {:error, {:pipeline_not_found, requested, available_pipeline_labels(pipelines)}}
    end
  end

  defp ensure_running(opts) do
    case Status.inspect_stack(opts).stack_status do
      :running -> :ok
      :partial -> {:error, :stack_not_healthy}
      :stale -> {:error, :stack_not_running}
      :stopped -> {:error, :stack_not_running}
      _other -> {:error, :stack_not_running}
    end
  end

  defp read_runtime_snapshot(opts) do
    State.read_runtime(opts)
  end

  defp local_credentials, do: %{service_token: ""}

  defp local_dev_context do
    %{
      "actor_id" => "local-dev-cli",
      "session_id" => "local-dev-cli",
      "local_dev_context" => "trusted"
    }
  end

  defp base_url(runtime, opts) do
    runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url
  end

  defp parse_window_request(opts) do
    case Keyword.fetch(opts, :window) do
      :error ->
        {:ok, nil}

      {:ok, value} ->
        parse_opts =
          case Keyword.fetch(opts, :timezone) do
            {:ok, timezone} -> [timezone: timezone]
            :error -> []
          end

        case WindowRequest.parse(value, parse_opts) do
          {:ok, request} -> {:ok, request}
          {:error, reason} -> {:error, {:invalid_window_request, reason}}
        end
    end
  end

  defp submit_pipeline_run(base_url, service_token, session_context, target, window_request) do
    case target do
      %{"target_id" => target_id} when is_binary(target_id) and target_id != "" ->
        payload =
          %{
            target: %{type: "pipeline", id: target_id},
            manifest_selection: %{mode: "active"}
          }
          |> maybe_put_window(window_request)

        case OrchestratorClient.submit_run(base_url, service_token, session_context, payload) do
          {:ok, _run} = ok -> ok
          {:error, reason} -> {:error, unwrap_submit_error(reason)}
        end

      _other ->
        {:error, :invalid_pipeline_target}
    end
  end

  defp maybe_put_window(payload, nil), do: payload

  defp maybe_put_window(payload, %WindowRequest{} = request) do
    Map.put(payload, :window, %{
      mode: Atom.to_string(request.mode),
      kind: Atom.to_string(request.kind),
      value: request.value,
      timezone: request.timezone
    })
  end

  defp unwrap_submit_error(%{operation: :submit_run, reason: {:http_error, 422, payload}}) do
    case get_in(payload, ["error", "message"]) do
      message when is_binary(message) and message != "" ->
        {:orchestrator_validation_failed, message}

      _other ->
        {:orchestrator_validation_failed, inspect(payload)}
    end
  end

  defp unwrap_submit_error(reason), do: reason

  defp maybe_wait(run, runtime, service_token, session_context, opts) do
    case {Keyword.get(opts, :wait, true), run} do
      {false, _run} ->
        {:ok, run}

      {true, %{"id" => run_id}} when is_binary(run_id) and run_id != "" ->
        timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
        poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
        deadline = System.monotonic_time(:millisecond) + timeout_ms

        wait_for_run(
          run,
          run_id,
          runtime,
          service_token,
          session_context,
          deadline,
          poll_interval_ms,
          opts
        )

      _other ->
        {:error, :invalid_run_response}
    end
  end

  defp wait_for_run(
         run,
         run_id,
         runtime,
         service_token,
         session_context,
         deadline,
         poll_interval_ms,
         opts
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
                 base_url(runtime, opts),
                 service_token,
                 session_context,
                 run_id
               ) do
          wait_for_run(
            next_run,
            run_id,
            runtime,
            service_token,
            session_context,
            deadline,
            poll_interval_ms,
            opts
          )
        end
      end
    end
  end

  defp ensure_success(_run, false), do: :ok

  defp ensure_success(run, true) do
    case run_status(run) do
      "ok" -> :ok
      status when status in ["error", "cancelled", "timed_out"] -> {:error, {:run_failed, run}}
      _other -> :ok
    end
  end

  defp terminal_status?(run), do: run_status(run) in @terminal_statuses

  defp run_status(run), do: Map.get(run, "status") || Map.get(run, :status)

  defp normalize_pipeline_name(pipeline_module) when is_atom(pipeline_module),
    do: inspect(pipeline_module)

  defp normalize_pipeline_name(pipeline_module) when is_binary(pipeline_module),
    do: String.trim(pipeline_module)

  defp pipeline_target_match?(%{} = target, requested) do
    Map.get(target, "label") == requested or Map.get(target, "target_id") == requested
  end

  defp pipeline_target_match?(_target, _requested), do: false

  defp available_pipeline_labels(pipelines) do
    pipelines
    |> Enum.map(fn
      %{"label" => label} when is_binary(label) -> label
      %{"target_id" => target_id} when is_binary(target_id) -> target_id
      _other -> nil
    end)
    |> Enum.reject(&is_nil/1)
  end
end
