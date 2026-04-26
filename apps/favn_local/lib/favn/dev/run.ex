defmodule Favn.Dev.Run do
  @moduledoc """
  Local run submission workflow for a running `mix favn.dev` stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.State
  alias Favn.Dev.Status

  @terminal_statuses ["ok", "error", "cancelled", "timed_out"]
  @default_timeout_ms 60_000
  @default_poll_interval_ms 1_000

  @type run_opts :: [
          root_dir: Path.t(),
          wait: boolean(),
          timeout_ms: non_neg_integer(),
          poll_interval_ms: pos_integer()
        ]

  @spec pipeline(module() | String.t(), run_opts()) :: {:ok, map()} | {:error, term()}
  def pipeline(pipeline_module, opts \\ [])

  def pipeline(pipeline_module, opts) when is_atom(pipeline_module) or is_binary(pipeline_module) do
    with :ok <- validate_opts(opts),
         :ok <- ensure_running(opts),
         {:ok, runtime, secrets} <- read_runtime_snapshot(opts),
         {:ok, credentials} <- local_credentials(secrets),
         {:ok, session_context} <-
           OrchestratorClient.password_login(
             base_url(runtime, opts),
             credentials.service_token,
             credentials.username,
             credentials.password
           ),
         {:ok, active_manifest} <-
           OrchestratorClient.active_manifest(
             base_url(runtime, opts),
             credentials.service_token,
             session_context
           ),
         {:ok, target} <- resolve_pipeline_target(active_manifest, pipeline_module),
         {:ok, run} <-
           submit_pipeline_run(base_url(runtime, opts), credentials.service_token, session_context, target),
         {:ok, final_run} <- maybe_wait(run, runtime, credentials.service_token, session_context, opts),
         :ok <- ensure_success(final_run, Keyword.get(opts, :wait, true)) do
      {:ok, final_run}
    end
  end

  def pipeline(_pipeline_module, _opts), do: {:error, :invalid_pipeline}

  defp validate_opts(opts) do
    case validate_positive_integer(opts, :timeout_ms) do
      :ok -> validate_positive_integer(opts, :poll_interval_ms)
      {:error, _reason} = error -> error
    end
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
    with {:ok, runtime} <- State.read_runtime(opts),
         {:ok, secrets} <- State.read_secrets(opts) do
      {:ok, runtime, secrets}
    end
  end

  defp local_credentials(secrets) do
    with token when is_binary(token) and token != "" <- secrets["service_token"],
         username when is_binary(username) and username != "" <- secrets["local_operator_username"],
         password when is_binary(password) and password != "" <- secrets["local_operator_password"] do
      {:ok, %{service_token: token, username: username, password: password}}
    else
      _other -> {:error, :missing_local_operator_credentials}
    end
  end

  defp base_url(runtime, opts) do
    runtime["orchestrator_base_url"] || Config.resolve(opts).orchestrator_base_url
  end

  defp submit_pipeline_run(base_url, service_token, session_context, target) do
    case target do
      %{"target_id" => target_id} when is_binary(target_id) and target_id != "" ->
        OrchestratorClient.submit_run(base_url, service_token, session_context, %{
          target: %{type: "pipeline", id: target_id},
          manifest_selection: %{mode: "active"}
        })

      _other ->
        {:error, :invalid_pipeline_target}
    end
  end

  defp maybe_wait(run, runtime, service_token, session_context, opts) do
    case {Keyword.get(opts, :wait, true), run} do
      {false, _run} ->
        {:ok, run}

      {true, %{"id" => run_id}} when is_binary(run_id) and run_id != "" ->
        timeout_ms = Keyword.get(opts, :timeout_ms, @default_timeout_ms)
        poll_interval_ms = Keyword.get(opts, :poll_interval_ms, @default_poll_interval_ms)
        deadline = System.monotonic_time(:millisecond) + timeout_ms

        wait_for_run(run, run_id, runtime, service_token, session_context, deadline, poll_interval_ms, opts)

      _other ->
        {:error, :invalid_run_response}
    end
  end

  defp wait_for_run(run, run_id, runtime, service_token, session_context, deadline, poll_interval_ms, opts) do
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
          wait_for_run(next_run, run_id, runtime, service_token, session_context, deadline, poll_interval_ms, opts)
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
  defp normalize_pipeline_name(pipeline_module) when is_atom(pipeline_module), do: inspect(pipeline_module)
  defp normalize_pipeline_name(pipeline_module) when is_binary(pipeline_module), do: String.trim(pipeline_module)

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
