defmodule Favn.Dev.Run do
  @moduledoc """
  Local run submission workflow for a running `mix favn.dev` stack.
  """

  alias Favn.Dev.Config
  alias Favn.Dev.LocalContext
  alias Favn.Dev.OrchestratorClient
  alias Favn.Dev.State
  alias Favn.Dev.Status
  alias Favn.Window.Request, as: WindowRequest

  @terminal_statuses ["ok", "error", "cancelled", "timed_out"]
  @dependency_modes ~w(all none)
  @asset_refresh_modes ~w(auto missing force_selected force_selected_upstream force_all)
  @pipeline_refresh_modes ~w(auto missing force_all)
  @default_wait_timeout_ms 60_000
  @default_poll_interval_ms 1_000

  @type run_opts :: [
          root_dir: Path.t(),
          wait: boolean(),
          window: String.t(),
          timezone: String.t(),
          dependencies: String.t(),
          refresh: String.t(),
          idempotency_key: String.t(),
          timeout_ms: non_neg_integer(),
          wait_timeout_ms: pos_integer(),
          run_timeout_ms: pos_integer(),
          poll_interval_ms: pos_integer()
        ]

  @spec submit(module() | String.t(), run_opts()) :: {:ok, map()} | {:error, term()}
  def submit(target, opts \\ [])

  def submit(target, opts)
      when is_atom(target) or is_binary(target) do
    with :ok <- validate_opts(opts),
         {:ok, window_request} <- parse_window_request(opts),
         :ok <- ensure_running(opts),
         {:ok, runtime} <- read_runtime_snapshot(opts),
         credentials = LocalContext.credentials(),
         session_context = LocalContext.session_context(),
         {:ok, active_manifest} <-
           OrchestratorClient.active_manifest(
             base_url(runtime, opts),
             credentials.service_token,
             session_context
           ),
         {:ok, target} <- resolve_run_target(active_manifest, target),
         :ok <- validate_target_opts(target, opts),
         {:ok, run} <-
           submit_run(
             base_url(runtime, opts),
             credentials.service_token,
             session_context,
             target,
             window_request,
             opts
           ),
         {:ok, final_run} <-
           maybe_wait(run, runtime, credentials.service_token, session_context, opts),
         :ok <- ensure_success(final_run, Keyword.get(opts, :wait, true)) do
      {:ok, final_run}
    end
  end

  def submit(_target, _opts), do: {:error, :invalid_target}

  defp validate_opts(opts) do
    with :ok <- validate_choice(opts, :dependencies, @dependency_modes),
         :ok <- validate_choice(opts, :refresh, @asset_refresh_modes),
         :ok <- validate_timezone_without_window(opts),
         :ok <- validate_positive_integer(opts, :timeout_ms),
         :ok <- validate_positive_integer(opts, :wait_timeout_ms),
         :ok <- validate_positive_integer(opts, :run_timeout_ms),
         :ok <- validate_positive_integer(opts, :poll_interval_ms) do
      validate_idempotency_key(opts)
    else
      {:error, _reason} = error -> error
    end
  end

  defp validate_choice(opts, key, choices) do
    case Keyword.fetch(opts, key) do
      :error -> :ok

      {:ok, value} ->
        if value in choices,
          do: :ok,
          else: {:error, {:invalid_option, key, value}}
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

  defp validate_idempotency_key(opts) do
    case Keyword.fetch(opts, :idempotency_key) do
      :error ->
        :ok

      {:ok, key} when is_binary(key) ->
        key = String.trim(key)

        if key != "" and byte_size(key) <= 512 do
          :ok
        else
          {:error, {:invalid_option, :idempotency_key}}
        end

      {:ok, _key} ->
        {:error, {:invalid_option, :idempotency_key}}
    end
  end

  defp validate_target_opts(%{"target_type" => "asset"}, opts) do
    case {Keyword.get(opts, :dependencies), Keyword.get(opts, :refresh)} do
      {"none", "force_selected_upstream"} ->
        {:error, {:refresh_include_upstream_requires_dependencies, :all}}

      _other ->
        :ok
    end
  end

  defp validate_target_opts(%{"target_type" => "pipeline"}, opts) do
    if Keyword.has_key?(opts, :dependencies) do
      {:error, :dependencies_only_supported_for_assets}
    else
      case Keyword.get(opts, :refresh) do
        nil -> :ok
        refresh when refresh in @pipeline_refresh_modes -> :ok
        refresh -> {:error, {:invalid_pipeline_refresh_mode, refresh}}
      end
    end
  end

  defp validate_target_opts(_target, _opts), do: {:error, :invalid_target}

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

  @doc false
  @spec resolve_run_target(map(), module() | String.t()) :: {:ok, map()} | {:error, term()}
  def resolve_run_target(active_manifest, target)
      when is_map(active_manifest) and (is_atom(target) or is_binary(target)) do
    requested = normalize_pipeline_name(target)
    targets = active_manifest["targets"] || %{}
    pipelines = Map.get(targets, "pipelines", [])
    assets = Map.get(targets, "assets", [])

    cond do
      pipeline = Enum.find(pipelines, &pipeline_target_match?(&1, requested)) ->
        {:ok, Map.put(pipeline, "target_type", "pipeline")}

      asset = Enum.find(assets, &asset_target_match?(&1, requested)) ->
        {:ok, Map.put(asset, "target_type", "asset")}

      true ->
        {:error, {:target_not_found, requested, available_target_labels(pipelines, assets)}}
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

  defp submit_run(
         base_url,
         service_token,
         session_context,
         target,
         window_request,
         opts
       ) do
    case target do
      %{"target_id" => target_id, "target_type" => target_type}
      when target_type in ["asset", "pipeline"] and is_binary(target_id) and target_id != "" ->
        payload =
          %{
            target: %{type: target_type, id: target_id},
            manifest_selection: %{mode: "active"}
          }
          |> maybe_put_window(window_request)
          |> maybe_put(:dependencies, Keyword.get(opts, :dependencies))
          |> maybe_put(:refresh, Keyword.get(opts, :refresh))
          |> maybe_put(:timeout_ms, run_timeout_ms(opts))

        case OrchestratorClient.submit_run(base_url, service_token, session_context, payload,
               idempotency_key: run_idempotency_key(opts)
             ) do
          {:ok, _run} = ok -> ok
          {:error, reason} -> {:error, unwrap_submit_error(reason)}
        end

      _other ->
        {:error, :invalid_target}
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

  defp maybe_put(payload, _key, nil), do: payload
  defp maybe_put(payload, key, value), do: Map.put(payload, key, value)

  defp unwrap_submit_error(%{operation: :submit_run, reason: {:http_error, 422, payload}}) do
    case get_in(payload, ["error", "message"]) do
      message when is_binary(message) and message != "" ->
        {:orchestrator_validation_failed, message}

      _other ->
        {:orchestrator_validation_failed, inspect(payload)}
    end
  end

  defp unwrap_submit_error(reason), do: reason

  defp run_idempotency_key(opts) do
    case Keyword.fetch(opts, :idempotency_key) do
      {:ok, key} when is_binary(key) -> String.trim(key)
      :error -> fresh_run_idempotency_key()
    end
  end

  defp fresh_run_idempotency_key do
    "favn-local-run-" <> Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
  end

  defp run_timeout_ms(opts),
    do: Keyword.get(opts, :run_timeout_ms, Keyword.get(opts, :timeout_ms))

  defp wait_timeout_ms(opts),
    do:
      Keyword.get(
        opts,
        :wait_timeout_ms,
        Keyword.get(opts, :timeout_ms, @default_wait_timeout_ms)
      )

  defp maybe_wait(run, runtime, service_token, session_context, opts) do
    case {Keyword.get(opts, :wait, true), run} do
      {false, _run} ->
        {:ok, run}

      {true, %{"id" => run_id}} when is_binary(run_id) and run_id != "" ->
        timeout_ms = wait_timeout_ms(opts)
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
        {:error, {:run_wait_timeout, run_id, wait_timeout_ms(opts)}}
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

  defp available_target_labels(pipelines, assets) do
    asset_labels =
      Enum.flat_map(assets, fn asset ->
        [asset["asset_ref"], asset["target_id"], asset["label"]]
        |> Enum.filter(&(is_binary(&1) and &1 != ""))
      end)

    pipelines
    |> available_pipeline_labels()
    |> Kernel.++(asset_labels)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp asset_target_match?(asset, requested) when is_map(asset) do
    requested in [asset["asset_ref"], asset["target_id"], asset["label"]]
  end

  defp asset_target_match?(_asset, _requested), do: false
end
