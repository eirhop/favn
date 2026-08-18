defmodule Favn.CLI.ActivationReconciler do
  @moduledoc false

  alias Favn.CLI.ActivationOptions

  @poll_interval_ms 100

  @spec run(ActivationOptions.t(), String.t(), String.t(), (keyword() -> term())) ::
          {:ok, map()} | {:error, {:activation_outcome_unknown, map()}}
  def run(%ActivationOptions{} = options, manifest_version_id, workspace_id, read_active)
      when is_binary(manifest_version_id) and is_binary(workspace_id) and
             is_function(read_active, 1) do
    deadline = System.monotonic_time(:millisecond) + options.reconcile_timeout_ms

    poll(
      options,
      manifest_version_id,
      workspace_id,
      read_active,
      deadline,
      :request_timed_out
    )
  end

  defp poll(options, manifest_version_id, workspace_id, read_active, deadline, last_observation) do
    remaining_ms = deadline - System.monotonic_time(:millisecond)

    if remaining_ms > 0 do
      request_options = [
        connect_timeout_ms: min(remaining_ms, 1_000),
        timeout_ms: min(remaining_ms, 2_000),
        total_timeout_ms: remaining_ms
      ]

      case observation(read_active.(request_options), manifest_version_id) do
        {:active, runner_releases} ->
          reconciled_success(options, manifest_version_id, runner_releases)

        {:pending, observation} ->
          sleep_ms = min(deadline - System.monotonic_time(:millisecond), @poll_interval_ms)

          if sleep_ms > 0 do
            Process.sleep(sleep_ms)
          end

          poll(
            options,
            manifest_version_id,
            workspace_id,
            read_active,
            deadline,
            observation
          )
      end
    else
      unknown_outcome(options, manifest_version_id, workspace_id, last_observation)
    end
  end

  defp observation(
         {:ok,
          %{
            "data" => %{
              "manifest" => %{
                "manifest_version_id" => manifest_version_id,
                "runner_releases" => runner_releases
              }
            }
          }},
         manifest_version_id
       )
       when is_map(runner_releases),
       do: {:active, runner_releases}

  defp observation(
         {:ok, %{"data" => %{"manifest" => %{"manifest_version_id" => manifest_version_id}}}},
         _requested_manifest_version_id
       )
       when is_binary(manifest_version_id),
       do: {:pending, :different_manifest_active}

  defp observation({:error, %{reason: {:http_error, 404, _summary}}}, _manifest_version_id),
    do: {:pending, :active_manifest_not_set}

  defp observation({:error, %{reason: {:timeout, :request}}}, _manifest_version_id),
    do: {:pending, :reconciliation_timed_out}

  defp observation(_other, _manifest_version_id),
    do: {:pending, :reconciliation_unavailable}

  defp reconciled_success(options, manifest_version_id, runner_releases) do
    {:ok,
     %{
       "data" => %{
         "activated" => true,
         "manifest_version_id" => manifest_version_id,
         "operation_id" => options.operation_id,
         "reconciled" => true,
         "runner_releases" => runner_releases
       }
     }}
  end

  defp unknown_outcome(options, manifest_version_id, workspace_id, last_observation) do
    {:error,
     {:activation_outcome_unknown,
      %{
        manifest_version_id: manifest_version_id,
        workspace_id: workspace_id,
        operation_id: options.operation_id,
        request_timeout_ms: options.timeout_ms,
        reconcile_timeout_ms: options.reconcile_timeout_ms,
        last_observation: last_observation,
        retry_safe?: true,
        retry_guidance: "retry with the same operation_id"
      }}}
  end
end
