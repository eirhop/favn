defmodule FavnView.RunDetailTelemetry do
  @moduledoc false

  @pending_key {__MODULE__, :pending_render}
  @event [:favn, :view, :run_detail, :render]

  @spec begin_render(atom(), :connected, non_neg_integer()) :: :ok
  def begin_render(mode, mount_kind, step_count)
      when is_atom(mode) and mount_kind == :connected and is_integer(step_count) and
             step_count >= 0 do
    Process.put(@pending_key, %{
      mode: mode,
      mount_kind: mount_kind,
      step_count: step_count,
      started_at: System.monotonic_time()
    })

    :ok
  end

  @spec finish_render(non_neg_integer()) :: :ok
  def finish_render(diff_bytes) when is_integer(diff_bytes) and diff_bytes >= 0 do
    case Process.delete(@pending_key) do
      %{started_at: started_at} = pending ->
        :telemetry.execute(
          @event,
          %{
            duration: System.monotonic_time() - started_at,
            diff_bytes: diff_bytes,
            step_count: pending.step_count
          },
          %{mode: pending.mode, mount_kind: pending.mount_kind}
        )

      nil ->
        :ok
    end

    :ok
  end
end
