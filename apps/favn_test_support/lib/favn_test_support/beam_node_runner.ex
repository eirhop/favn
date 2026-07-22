defmodule FavnTestSupport.BeamNodeRunner do
  @moduledoc false

  def diagnostics(opts), do: {:ok, Keyword.fetch!(opts, :peer_diagnostics)}

  def await_result(_execution_id, _timeout, opts) do
    Process.sleep(Keyword.get(opts, :peer_delay_ms, 0))
    {:error, :not_found}
  end

  def cancel_work(_execution_id, _reason, opts) do
    raise Keyword.get(opts, :peer_exception_message, "peer runner failure")
  end
end
