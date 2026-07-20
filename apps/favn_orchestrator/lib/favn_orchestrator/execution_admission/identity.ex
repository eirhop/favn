defmodule FavnOrchestrator.ExecutionAdmission.Identity do
  @moduledoc false

  @spec lease_id(String.t(), String.t(), non_neg_integer(), pos_integer()) :: String.t()
  def lease_id(run_id, asset_step_id, stage, attempt)
      when is_binary(run_id) and is_binary(asset_step_id) and is_integer(stage) and stage >= 0 and
             is_integer(attempt) and attempt > 0 do
    "execution_lease:" <> digest({run_id, asset_step_id, stage, attempt})
  end

  @spec waiter_id(String.t(), String.t(), non_neg_integer(), pos_integer()) :: String.t()
  def waiter_id(run_id, asset_step_id, stage, attempt)
      when is_binary(run_id) and is_binary(asset_step_id) and is_integer(stage) and stage >= 0 and
             is_integer(attempt) and attempt > 0 do
    "execution_waiter:" <> digest({run_id, asset_step_id, stage, attempt})
  end

  defp digest(identity) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(identity))
    |> Base.url_encode64(padding: false)
  end
end
