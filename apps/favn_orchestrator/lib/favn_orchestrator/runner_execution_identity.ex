defmodule FavnOrchestrator.RunnerExecutionIdentity do
  @moduledoc """
  Builds deterministic, bounded identities for runner execution attempts.

  The readable prefixes are diagnostic hints only. A SHA-256 digest of the
  complete `{run_id, asset_step_id, attempt}` tuple provides the identity.
  """

  alias FavnOrchestrator.Persistence.Identity

  @prefix_bytes 32

  @doc "Builds one bounded runner execution identity."
  @spec build(String.t(), String.t(), pos_integer() | nil) :: String.t()
  def build(run_id, asset_step_id, attempt)
      when is_binary(run_id) and is_binary(asset_step_id) and
             (is_nil(attempt) or (is_integer(attempt) and attempt > 0)) do
    attempt = attempt || 1

    digest =
      {run_id, asset_step_id, attempt}
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    identity =
      Enum.join(
        ["rex", readable_prefix(run_id), readable_prefix(asset_step_id), digest],
        ":"
      )

    true = byte_size(identity) <= Identity.max_bytes()
    identity
  end

  defp readable_prefix(value) do
    value
    |> binary_part(0, min(byte_size(value), @prefix_bytes))
    |> :binary.bin_to_list()
    |> Enum.map(fn
      byte when byte in ?a..?z -> byte
      byte when byte in ?A..?Z -> byte
      byte when byte in ?0..?9 -> byte
      byte when byte in [?_, ?-] -> byte
      _byte -> ?-
    end)
    |> case do
      [] -> ~c"id"
      bytes -> bytes
    end
    |> List.to_string()
  end
end
