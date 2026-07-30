defmodule FavnView.CommandAttempt do
  @moduledoc """
  Idempotency keys for the mutating commands an operator issues from the browser.

  Every orchestrator command requires a command id, and the control plane treats
  a repeat of one id two ways: an identical request replays the stored result,
  and a changed request is a conflict. The key is therefore a statement about
  intent and cannot be derived from the request, because the same request can be
  meant twice — enabling a schedule, disabling it, and enabling it again are
  three commands whose enable requests are byte-identical.

  So a key is minted once per intent and held in the LiveView until the command
  settles:

    * retrying the same intent reuses the key, so a command that may already
      have completed is replayed instead of repeated
    * a different intent mints a new key
    * a terminal outcome clears the assign, so the next command of that intent
      is a new command and not a replay of the last one. An unknown outcome is
      not terminal: keep the attempt, so the retry replays instead of repeating.

  `phx-disable-with` on the control covers the double-click; this covers the
  retry, where the first outcome is unknown and repeating the write is the one
  thing Favn must not do.

  ## Examples

      iex> attempt = FavnView.CommandAttempt.next(nil, "schedule_activation", {"s1", :enable})
      iex> FavnView.CommandAttempt.next(attempt, "schedule_activation", {"s1", :enable}) == attempt
      true

      iex> attempt = FavnView.CommandAttempt.next(nil, "schedule_activation", {"s1", :enable})
      iex> FavnView.CommandAttempt.next(attempt, "schedule_activation", {"s1", :disable}) == attempt
      false
  """

  @enforce_keys [:key, :intent]
  defstruct [:key, :intent]

  @type t :: %__MODULE__{key: String.t(), intent: term()}

  @doc """
  The attempt to send for `intent`, reusing `attempt` when the intent is unchanged.

  `operation` names the command and appears in the key, so an audit entry says
  which command an operator issued without a lookup. `intent` is any term that
  identifies what the operator asked for: include everything that would make a
  second click a different command, and nothing that merely restates the request.

  ## Examples

      iex> attempt = FavnView.CommandAttempt.next(nil, "rebuild_start", "plan_7")
      iex> String.starts_with?(attempt.key, "rebuild_start:ui:")
      true
  """
  @spec next(t() | nil, String.t(), term()) :: t()
  def next(attempt, operation, intent)

  def next(%__MODULE__{intent: intent} = attempt, _operation, intent), do: attempt

  def next(_attempt, operation, intent) when is_binary(operation) do
    %__MODULE__{key: mint(operation), intent: intent}
  end

  defp mint(operation) do
    operation <> ":ui:" <> Base.encode16(:crypto.strong_rand_bytes(16), case: :lower)
  end
end
