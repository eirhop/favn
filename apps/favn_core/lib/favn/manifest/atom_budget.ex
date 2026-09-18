defmodule Favn.Manifest.AtomBudget do
  @moduledoc """
  Preserves VM atom headroom before interning validated manifest identifiers.

  Callers must validate identifier shape, count and trusted manifest ownership
  separately. This check creates no atoms and reserves 100,000 slots for the VM
  and other runtime work; it is a conservative guard, not a global allocation lock.
  """

  @min_headroom 100_000

  @doc "Checks the current VM budget for a collection of unique identifier strings."
  @spec check_headroom(Enumerable.t()) :: :ok | {:error, term()}
  def check_headroom(identifiers) do
    new_count = Enum.count(identifiers, &new_atom?/1)
    count = :erlang.system_info(:atom_count)
    limit = :erlang.system_info(:atom_limit)

    if limit - count - new_count >= @min_headroom,
      do: :ok,
      else: {:error, {:manifest_atom_headroom_exceeded, count, limit, new_count}}
  end

  defp new_atom?(value) do
    _ = String.to_existing_atom(value)
    false
  rescue
    ArgumentError -> true
  end
end
