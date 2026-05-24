defmodule FavnOrchestrator.ExecutionAdmission.LeaseRelease do
  @moduledoc """
  Result returned after releasing execution admission leases for a run.

  Storage adapters return the scopes that were freed so the admission coordinator
  can wake only waiters that may now make progress.
  """

  @enforce_keys [:run_id, :released_count, :scopes]
  defstruct [:run_id, :released_count, :scopes]

  @type t :: %__MODULE__{
          run_id: String.t(),
          released_count: non_neg_integer(),
          scopes: [map()]
        }

  @doc """
  Builds a lease release result.
  """
  @spec new(String.t(), non_neg_integer(), [map()]) :: t()
  def new(run_id, released_count, scopes)
      when is_binary(run_id) and is_integer(released_count) and released_count >= 0 and
             is_list(scopes) do
    %__MODULE__{run_id: run_id, released_count: released_count, scopes: scopes}
  end
end
