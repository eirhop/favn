defmodule FavnOrchestrator.RunServer.PersistenceRetry do
  @moduledoc false

  alias FavnOrchestrator.RunServer.Persistence
  alias FavnOrchestrator.RunState

  @enforce_keys [:run, :event_type, :data, :resume]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          run: RunState.t(),
          event_type: atom(),
          data: map(),
          resume: term()
        }

  @spec new(RunState.t(), atom(), map(), term()) :: t()
  def new(%RunState{} = run, event_type, data, resume)
      when is_atom(event_type) and is_map(data) do
    %__MODULE__{run: run, event_type: event_type, data: data, resume: resume}
  end

  @spec persist(t()) :: :ok | {:error, term()}
  def persist(%__MODULE__{} = retry) do
    Persistence.persist_run_step(retry.run, retry.event_type, retry.data)
  end
end
