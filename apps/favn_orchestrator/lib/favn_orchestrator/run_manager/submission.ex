defmodule FavnOrchestrator.RunManager.Submission do
  @moduledoc false

  alias FavnOrchestrator.RunState

  @type submit_kind :: :manual | :pipeline | :rerun

  @type t :: %__MODULE__{
          run_state: RunState.t(),
          manifest_version: term(),
          submit_kind: submit_kind(),
          transition_metadata: map(),
          event_metadata: map()
        }

  defstruct [
    :run_state,
    :manifest_version,
    :submit_kind,
    transition_metadata: %{},
    event_metadata: %{}
  ]
end
