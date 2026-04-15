defmodule Favn.Pipeline.Definition do
  @moduledoc """
  Canonical code-level pipeline definition produced by `use Favn.Pipeline`.
  """

  alias Favn.Ref

  @type selector ::
          {:asset, Ref.t()}
          | {:module, module()}
          | {:tag, atom() | String.t()}
          | {:category, atom() | String.t()}

  @type selection_mode :: :shorthand | :select | nil
  @type schedule_clause ::
          {:ref, Favn.Triggers.Schedule.ref()}
          | {:inline, Favn.Triggers.Schedule.unresolved_t()}
          | nil

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          selectors: [selector()],
          selection_mode: selection_mode(),
          deps: Favn.dependencies_mode(),
          config: map(),
          meta: map(),
          schedule: schedule_clause(),
          window: atom() | nil,
          source: atom() | nil,
          outputs: [atom()]
        }

  defstruct module: nil,
            name: nil,
            selectors: [],
            selection_mode: nil,
            deps: :all,
            config: %{},
            meta: %{},
            schedule: nil,
            window: nil,
            source: nil,
            outputs: []
end
