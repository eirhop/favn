defmodule Favn.Manifest.Schedule do
  @moduledoc """
  Manifest entry for one named schedule.
  """

  alias Favn.Triggers.Schedule

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          schedule: Schedule.unresolved_t()
        }

  defstruct [:module, :name, :schedule]
end
