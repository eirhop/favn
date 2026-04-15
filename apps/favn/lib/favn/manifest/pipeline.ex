defmodule Favn.Manifest.Pipeline do
  @moduledoc """
  Manifest entry for one compiled pipeline definition.
  """

  alias Favn.Pipeline.Definition

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          definition: Definition.t()
        }

  defstruct [:module, :name, :definition]

  @spec from_definition(Definition.t()) :: t()
  def from_definition(%Definition{} = definition) do
    %__MODULE__{module: definition.module, name: definition.name, definition: definition}
  end
end
