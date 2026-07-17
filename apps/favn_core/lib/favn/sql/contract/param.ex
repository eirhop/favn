defmodule Favn.SQL.Contract.Param do
  @moduledoc """
  Typed reference to one runtime-bound SQL contract parameter.

  Contract parameters reuse the normal SQL parameter binding path and retain
  only the parameter name in the manifest. Asset settings or runtime params
  provide the value before SQL execution.
  """

  alias Favn.SQL.Template

  @enforce_keys [:name]
  defstruct [:name]

  @type t :: %__MODULE__{name: atom()}

  @doc "Builds and validates a contract parameter reference."
  @spec new!(atom()) :: t()
  def new!(name), do: %__MODULE__{name: name} |> validate!()

  @doc "Validates a compiled or rehydrated contract parameter reference."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{name: name} = param) do
    unless is_atom(name) and not is_nil(name),
      do: raise(ArgumentError, "contract param name must be a non-nil atom")

    unless Regex.match?(~r/^[a-z][A-Za-z0-9_]*$/, Atom.to_string(name)),
      do:
        raise(
          ArgumentError,
          "contract param name must start with a lowercase letter and contain only letters, digits, or underscores"
        )

    if name in Template.reserved_runtime_inputs(),
      do: raise(ArgumentError, "contract param @#{name} is reserved for Favn runtime input")

    param
  end
end
