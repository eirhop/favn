defmodule Favn.RuntimeConfig.Bundle do
  @moduledoc """
  An unresolved, reusable set of asset runtime configuration requirements.

  Bundles contain only `Favn.RuntimeConfig.Ref` values and declaration
  provenance. They never read or retain resolved runtime values.
  """

  alias Favn.RuntimeConfig.Requirements

  @enforce_keys [:declarations, :origin]
  defstruct [:name, :declarations, :origin]

  @type origin :: %{
          required(:module) => module(),
          required(:file) => String.t(),
          required(:line) => pos_integer()
        }

  @type t :: %__MODULE__{
          name: atom(),
          declarations: Requirements.declarations(),
          origin: origin()
        }

  @doc """
  Builds and validates a named bundle for one runtime configuration scope.
  """
  @spec new!(atom(), keyword() | map(), keyword()) :: t()
  def new!(name, fields, opts) when is_atom(name) and is_list(opts) do
    origin = origin!(opts)

    %__MODULE__{
      name: name,
      declarations: Requirements.normalize!(%{name => fields}),
      origin: origin
    }
  end

  def new!(_name, _fields, _opts) do
    raise ArgumentError, "runtime config bundle name must be an atom"
  end

  @doc """
  Builds and validates an inline declaration for one runtime configuration scope.
  """
  @spec inline!(atom(), keyword() | map(), keyword()) :: t()
  def inline!(scope, fields, opts), do: new!(scope, fields, opts)

  @doc """
  Validates a bundle returned by authoring code.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{name: name, declarations: declarations, origin: origin} = bundle)
      when is_atom(name) do
    %{
      bundle
      | declarations: Requirements.normalize!(declarations),
        origin: validate_origin!(origin)
    }
  end

  def validate!(_other) do
    raise ArgumentError, "runtime_config/1 expects a Favn.RuntimeConfig.Bundle"
  end

  defp origin!(opts) do
    opts
    |> Map.new()
    |> validate_origin!()
  end

  defp validate_origin!(%{module: module, file: file, line: line} = origin)
       when is_atom(module) and is_binary(file) and is_integer(line) and line > 0 do
    Map.take(origin, [:module, :file, :line])
  end

  defp validate_origin!(_origin) do
    raise ArgumentError,
          "runtime config declaration origin must contain module, file, and line"
  end
end
