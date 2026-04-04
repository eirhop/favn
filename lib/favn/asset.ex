defmodule Favn.Asset do
  @moduledoc """
  Canonical asset metadata captured from an authored Favn asset.

  `Favn.Asset` is the normalized shape used by the rest of Favn for
  introspection, dependency resolution, and execution planning.

  This module owns validation of the final canonical asset shape after the DSL
  has normalized authoring-friendly input into runtime-ready values.
  """

  alias Favn.Ref

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          ref: Ref.t(),
          arity: non_neg_integer(),
          title: String.t() | nil,
          doc: String.t() | nil,
          file: String.t(),
          line: pos_integer(),
          meta: map(),
          depends_on: [Ref.t()]
        }

  @typedoc """
  Canonical return shape expected from asset function execution.
  """
  @type return_value :: :ok | {:ok, map() | keyword()} | {:error, term()}

  defstruct [
    :module,
    :name,
    :ref,
    :arity,
    :title,
    :doc,
    :file,
    :line,
    meta: %{},
    depends_on: []
  ]

  @doc """
  Validate a canonical `%Favn.Asset{}`.

  This function expects an already-built asset struct. In particular,
  `depends_on` must already be a list of `Favn.Ref.t()` values.

  ## Raises

    * `ArgumentError` when `meta` is not a map
    * `ArgumentError` when `depends_on` is not a list of canonical refs
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = asset) do
    validate_meta!(asset.meta)
    validate_depends_on!(asset.depends_on)

    asset
  end

  defp validate_meta!(meta) when is_map(meta), do: :ok

  defp validate_meta!(meta),
    do: raise(ArgumentError, "asset meta must be a map, got: #{inspect(meta)}")

  defp validate_depends_on!(depends_on) when is_list(depends_on) do
    Enum.each(depends_on, fn
      {module, name} when is_atom(module) and is_atom(name) ->
        :ok

      dependency ->
        raise ArgumentError,
              "asset depends_on must be a list of Favn.Ref values, got: #{inspect(dependency)}"
    end)
  end

  defp validate_depends_on!(depends_on) do
    raise ArgumentError,
          "asset depends_on must be a list of Favn.Ref values, got: #{inspect(depends_on)}"
  end
end
