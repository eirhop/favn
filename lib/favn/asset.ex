defmodule Favn.Asset do
  @moduledoc """
  Canonical asset metadata captured from an authored Favn asset.

  `Favn.Asset` is the normalized shape used by the rest of Favn for
  introspection, dependency resolution, and execution planning.

  This module owns validation of the final canonical asset shape after the DSL
  has normalized authoring-friendly input into runtime-ready values.
  """

  alias Favn.Ref
  alias Favn.Window.Spec

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
          depends_on: [Ref.t()],
          window_spec: Spec.t() | nil
        }

  @typedoc """
  Canonical return shape expected from asset function execution.
  """
  @type return_value :: :ok | {:ok, map()} | {:error, term()}

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
    depends_on: [],
    window_spec: nil
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
    meta = normalize_meta!(asset.meta)
    validate_depends_on!(asset.depends_on)
    validate_window_spec!(asset.window_spec)

    %{asset | meta: meta}
  end

  @doc """
  Normalize and validate authored asset metadata (`@meta`).

  This is for DSL/catalog metadata only and is separate from runtime success
  return metadata, which must be a map.
  """
  @spec normalize_meta!(map() | keyword() | nil) :: map()
  def normalize_meta!(nil), do: %{}

  def normalize_meta!(meta) when is_list(meta) do
    if Keyword.keyword?(meta) do
      normalize_meta!(Map.new(meta))
    else
      raise ArgumentError, "asset meta must be a keyword list or map, got: #{inspect(meta)}"
    end
  end

  def normalize_meta!(meta) when is_map(meta) do
    supported = MapSet.new([:owner, :category, :tags])

    Enum.each(meta, fn
      {:owner, owner} when is_binary(owner) ->
        :ok

      {:owner, value} ->
        raise ArgumentError, "asset meta owner must be a string, got: #{inspect(value)}"

      {:category, category} when is_atom(category) ->
        :ok

      {:category, value} ->
        raise ArgumentError, "asset meta category must be an atom, got: #{inspect(value)}"

      {:tags, tags} when is_list(tags) ->
        Enum.each(tags, fn
          tag when is_atom(tag) or is_binary(tag) ->
            :ok

          tag ->
            raise ArgumentError,
                  "asset meta tags entries must be atoms or strings, got: #{inspect(tag)}"
        end)

      {:tags, value} ->
        raise ArgumentError, "asset meta tags must be a list, got: #{inspect(value)}"

      {key, _value} ->
        if MapSet.member?(supported, key) do
          :ok
        else
          raise ArgumentError,
                "asset meta contains unsupported key #{inspect(key)}; allowed keys: [:owner, :category, :tags]"
        end
    end)

    meta
  end

  def normalize_meta!(meta),
    do: raise(ArgumentError, "asset meta must be a keyword list or map, got: #{inspect(meta)}")

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

  defp validate_window_spec!(nil), do: :ok

  defp validate_window_spec!(%Spec{} = spec) do
    case Spec.validate(spec) do
      :ok -> :ok
      {:error, reason} -> raise ArgumentError, "invalid asset window_spec: #{inspect(reason)}"
    end
  end

  defp validate_window_spec!(value) do
    raise ArgumentError,
          "asset window_spec must be a Favn.Window.Spec or nil, got: #{inspect(value)}"
  end
end
