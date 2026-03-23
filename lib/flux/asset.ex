defmodule Flux.Asset do
  @moduledoc """
  Canonical asset metadata captured from an authored Flux asset.

  `Flux.Asset` is the normalized shape used by the rest of Flux for
  introspection, dependency resolution, and execution planning.

  This module owns validation of the final canonical asset shape after the DSL
  has normalized authoring-friendly input into runtime-ready values.
  """

  alias Flux.Ref

  @typedoc """
  Supported asset kinds.

  These kinds describe how an asset should be treated by the runtime.
  """
  @type kind :: :materialized | :view | :ephemeral

  @typedoc """
  Tag attached to an asset.
  """
  @type tag :: atom() | String.t()

  @type t :: %__MODULE__{
          module: module(),
          name: atom(),
          ref: Ref.t(),
          arity: non_neg_integer(),
          doc: String.t() | nil,
          file: String.t(),
          line: pos_integer(),
          kind: kind(),
          tags: [tag()],
          depends_on: [Ref.t()]
        }

  @valid_kinds [:materialized, :view, :ephemeral]

  defstruct [
    :module,
    :name,
    :ref,
    :arity,
    :doc,
    :file,
    :line,
    kind: :materialized,
    tags: [],
    depends_on: []
  ]

  @doc """
  Validate a canonical `%Flux.Asset{}`.

  This function expects an already-built asset struct. In particular,
  `depends_on` must already be a list of `Flux.Ref.t()` values.

  ## Raises

    * `ArgumentError` when `kind` is not supported
    * `ArgumentError` when `tags` is not a list of atoms or strings
    * `ArgumentError` when `depends_on` is not a list of canonical refs
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = asset) do
    validate_kind!(asset.kind)
    validate_tags!(asset.tags)
    validate_depends_on!(asset.depends_on)

    asset
  end

  @spec valid_kinds() :: [kind()]
  def valid_kinds, do: @valid_kinds

  defp validate_kind!(kind) do
    if kind in @valid_kinds do
      :ok
    else
      raise ArgumentError,
            "invalid asset kind #{inspect(kind)}; expected one of #{inspect(@valid_kinds)}"
    end
  end

  defp validate_tags!(tags) when is_list(tags) do
    Enum.each(tags, fn
      tag when is_atom(tag) or is_binary(tag) ->
        :ok

      tag ->
        raise ArgumentError,
              "asset tags must be atoms or strings, got: #{inspect(tag)}"
    end)
  end

  defp validate_tags!(tags) do
    raise ArgumentError, "asset tags must be a list of atoms or strings, got: #{inspect(tags)}"
  end

  defp validate_depends_on!(depends_on) when is_list(depends_on) do
    Enum.each(depends_on, fn
      {module, name} when is_atom(module) and is_atom(name) ->
        :ok

      dependency ->
        raise ArgumentError,
              "asset depends_on must be a list of Flux.Ref values, got: #{inspect(dependency)}"
    end)
  end

  defp validate_depends_on!(depends_on) do
    raise ArgumentError,
          "asset depends_on must be a list of Flux.Ref values, got: #{inspect(depends_on)}"
  end
end
