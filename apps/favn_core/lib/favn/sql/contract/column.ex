defmodule Favn.SQL.Contract.Column do
  @moduledoc """
  One ordered output-column claim in a SQL asset contract.

  `type` is a backend-neutral logical type. `nullable?` states the published
  data guarantee, while `sources` and `via` retain explicit column lineage.
  """

  alias Favn.SQL.Contract.Lineage

  @supported_types [
    :boolean,
    :integer,
    :float,
    :decimal,
    :string,
    :binary,
    :date,
    :time,
    :datetime,
    :json,
    :uuid
  ]
  @lineage_modes [nil, :identity, :transformation, :aggregation]

  @enforce_keys [:name, :type, :nullable?]
  defstruct [:name, :type, :nullable?, :description, :renamed_from, :via, tags: [], sources: []]

  @type logical_type ::
          :boolean
          | :integer
          | :float
          | :decimal
          | :string
          | :binary
          | :date
          | :time
          | :datetime
          | :json
          | :uuid

  @type lineage_mode :: :identity | :transformation | :aggregation

  @type t :: %__MODULE__{
          name: atom(),
          type: logical_type(),
          nullable?: boolean(),
          description: String.t() | nil,
          renamed_from: atom() | nil,
          tags: [String.t()],
          sources: [Lineage.t()],
          via: lineage_mode() | nil
        }

  @doc "Builds and validates one column claim."
  @spec new!(atom(), atom(), keyword() | map()) :: t()
  def new!(name, type, opts) when is_list(opts) or is_map(opts) do
    opts = Map.new(opts)

    %__MODULE__{
      name: name,
      type: type,
      nullable?: Map.get(opts, :null, true),
      description: Map.get(opts, :description),
      renamed_from: Map.get(opts, :renamed_from),
      tags: normalize_tags(Map.get(opts, :tags, [])),
      sources: Enum.map(List.wrap(Map.get(opts, :from, [])), &Lineage.new!/1),
      via: Map.get(opts, :via)
    }
    |> validate!()
  end

  @doc "Validates a compiled or rehydrated column claim."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = column) do
    unless is_atom(column.name) and not is_nil(column.name),
      do: raise(ArgumentError, "contract column name must be a non-nil atom")

    unless column.type in @supported_types,
      do:
        raise(
          ArgumentError,
          "unsupported contract column type #{inspect(column.type)}; expected one of #{inspect(@supported_types)}"
        )

    unless is_boolean(column.nullable?),
      do: raise(ArgumentError, "contract column null: must be a boolean")

    unless is_nil(column.description) or
             (is_binary(column.description) and String.trim(column.description) != ""),
           do: raise(ArgumentError, "contract column description must be a non-empty string")

    case column.renamed_from do
      nil -> :ok
      value when is_atom(value) -> :ok
      _value -> raise ArgumentError, "contract column renamed_from: must be a column atom"
    end

    if column.renamed_from == column.name,
      do: raise(ArgumentError, "contract column cannot be renamed from itself")

    unless is_list(column.tags) and Enum.all?(column.tags, &is_binary/1),
      do: raise(ArgumentError, "contract column tags must be atoms or strings")

    unless is_list(column.sources),
      do: raise(ArgumentError, "contract column from: must be a list")

    Enum.each(column.sources, &Lineage.validate!/1)

    unless column.via in @lineage_modes,
      do:
        raise(
          ArgumentError,
          "contract column via: must be :identity, :transformation, or :aggregation"
        )

    if column.via != nil and column.sources == [],
      do: raise(ArgumentError, "contract column via: requires at least one source")

    if column.via == :identity and length(column.sources) != 1,
      do: raise(ArgumentError, "contract column via: :identity requires exactly one source")

    column
  end

  @doc "Returns the canonical logical types accepted by contracts."
  @spec supported_types() :: [logical_type()]
  def supported_types, do: @supported_types

  defp normalize_tags(tags) when is_list(tags) do
    tags
    |> Enum.map(fn
      tag when is_atom(tag) -> Atom.to_string(tag)
      tag when is_binary(tag) and tag != "" -> tag
      tag -> raise ArgumentError, "invalid contract column tag #{inspect(tag)}"
    end)
    |> Enum.uniq()
  end

  defp normalize_tags(other),
    do: raise(ArgumentError, "contract column tags: must be a list, got: #{inspect(other)}")
end
