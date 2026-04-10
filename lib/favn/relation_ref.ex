defmodule Favn.RelationRef do
  @moduledoc """
  Shared canonical relation identity for produced asset ownership and SQL-facing
  relation references.
  """

  @enforce_keys [:name]
  defstruct [:connection, :catalog, :schema, :name]

  @type t :: %__MODULE__{
          connection: atom() | nil,
          catalog: binary() | nil,
          schema: binary() | nil,
          name: binary()
        }

  @type input :: %__MODULE__{} | map() | keyword()

  @doc """
  Normalize a relation reference into the canonical `%Favn.RelationRef{}` shape.
  """
  @spec new!(input()) :: t()
  def new!(%__MODULE__{} = ref), do: validate!(ref)

  def new!(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      attrs |> Map.new() |> new!()
    else
      raise ArgumentError,
            "relation ref must be a keyword list, map, or %Favn.RelationRef{}, got: #{inspect(attrs)}"
    end
  end

  def new!(attrs) when is_map(attrs) do
    normalized = normalize_keys!(attrs)

    ref = %__MODULE__{
      connection: normalize_connection!(Map.get(normalized, :connection)),
      catalog: normalize_optional_identifier!(Map.get(normalized, :catalog), :catalog),
      schema: normalize_optional_identifier!(Map.get(normalized, :schema), :schema),
      name: normalize_required_name!(Map.get(normalized, :name))
    }

    validate!(ref)
  end

  def new!(attrs) do
    raise ArgumentError,
          "relation ref must be a keyword list, map, or %Favn.RelationRef{}, got: #{inspect(attrs)}"
  end

  @doc """
  Validate an existing `%Favn.RelationRef{}`.
  """
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = ref) do
    _ = normalize_connection!(ref.connection)
    _ = normalize_optional_identifier!(ref.catalog, :catalog)
    _ = normalize_optional_identifier!(ref.schema, :schema)
    _ = normalize_required_name!(ref.name)
    ref
  end

  defp normalize_keys!(attrs) do
    attrs
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      canonical_key = normalize_key!(key)

      if Map.has_key?(acc, canonical_key) do
        raise ArgumentError,
              "relation ref received duplicate values for canonical key #{inspect(canonical_key)}"
      else
        Map.put(acc, canonical_key, value)
      end
    end)
  end

  defp normalize_key!(:database), do: :catalog
  defp normalize_key!("database"), do: :catalog
  defp normalize_key!(:table), do: :name
  defp normalize_key!("table"), do: :name
  defp normalize_key!(key) when key in [:connection, :catalog, :schema, :name], do: key
  defp normalize_key!("connection"), do: :connection
  defp normalize_key!("catalog"), do: :catalog
  defp normalize_key!("schema"), do: :schema
  defp normalize_key!("name"), do: :name

  defp normalize_key!(key) do
    raise ArgumentError,
          "relation ref contains unsupported key #{inspect(key)}; allowed keys: [:connection, :catalog, :schema, :name, :database, :table]"
  end

  defp normalize_connection!(nil), do: nil
  defp normalize_connection!(connection) when is_atom(connection), do: connection

  defp normalize_connection!(value) do
    raise ArgumentError, "relation ref connection must be an atom or nil, got: #{inspect(value)}"
  end

  defp normalize_required_name!(value), do: normalize_identifier(value, :name)

  defp normalize_optional_identifier!(nil, _field), do: nil
  defp normalize_optional_identifier!(value, field), do: normalize_identifier(value, field)

  defp normalize_identifier(value, _field) when is_binary(value), do: value
  defp normalize_identifier(value, _field) when is_atom(value), do: Atom.to_string(value)

  defp normalize_identifier(nil, :name) do
    raise ArgumentError, "relation ref name is required"
  end

  defp normalize_identifier(value, field) do
    raise ArgumentError,
          "relation ref #{field} must be an atom, string, or nil, got: #{inspect(value)}"
  end
end
