defmodule Favn.TargetCompatibility.PhysicalFingerprint do
  @moduledoc """
  Canonical physical-schema fingerprint produced from runner relation inspection.

  Only physical compatibility inputs are retained: adapter identity, discovered
  relation identity and kind, and ordered column name/type/nullability. Row
  counts, samples, defaults, comments, and adapter metadata do not affect the
  fingerprint.
  """

  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Manifest.Serializer
  alias Favn.Manifest.TargetDescriptor
  alias Favn.SQL.Contract

  @schema_version 1

  @type relation :: %{
          required(:catalog) => String.t() | nil,
          required(:schema) => String.t() | nil,
          required(:name) => String.t(),
          required(:kind) => String.t()
        }

  @type column :: %{
          required(:name) => String.t(),
          required(:native_type) => String.t() | nil,
          required(:logical_type) => String.t(),
          required(:nullable) => boolean() | nil
        }

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          adapter: String.t(),
          relation: relation(),
          columns: [column()],
          fingerprint: String.t()
        }

  @enforce_keys [:adapter, :relation, :columns, :fingerprint]
  defstruct [:adapter, :relation, :columns, :fingerprint, schema_version: @schema_version]

  @doc """
  Builds a fingerprint from an existing relation inspection result.

  `:not_found` is returned only when the runner inspection authoritatively found
  no relation. Failed relation or column inspection remains an explicit error.
  """
  @spec from_inspection(RelationInspectionResult.t()) ::
          {:ok, t() | :not_found} | {:error, term()}
  def from_inspection(%RelationInspectionResult{error: error}) when not is_nil(error),
    do: {:error, :relation_inspection_failed}

  def from_inspection(%RelationInspectionResult{} = result) do
    cond do
      warning?(result, :relation_failed) ->
        {:error, :relation_inspection_failed}

      is_nil(result.relation) ->
        {:ok, :not_found}

      warning?(result, :columns_failed) ->
        {:error, :column_inspection_failed}

      true ->
        new(adapter: result.adapter, relation: result.relation, columns: result.columns)
    end
  end

  @doc "Builds a canonical physical fingerprint from normalized inspection inputs."
  @spec new(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_list(attrs) or is_map(attrs) do
    attrs = Map.new(attrs)

    with {:ok, adapter} <- normalize_adapter(Map.get(attrs, :adapter)),
         {:ok, relation} <- normalize_relation(Map.get(attrs, :relation)),
         {:ok, columns} <- normalize_columns(Map.get(attrs, :columns)) do
      value = %__MODULE__{
        adapter: adapter,
        relation: relation,
        columns: columns,
        fingerprint: ""
      }

      {:ok, %{value | fingerprint: fingerprint(value)}}
    end
  end

  def new(_attrs), do: {:error, :invalid_physical_fingerprint}

  @doc """
  Returns adapter or relation-identity differences from the desired target.

  An omitted catalog or schema in the logical target accepts the concrete
  default namespace reported by the adapter. Explicit namespaces still match
  exactly.
  """
  @spec identity_diff(TargetDescriptor.t(), t()) :: [map()]
  def identity_diff(%TargetDescriptor{} = desired, %__MODULE__{} = observed) do
    desired_relation = Map.take(desired.relation, [:catalog, :schema, :name])
    observed_relation = Map.take(observed.relation, [:catalog, :schema, :name])

    [
      identity_difference(:adapter, desired.adapter, observed.adapter),
      relation_identity_difference(desired_relation, observed_relation),
      identity_difference(:relation_kind, "table", observed.relation.kind),
      identity_difference(
        :contract_fingerprint,
        desired.contract_fingerprint,
        observed_contract_fingerprint(desired.contract_fingerprint, observed.columns)
      )
    ]
    |> List.flatten()
  end

  defp relation_identity_difference(desired, observed) do
    if desired.name == observed.name and
         namespace_matches?(desired.catalog, observed.catalog) and
         namespace_matches?(desired.schema, observed.schema),
       do: [],
       else: identity_difference(:relation, desired, observed)
  end

  defp namespace_matches?(nil, _observed), do: true
  defp namespace_matches?(value, value), do: true
  defp namespace_matches?(_desired, _observed), do: false

  defp identity_difference(_field, value, value), do: []

  defp identity_difference(field, desired, observed),
    do: [%{field: field, desired: desired, observed: observed}]

  defp observed_contract_fingerprint(nil, _columns), do: nil

  defp observed_contract_fingerprint(_desired_fingerprint, columns) do
    columns
    |> Enum.map(fn column ->
      %{
        name: column.name,
        type: column.logical_type,
        nullable: column.nullable
      }
    end)
    |> Serializer.encode_canonical!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp normalize_adapter(adapter) when is_atom(adapter) and not is_nil(adapter),
    do: {:ok, Atom.to_string(adapter)}

  defp normalize_adapter(adapter) when is_binary(adapter) and adapter != "", do: {:ok, adapter}
  defp normalize_adapter(_adapter), do: {:error, :invalid_physical_adapter}

  defp normalize_relation(relation) when is_map(relation) do
    name = field(relation, :name)
    kind = field(relation, :type)

    if valid_identifier?(name) and valid_identifier?(kind) do
      {:ok,
       %{
         catalog: optional_identifier(field(relation, :catalog)),
         schema: optional_identifier(field(relation, :schema)),
         name: to_string(name),
         kind: identifier(kind)
       }}
    else
      {:error, :invalid_physical_relation}
    end
  end

  defp normalize_relation(_relation), do: {:error, :invalid_physical_relation}

  defp normalize_columns(columns) when is_list(columns) do
    cond do
      length(columns) > Contract.max_columns() ->
        {:error, :physical_column_limit_exceeded}

      true ->
        Enum.reduce_while(columns, {:ok, []}, fn column, {:ok, acc} ->
          case normalize_column(column) do
            {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> then(fn
          {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
          {:error, _reason} = error -> error
        end)
    end
  end

  defp normalize_columns(_columns), do: {:error, :invalid_physical_columns}

  defp normalize_column(column) when is_map(column) do
    name = field(column, :name)
    data_type = field(column, :data_type)
    nullable = field(column, :nullable?)

    if valid_identifier?(name) and optional_data_type?(data_type) and
         (is_boolean(nullable) or is_nil(nullable)) do
      native_type = normalize_native_type(data_type)

      {:ok,
       %{
         name: to_string(name),
         native_type: native_type,
         logical_type: native_type |> Contract.normalize_observed_type() |> Atom.to_string(),
         nullable: nullable
       }}
    else
      {:error, :invalid_physical_column}
    end
  end

  defp normalize_column(_column), do: {:error, :invalid_physical_column}

  defp fingerprint(value) do
    value
    |> Map.from_struct()
    |> Map.delete(:fingerprint)
    |> Serializer.encode_manifest!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp warning?(result, code) do
    Enum.any?(result.warnings, fn warning ->
      field(warning, :code) in [code, Atom.to_string(code)]
    end)
  end

  defp normalize_native_type(nil), do: nil

  defp normalize_native_type(type) do
    type
    |> to_string()
    |> String.trim()
    |> String.upcase()
    |> String.replace(~r/\s+/, " ")
  end

  defp optional_identifier(nil), do: nil
  defp optional_identifier(value), do: to_string(value)
  defp identifier(value) when is_atom(value), do: Atom.to_string(value)
  defp identifier(value), do: value

  defp valid_identifier?(value),
    do: (is_atom(value) and not is_nil(value)) or optional_identifier?(value)

  defp optional_identifier?(value), do: is_binary(value) and value != ""
  defp optional_data_type?(nil), do: true
  defp optional_data_type?(value), do: optional_identifier?(value)

  defp field(value, key),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key)))
end
