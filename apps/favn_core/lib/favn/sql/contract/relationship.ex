defmodule Favn.SQL.Contract.Relationship do
  @moduledoc """
  Ordered foreign-key claim from one SQL output contract to a dependency grain.

  Checks validate the publication snapshot; they do not install a permanent
  database foreign key. Nullable composite keys must be entirely null or complete.
  """
  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Grain

  @enforce_keys [:name, :target, :on, :cardinality, :on_violation]
  defstruct [:name, :target, :on, :cardinality, :on_violation]

  @type t :: %__MODULE__{
          name: atom(),
          target: Favn.Ref.t(),
          on: keyword(atom()),
          cardinality: :many_to_one | :one_to_one,
          on_violation: :fail | :warn
        }

  @doc "Builds a bounded, explicit relationship claim."
  @spec new!(map() | keyword()) :: t()
  def new!(%__MODULE__{} = value), do: validate!(value)
  def new!(fields), do: fields |> then(&struct!(__MODULE__, &1)) |> validate!()

  @doc "Validates a relationship's shape."
  @spec validate!(t()) :: t()
  def validate!(
        %__MODULE__{
          name: name,
          target: {module, :asset},
          on: pairs,
          cardinality: cardinality,
          on_violation: policy
        } = value
      )
      when is_atom(name) and not is_nil(name) and is_atom(module) and not is_nil(module) and
             cardinality in [:many_to_one, :one_to_one] and policy in [:fail, :warn] do
    unless is_list(pairs) and pairs != [] and length(pairs) <= 1_000 and
             Keyword.keyword?(pairs) and
             Enum.all?(pairs, fn {a, b} -> is_atom(a) and is_atom(b) and not is_nil(b) end) and
             length(Enum.uniq(Keyword.keys(pairs))) == length(pairs) and
             length(Enum.uniq(Keyword.values(pairs))) == length(pairs),
           do:
             raise(
               ArgumentError,
               "relationship on: requires distinct ordered source and target columns"
             )

    value
  end

  def validate!(_),
    do:
      raise(
        ArgumentError,
        "invalid relationship; require role, SQL asset target, on:, cardinality:, and on_violation:"
      )

  @doc "Validates local keys and consistent composite-key nullability."
  @spec validate_source!(t(), Contract.t()) :: t()
  def validate_source!(relationship, contract) do
    relationship = validate!(relationship)
    columns = Map.new(contract.columns, &{&1.name, &1})

    keys =
      Enum.map(relationship.on, fn {local, _} ->
        Map.get(columns, local) ||
          raise(ArgumentError, "unknown relationship source column #{inspect(local)}")
      end)

    if length(Enum.uniq(Enum.map(keys, & &1.nullable?))) != 1,
      do:
        raise(
          ArgumentError,
          "relationship composite columns must be entirely nullable or required"
        )

    relationship
  end

  @doc "Validates the complete target grain and matching logical key types."
  @spec validate_target!(t(), Contract.t(), Contract.t() | nil) :: t()
  def validate_target!(
        relationship,
        source,
        %Contract{grain: %Grain{by: [_ | _] = grain}} = target
      ) do
    unless Keyword.values(relationship.on) == grain,
      do: raise(ArgumentError, "relationship must map the complete target grain in grain order")

    source_columns = Map.new(source.columns, &{&1.name, &1.type})
    target_columns = Map.new(target.columns, &{&1.name, &1.type})

    Enum.each(relationship.on, fn {local, remote} ->
      unless Map.fetch!(source_columns, local) == Map.fetch!(target_columns, remote),
        do: raise(ArgumentError, "relationship key types must match")
    end)

    relationship
  end

  def validate_target!(_, _, _),
    do: raise(ArgumentError, "relationship target requires a structured contract grain")

  @doc "Returns grouped before/after SQL check claims using normal asset references."
  @spec check_specs(t()) :: [Contract.check_spec()]
  def check_specs(%__MODULE__{target: {module, :asset}} = r) do
    target = inspect(module)
    join = Enum.map_join(r.on, " AND ", fn {a, b} -> "s.#{quote_id(a)} = t.#{quote_id(b)}" end)
    any = Enum.map_join(r.on, " OR ", fn {a, _} -> "s.#{quote_id(a)} IS NOT NULL" end)
    remote = Enum.map_join(r.on, ", ", fn {_, b} -> quote_id(b) end)

    before_sql =
      "SELECT count(*) = 0 AS passed, count(*) AS invalid_rows FROM (" <>
        "SELECT 1 FROM query() AS s WHERE (#{any}) AND NOT EXISTS (SELECT 1 FROM #{target} AS t WHERE #{join}) " <>
        "UNION ALL SELECT 1 FROM #{target} GROUP BY #{remote} HAVING count(*) > 1) AS invalid_relationship"

    before = spec(r, "reference", :before_materialize, before_sql)

    if r.cardinality == :one_to_one do
      local = Enum.map_join(r.on, ", ", fn {a, _} -> "s.#{quote_id(a)}" end)

      sql =
        "SELECT count(*) = 0 AS passed, count(*) AS invalid_rows FROM (SELECT 1 FROM target() AS s WHERE (#{any}) GROUP BY #{local} HAVING count(*) > 1) AS duplicate_relationship"

      [before, spec(r, "unique", :after_materialize, sql)]
    else
      [before]
    end
  end

  defp spec(r, suffix, at, sql) do
    id = "relationship.#{r.name}.#{suffix}"
    digest = :crypto.hash(:sha256, id) |> Base.encode16(case: :lower) |> binary_part(0, 16)

    %{
      name: String.to_atom("contract_#{digest}"),
      claim_id: id,
      at: at,
      on_violation: r.on_violation,
      when: nil,
      message: "Contract relationship #{r.name} #{suffix} must hold",
      sql: sql
    }
  end

  defp quote_id(name), do: "\"" <> String.replace(Atom.to_string(name), "\"", "\"\"") <> "\""
end
