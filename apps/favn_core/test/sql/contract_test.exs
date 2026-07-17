defmodule Favn.SQL.ContractTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.{Check, Contract, Template}
  alias Favn.SQL.Contract.{Composition, Diff, Fragment, Param}
  alias Favn.SQL.ContractValidation

  test "normalizes common backend types and reports ordered schema differences" do
    contract =
      Contract.new!(%{
        columns: [
          %{name: :id, type: :integer, null: false},
          %{name: :payload, type: :string, null: true}
        ]
      })

    validation =
      ContractValidation.compare(contract, [
        %{name: "payload", data_type: "VARCHAR", nullable?: true},
        %{name: "id", data_type: "BIGINT", nullable?: true}
      ])

    assert validation.status == :failed
    assert [%{kind: :order}] = validation.differences
  end

  test "reports missing, unexpected, and incompatible types" do
    contract =
      Contract.new!(%{
        columns: [
          %{name: :id, type: :integer, null: true},
          %{name: :payload, type: :json, null: true}
        ]
      })

    validation =
      ContractValidation.compare(contract, [
        %{name: "id", data_type: "VARCHAR"},
        %{name: "extra", data_type: "BOOLEAN"}
      ])

    assert validation.status == :failed
    assert Enum.any?(validation.differences, &match?(%{kind: :missing, column: "payload"}, &1))
    assert Enum.any?(validation.differences, &match?(%{kind: :unexpected, column: "extra"}, &1))

    assert Enum.any?(
             validation.differences,
             &match?(%{kind: :type, column: "id", expected: :integer}, &1)
           )
  end

  test "checks nullability only when the adapter marks it reliable" do
    contract = Contract.new!(%{columns: [%{name: :id, type: :integer, null: false}]})

    assert %ContractValidation{status: :passed} =
             ContractValidation.compare(contract, [
               %{name: "id", data_type: "INTEGER", nullable?: true, metadata: %{}}
             ])

    assert %ContractValidation{
             status: :failed,
             differences: [
               %{kind: :nullability, column: "id", expected: false, observed: true}
             ]
           } =
             ContractValidation.compare(contract, [
               %{
                 name: "id",
                 data_type: "INTEGER",
                 nullable?: true,
                 metadata: %{contract_nullability: :reliable}
               }
             ])
  end

  test "generates stable claim identities independent of column descriptions" do
    first =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer, null: false, description: "first"}]
      })

    second =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer, null: false, description: "second"}]
      })

    assert [%{name: name, claim_id: "columns.not_null"}] =
             Contract.generated_check_specs(first)

    assert [%{name: ^name}] = Contract.generated_check_specs(second)
  end

  test "diffs explicit renames and compatibility-relevant changes without guessing" do
    previous =
      Contract.new!(%{
        columns: [%{name: :old_id, type: :integer, null: true}],
        unique_keys: [[:old_id]],
        row_count: [min: 1]
      })

    current =
      Contract.new!(%{
        columns: [
          %{name: :id, type: :string, null: false, renamed_from: :old_id},
          %{name: :payload, type: :json}
        ],
        unique_keys: [[:id]],
        row_count: [min: 2]
      })

    changes = Diff.between(previous, current)

    assert %{kind: :column_renamed, from: :old_id, to: :id} in changes
    assert %{kind: :column_added, column: :payload} in changes
    assert %{kind: :type_changed, column: :id, from: :integer, to: :string} in changes
    assert %{kind: :nullability_changed, column: :id, from: true, to: false} in changes
    assert Enum.any?(changes, &match?(%{kind: :unique_keys_changed}, &1))
    assert Enum.any?(changes, &match?(%{kind: :row_count_changed}, &1))
    refute Enum.any?(changes, &match?(%{kind: :column_removed, column: :old_id}, &1))
  end

  test "reports reorder-only contract changes" do
    previous =
      Contract.new!(%{
        columns: [%{name: :first, type: :integer}, %{name: :second, type: :string}]
      })

    current =
      Contract.new!(%{
        columns: [%{name: :second, type: :string}, %{name: :first, type: :integer}]
      })

    assert [%{kind: :column_order_changed, from: [:first, :second], to: [:second, :first]}] =
             Diff.between(previous, current)
  end

  test "does not normalize unsupported array types as scalars" do
    assert Contract.normalize_observed_type("INTEGER[]") == :unknown
    refute Contract.compatible_type?(:integer, "INTEGER[]")
  end

  test "bounds oversized observed schemas with explicit evidence" do
    contract = Contract.new!(%{columns: [%{name: :id, type: :integer}]})

    validation =
      ContractValidation.compare(
        contract,
        for(index <- 1..1_001, do: %{name: "column_#{index}", data_type: "INTEGER"})
      )

    assert validation.status == :failed
    assert validation.observed_column_count == 1_001
    assert validation.observed_truncated?
    assert length(validation.observed_columns) == 1_000
    assert [%{kind: :column_limit} | _rest] = validation.differences
  end

  test "groups required-column and uniqueness claims for wide contracts" do
    columns =
      for index <- 1..60 do
        %{name: String.to_atom("column_#{index}"), type: :integer, null: false}
      end

    contract = Contract.new!(%{columns: columns, unique_keys: [[:column_1], [:column_2]]})

    assert [required, unique] = Contract.generated_check_specs(contract)
    assert required.claim_id == "columns.not_null"
    assert unique.claim_id == "keys.unique"
  end

  test "generates stable row-count claims for exact and bounded constraints" do
    cases = [
      {[equals: 12], "row_count.equals.literal.12", "actual = 12", "12 AS expected"},
      {[min: 2, max: 9], "row_count.range.2.9", "actual >= 2 AND actual <= 9",
       "2 AS min, 9 AS max"},
      {[max: 50], "row_count.max.50", "actual <= 50", "50 AS max"}
    ]

    Enum.each(cases, fn {row_count, claim_id, predicate, metrics} ->
      contract = Contract.new!(%{columns: [%{name: :id, type: :integer}], row_count: row_count})

      assert [%{claim_id: ^claim_id, sql: sql}] = Contract.generated_check_specs(contract)
      assert sql =~ predicate
      assert sql =~ metrics
      assert sql =~ "FROM (SELECT count(*) AS actual FROM query())"
    end)
  end

  test "exposes typed requirements for parameterized exact row counts" do
    contract =
      Contract.new!(%{
        columns: [%{name: :id, type: :integer}],
        row_count: [equals: Param.new!(:expected_rows)]
      })

    assert %{expected_rows: :non_neg_integer} = Contract.runtime_param_requirements(contract)

    assert [%{claim_id: "row_count.equals.param.expected_rows", sql: sql}] =
             Contract.generated_check_specs(contract)

    assert sql =~ "actual = @expected_rows"
    assert sql =~ "@expected_rows AS expected"
  end

  test "rejects ambiguous and invalid row-count constraints" do
    columns = [%{name: :id, type: :integer}]

    assert_raise ArgumentError, ~r/equals: cannot be combined/, fn ->
      Contract.new!(%{columns: columns, row_count: [equals: 1, min: 1]})
    end

    assert_raise ArgumentError, ~r/min: must not exceed max:/, fn ->
      Contract.new!(%{columns: columns, row_count: [min: 2, max: 1]})
    end

    assert_raise ArgumentError, ~r/max: must be a non-negative integer/, fn ->
      Contract.new!(%{columns: columns, row_count: [max: -1]})
    end

    assert_raise ArgumentError, ~r/reserved for Favn runtime input/, fn ->
      Param.new!(:favn_run_id)
    end

    assert_raise ArgumentError, ~r/must start with a lowercase letter/, fn ->
      Param.new!(:"expected-rows")
    end
  end

  test "validates flattened fragment composition and keeps provenance out of semantic diffs" do
    previous =
      Contract.new!(%{
        columns: [
          %{name: :id, type: :integer},
          %{name: :created_at, type: :datetime},
          %{name: :run_id, type: :string}
        ],
        compositions: [Composition.new!(__MODULE__.AuditMetadata, 1, [:created_at, :run_id])]
      })

    current =
      Contract.new!(%{
        columns: previous.columns,
        compositions: [Composition.new!(__MODULE__.AuditMetadata, 0, [:id, :created_at])]
      })

    assert Diff.between(previous, current) == []

    assert [
             %{kind: :fragment_moved, module: __MODULE__.AuditMetadata, from: 1, to: 0},
             %{
               kind: :fragment_columns_changed,
               module: __MODULE__.AuditMetadata,
               from: [:created_at, :run_id],
               to: [:id, :created_at]
             }
           ] = Diff.provenance_between(previous, current)

    assert_raise ArgumentError, ~r/do not match the flattened contract/, fn ->
      Contract.new!(%{
        columns: previous.columns,
        compositions: [Composition.new!(__MODULE__.AuditMetadata, 1, [:created_at, :missing])]
      })
    end

    assert_raise ArgumentError, ~r/overlap or are out of order/, fn ->
      Contract.new!(%{
        columns: previous.columns,
        compositions: [
          Composition.new!(__MODULE__.First, 0, [:id, :created_at]),
          Composition.new!(__MODULE__.Second, 1, [:created_at, :run_id])
        ]
      })
    end
  end

  test "rejects non-module atoms in typed fragment provenance" do
    assert_raise ArgumentError, ~r/must be an Elixir module atom/, fn ->
      Composition.new!(:not_a_module, 0, [:id])
    end

    assert_raise ArgumentError, ~r/must be an Elixir module atom/, fn ->
      Fragment.new!(:not_a_module, [%{name: :id, type: :integer}])
    end
  end

  test "rejects generated checks whose executable template was tampered with" do
    contract = Contract.new!(%{columns: [%{name: :id, type: :integer}], row_count: [min: 1]})
    [spec] = Contract.generated_check_specs(contract)

    compile_opts = [
      file: "test/fixtures/contract_check.sql",
      line: 1,
      module: nil,
      scope: :query,
      enforce_query_root: true
    ]

    canonical_template = Template.compile!(spec.sql, compile_opts)
    altered_template = Template.compile!("SELECT false AS passed FROM query()", compile_opts)
    tampered_template = %{canonical_template | nodes: altered_template.nodes}

    check =
      Check.new!(%{
        name: spec.name,
        claim_id: spec.claim_id,
        at: spec.at,
        on_violation: spec.on_violation,
        when: spec.when,
        message: spec.message,
        sql: spec.sql,
        template: tampered_template,
        file: "test/fixtures/contract_check.sql",
        line: 1,
        origin: :contract,
        uses_query?: true,
        uses_target?: false
      })

    assert_raise ArgumentError, ~r/contract-generated check .* was modified/, fn ->
      Contract.validate_generated_checks!(contract, [check])
    end
  end
end
