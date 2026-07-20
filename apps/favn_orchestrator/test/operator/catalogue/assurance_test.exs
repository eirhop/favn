defmodule FavnOrchestrator.Operator.Catalogue.AssuranceTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.Contract
  alias FavnOrchestrator.Operator.Catalogue.Assurance

  test "associates ordered row-count claims with canonical ids and latest results" do
    asset_ref = {MyApp.Orders, :asset}

    contract =
      Contract.new!(
        columns: [%{name: :id, type: :integer, null: false}],
        row_counts: [[min: 1], [min: 1, on_violation: :warn]]
      )

    specs = Contract.generated_check_specs(contract)

    checks =
      Enum.map(specs, fn spec ->
        spec
        |> Map.take([:name, :claim_id, :at, :when, :on_violation, :message])
        |> Map.put(:origin, :contract)
      end)

    latest_run = %{
      id: "run-123",
      asset_results: %{
        asset_ref => %{
          meta: %{
            check_results: [
              %{name: Enum.at(specs, 0).name, outcome: :passed, metrics: %{actual: 5}},
              %{name: Enum.at(specs, 1).name, outcome: :warned, metrics: %{actual: 5}}
            ]
          }
        }
      }
    }

    detail =
      Assurance.detail(
        %{ref: asset_ref, assurance: %{contract: contract, checks: checks}},
        latest_run
      )

    assert Enum.map(detail.contract.row_counts, & &1.claim_id) == [
             "row_count.min.1",
             "row_count.min.1.occurrence.2"
           ]

    assert Enum.map(detail.contract.row_counts, & &1.latest_result.outcome) == [
             :passed,
             :warned
           ]
  end
end
