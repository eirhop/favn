defmodule FavnOrchestrator.Operator.Catalogue.AssuranceTest do
  use ExUnit.Case, async: true

  alias Favn.SQL.Contract
  alias FavnOrchestrator.Operator.Catalogue.Assurance
  alias FavnOrchestrator.Projector
  alias FavnOrchestrator.RunState

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

  test "reads evidence out of a projected run rather than a hand-built map" do
    asset_ref = {MyApp.Orders, :asset}

    contract = Contract.new!(columns: [%{name: :id, type: :integer, null: false}])

    checks = [
      %{
        name: :orders_present,
        claim_id: nil,
        at: :after_materialize,
        when: nil,
        on_violation: :fail,
        message: nil,
        origin: :asset
      }
    ]

    run =
      RunState.new(
        id: "run-projected",
        workspace_id: "ws-assurance",
        deployment_id: "deploy-assurance",
        manifest_version_id: "mv-assurance",
        manifest_content_hash: "hash-assurance",
        runner_releases: %{default: "rr_assurance"},
        asset_ref: asset_ref,
        target_refs: [asset_ref]
      )
      |> Map.put(:result, %{
        asset_results: [
          %{
            ref: asset_ref,
            status: :ok,
            meta: %{
              quality_status: :passed,
              write_outcome: :written,
              check_results: [
                %{name: :orders_present, outcome: :passed, metrics: %{actual: 12}}
              ]
            }
          }
        ]
      })

    detail =
      Assurance.detail(
        %{ref: asset_ref, assurance: %{contract: contract, checks: checks}},
        Projector.project_run(run)
      )

    # The other test builds its run by hand, so it passed while production fed this
    # projection a compact history row that carries no result at all and left every
    # value below nil.
    assert detail.quality_status == :passed
    assert detail.write_outcome == :written
    assert [%{latest_result: %{outcome: :passed, metrics: %{actual: 12}}}] = detail.checks
    assert detail.latest_run_id == "run-projected"
  end
end
