defmodule FavnAuthoring.RuntimeRootsTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest
  alias Favn.Manifest.{Asset, Build, ExecutionPackage, Pipeline, SQLExecution, Schedule}
  alias Favn.RuntimeInputResolver.Ref

  defmodule Resolver do
  end

  test "collects only Elixir assets and runtime-input resolver references" do
    build = %Build{
      manifest: %Manifest{
        assets: [
          %Asset{module: MyApp.ElixirAsset, type: :elixir},
          %Asset{module: MyApp.SQLAsset, type: :sql},
          %Asset{module: MyApp.SourceAsset, type: :source}
        ],
        pipelines: [%Pipeline{module: MyApp.Pipeline}],
        schedules: [%Schedule{module: MyApp.Schedule}]
      },
      execution_packages: [
        %ExecutionPackage{
          content_hash: String.duplicate("b", 64),
          asset_ref: {MyApp.SQLWithoutRuntimeInputs, :asset},
          sql_execution: %SQLExecution{sql: "select 2", template: nil, runtime_inputs: nil}
        },
        %ExecutionPackage{
          content_hash: String.duplicate("a", 64),
          asset_ref: {MyApp.SQLAsset, :asset},
          sql_execution: %SQLExecution{
            sql: "select 1",
            template: nil,
            runtime_inputs: Ref.new!(Resolver)
          }
        }
      ]
    }

    assert {:ok, roots} = FavnAuthoring.runtime_roots(build)

    assert roots.asset_modules == ["Elixir.MyApp.ElixirAsset"]

    assert roots.runtime_input_resolver_modules == [Atom.to_string(Resolver)]
  end
end
