defmodule FavnNativeSemanticParity.Sales do
  @moduledoc false
  use Favn.SQLAsset

  relation(connection: :analytics, schema: "mart", name: "sales")
  materialized(:table)

  contract do
    grain(by: [:id])
    column(:id, :integer, null: false)
    column(:sold_at, :date, null: false)
    column(:gross, :decimal, null: false)
    column(:discount, :decimal, null: false)
  end

  semantic :sales do
    time(:sold_at, grain: :day, timezone: "Europe/Oslo")

    metric revenue(gross, discount),
      unit: {:currency, "NOK"},
      time_aggregate: :aggregate,
      description: "Net sales revenue" do
      ~SQL"SUM(@gross - @discount)"
    end
  end

  query do
    ~SQL"SELECT 1 AS id, DATE '2026-01-01' AS sold_at, 100::DECIMAL AS gross, 10::DECIMAL AS discount"
  end
end

alias Favn.Semantic.Artifact
alias FavnAuthoring.Semantic.Builder
alias FavnDuckdbADBC.SemanticCompiler

output = System.argv() |> Enum.reject(&(&1 == "--")) |> List.first()
if is_nil(output), do: raise("usage: mix run scripts/semantic_artifact_parity.exs -- OUTPUT")
{:ok, assets} = FavnAuthoring.list_assets([FavnNativeSemanticParity.Sales])
{:ok, artifact} = Builder.compile(assets, SemanticCompiler)
{:ok, json} = Artifact.encode(artifact)
digest = Base.encode16(:crypto.hash(:sha256, json), case: :lower)

unless artifact.semantic_version ==
         "sm_fd4123e936c378c85114d0899074b9e8340c0ae3d8397ea930a1418957ccd7d7" and
         digest == "78100cda23beec7137157e95c6cefe57afd7399e080fb76fdb60badce237caea" do
  raise "semantic artifact differs from the pre-removal DuckDB 1.5.5 baseline"
end

:ok = File.write(output, json, [:binary])
IO.puts("#{artifact.semantic_version} #{digest}")
