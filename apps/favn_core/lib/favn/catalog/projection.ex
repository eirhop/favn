defmodule Favn.Catalog.Projection do
  @moduledoc """
  Pure version-scoped consumer rows derived from immutable catalog documents.

  Every table starts with `context` and `version`. Semantic source rows come from
  that artifact's embedded snapshot, never the currently selected manifest.
  Scalar discovery metadata is typed; repeated keys, grouping requirements,
  hierarchy levels and relationships have child rows with one-based ordinals.
  Missing declarations produce NULL scalars or no child rows. JSON detail remains
  the complete representation. See the public SQL catalog reference for joins.

  This is a breaking pre-v1 SQL layout. Existing metadata needs an explicit
  rebuild in the configured schema; artifact versions and macros are unchanged.
  """
  alias Favn.Catalog.Artifact
  alias Favn.Manifest.Serializer
  alias Favn.Semantic.Artifact, as: SemanticArtifact

  @type t :: %{
          kind: String.t(),
          version: String.t(),
          identity: String.t(),
          document: binary(),
          tables: map()
        }

  @doc "Produces deterministic, versioned relational rows for a validated artifact."
  @spec build(Artifact.t() | SemanticArtifact.t()) :: {:ok, t()} | {:error, term()}
  def build(%Artifact{document: doc} = artifact) do
    with {:ok, json} <- Artifact.encode(artifact) do
      tables = common(doc["assets"])

      tables =
        Map.merge(tables, %{
          "pipeline" => Enum.map(doc["pipelines"], &[&1["ref"], json(&1)]),
          "schedule" =>
            Enum.map(doc["schedules"], &[&1["ref"], json(&1)]) ++
              for(
                %{"schedule" => %{"inline" => schedule}} <- doc["pipelines"],
                do: [schedule["ref"], json(schedule)]
              )
        })

      finish("manifest", doc["manifest_version"], doc["catalog_version"], json, tables)
    end
  end

  def build(%SemanticArtifact{} = artifact) do
    with {:ok, encoded} <- SemanticArtifact.encode(artifact) do
      models = artifact.models
      metrics = for model <- models, metric <- model["metrics"], do: {model, metric}

      tables =
        Map.merge(common(artifact.snapshot), %{
          "model" =>
            Enum.map(
              models,
              &[
                &1["name"],
                &1["source_asset"],
                json(Map.delete(&1, "metrics")),
                get_in(&1, ["time", "column"]),
                get_in(&1, ["time", "grain"]),
                get_in(&1, ["time", "timezone"])
              ]
            ),
          "metric" =>
            Enum.map(metrics, fn {model, metric} ->
              [
                metric["ref"],
                model["name"],
                metric["macro_name"],
                nil,
                Favn.Semantic.Catalog.namespace(artifact),
                metric["canonical_sql"],
                json(metric),
                metric["description"],
                metric["unit"]["kind"],
                metric["unit"]["value"],
                get_in(metric, ["format", "style"]),
                get_in(metric, ["format", "decimals"]),
                metric["time_aggregate"]
              ]
            end),
          "metric_minimum_grain" => ordered_metrics(metrics, "minimum_grain"),
          "metric_entity_key" => ordered_metrics(metrics, "entity_key"),
          "metric_dependency" =>
            for(
              {_model, metric} <- metrics,
              ref <- metric["metric_dependencies"],
              do: [metric["ref"], ref]
            ),
          "dimension" =>
            for(
              model <- models,
              dimension = model["dimension"],
              dimension,
              do: [model["name"], dimension["name"], dimension["label"]]
            ),
          "dimension_key" =>
            for(
              model <- models,
              dimension = model["dimension"],
              dimension,
              {column, ordinal} <- Enum.with_index(dimension["key"], 1),
              do: [model["name"], ordinal, column]
            ),
          "hierarchy_level" =>
            for(
              model <- models,
              hierarchy <- model["hierarchies"],
              {column, ordinal} <- Enum.with_index(hierarchy["columns"], 1),
              do: [model["name"], hierarchy["name"], ordinal, column]
            ),
          "metric_input" =>
            for(
              {_model, metric} <- metrics,
              input <- metric["inputs"],
              do: [
                metric["ref"],
                input["position"],
                input["parameter"],
                input["source_asset"],
                input["column"],
                json(input)
              ]
            )
        })

      finish("semantic", artifact.semantic_version, artifact.semantic_version, encoded, tables)
    end
  end

  @doc "Ordered column names and portable SQL types for publisher-owned projections."
  @spec columns() :: map()
  def columns do
    %{
      "asset" => [
        {"ref", "VARCHAR"},
        {"kind", "VARCHAR"},
        {"description", "VARCHAR"},
        {"relation", "VARCHAR"},
        {"detail", "VARCHAR"}
      ],
      "column" => [
        {"asset_ref", "VARCHAR"},
        {"name", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"type", "VARCHAR"},
        {"nullable", "BOOLEAN"},
        {"description", "VARCHAR"},
        {"detail", "VARCHAR"}
      ],
      "contract" => [{"asset_ref", "VARCHAR"}, {"fingerprint", "VARCHAR"}, {"detail", "VARCHAR"}],
      "edge" => [{"asset_ref", "VARCHAR"}, {"dependency_ref", "VARCHAR"}],
      "pipeline" => [{"ref", "VARCHAR"}, {"detail", "VARCHAR"}],
      "schedule" => [{"ref", "VARCHAR"}, {"detail", "VARCHAR"}],
      "model" => [
        {"name", "VARCHAR"},
        {"source_asset", "VARCHAR"},
        {"detail", "VARCHAR"},
        {"time_column", "VARCHAR"},
        {"time_grain", "VARCHAR"},
        {"time_timezone", "VARCHAR"}
      ],
      "metric" => [
        {"ref", "VARCHAR"},
        {"model", "VARCHAR"},
        {"macro_name", "VARCHAR"},
        {"macro_catalog", "VARCHAR"},
        {"macro_schema", "VARCHAR"},
        {"canonical_sql", "VARCHAR"},
        {"detail", "VARCHAR"},
        {"description", "VARCHAR"},
        {"unit_kind", "VARCHAR"},
        {"unit_value", "VARCHAR"},
        {"format_style", "VARCHAR"},
        {"format_decimals", "BIGINT"},
        {"time_aggregate", "VARCHAR"}
      ],
      "metric_minimum_grain" => [
        {"metric_ref", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"relationship_name", "VARCHAR"}
      ],
      "metric_entity_key" => [
        {"metric_ref", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"column", "VARCHAR"}
      ],
      "metric_dependency" => [{"metric_ref", "VARCHAR"}, {"dependency_ref", "VARCHAR"}],
      "dimension" => [{"model", "VARCHAR"}, {"name", "VARCHAR"}, {"label_column", "VARCHAR"}],
      "dimension_key" => [{"model", "VARCHAR"}, {"ordinal", "BIGINT"}, {"column", "VARCHAR"}],
      "hierarchy_level" => [
        {"model", "VARCHAR"},
        {"hierarchy", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"column", "VARCHAR"}
      ],
      "relationship" => [
        {"asset_ref", "VARCHAR"},
        {"name", "VARCHAR"},
        {"target_asset_ref", "VARCHAR"},
        {"cardinality", "VARCHAR"},
        {"on_violation", "VARCHAR"}
      ],
      "relationship_key" => [
        {"asset_ref", "VARCHAR"},
        {"relationship_name", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"source_column", "VARCHAR"},
        {"target_column", "VARCHAR"}
      ],
      "metric_input" => [
        {"metric_ref", "VARCHAR"},
        {"ordinal", "BIGINT"},
        {"parameter", "VARCHAR"},
        {"source_asset", "VARCHAR"},
        {"column", "VARCHAR"},
        {"detail", "VARCHAR"}
      ]
    }
    |> Map.new(fn {table, columns} ->
      {table, [{"context", "VARCHAR"}, {"version", "VARCHAR"} | columns]}
    end)
  end

  defp ordered_metrics(metrics, field) do
    for {_model, metric} <- metrics,
        {value, ordinal} <- Enum.with_index(metric[field], 1),
        do: [metric["ref"], ordinal, value]
  end

  defp common(assets) do
    relationships =
      for asset <- assets,
          relationship <- get_in(asset, ["contract", "relationships"]) || [],
          do: {asset, relationship}

    %{
      "asset" =>
        Enum.map(
          assets,
          &[
            &1["ref"],
            &1["kind"],
            &1["description"],
            json(&1["relation"]),
            json(Map.drop(&1, ["contract"]))
          ]
        ),
      "contract" =>
        for(
          asset <- assets,
          asset["contract"],
          do: [asset["ref"], asset["fingerprint"], json(asset["contract"])]
        ),
      "column" =>
        for(
          asset <- assets,
          column <- get_in(asset, ["contract", "columns"]) || [],
          do: [
            asset["ref"],
            column["name"],
            column["ordinal"],
            column["type"],
            column["nullable"],
            column["description"],
            json(column)
          ]
        ),
      "relationship" =>
        for(
          {asset, relationship} <- relationships,
          do: [
            asset["ref"],
            relationship["name"],
            relationship["target"],
            relationship["cardinality"],
            relationship["on_violation"]
          ]
        ),
      "relationship_key" =>
        for(
          {asset, relationship} <- relationships,
          {pair, ordinal} <- Enum.with_index(relationship["on"], 1),
          do: [asset["ref"], relationship["name"], ordinal, pair["source"], pair["target"]]
        ),
      "edge" => for(asset <- assets, ref <- asset["dependencies"], do: [asset["ref"], ref])
    }
  end

  defp finish(kind, version, identity, document, tables) do
    tables = Map.new(tables, fn {name, rows} -> {name, Enum.map(rows, &[kind, version | &1])} end)

    if Enum.all?(tables, fn {_, rows} -> Enum.all?(rows, &(byte_size(json(&1)) <= 1_048_576)) end) do
      {:ok,
       %{kind: kind, version: version, identity: identity, document: document, tables: tables}}
    else
      {:error, :projection_row_too_large}
    end
  end

  defp json(value), do: Serializer.encode_canonical!(value)
end
