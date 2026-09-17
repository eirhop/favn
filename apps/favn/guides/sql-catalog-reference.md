# SQL Catalog Reference

[Publish artifacts](sql-catalog-publication.md) to expose these tables in your
configured target. The examples use `mart.meta`; replace it with your catalog
and schema. Every projection row belongs to a `context` (`manifest` or `semantic`)
and `version`. Join on **both**, then use `selection` to choose the current version.
A semantic version carries its own source contracts, independently of the selected
manifest. Selecting definitions does not prove that compatible data is served.

## Typed discovery fields

The complete artifact remains in `release.document`, with complete per-record
JSON in the existing `detail` columns. These typed fields avoid JSON extraction:

| Table | Additional columns |
| --- | --- |
| `metric` | `description`, `unit_kind`, `unit_value`, `format_style`, `format_decimals`, `time_aggregate` |
| `model` | `time_column`, `time_grain`, `time_timezone` |

All are `VARCHAR` except `format_decimals` (`BIGINT`). Missing declarations are
SQL `NULL`: a missing format differs from a declared style with unspecified
decimals. Unit value is optional (for example `currency` / `NOK`, or `count` / NULL).
The compiler's time aggregation value is exposed unchanged.

Search selected metric descriptions and retrieve their exact callable location:

```sql
-- catalog-example: descriptions
SELECT m.ref, m.description, m.unit_kind, m.unit_value,
       m.format_style, m.format_decimals, m.time_aggregate,
       m.macro_catalog, m.macro_schema, m.macro_name
FROM mart.meta.metric m
JOIN mart.meta.selection s ON s.context = m.context AND s.version = m.version
WHERE m.context = 'semantic' AND m.description ILIKE '%revenue%'
ORDER BY m.ref;
```

## Repeated metadata

Each table below also has `context` and `version`. All fields are `VARCHAR`
except `ordinal` (`BIGINT`). Ordinals start at one and preserve artifact order;
SQL callers must explicitly `ORDER BY` them. An absent declaration creates no rows.

| Table | Fields after context/version | Meaning |
| --- | --- | --- |
| `metric_minimum_grain` | `metric_ref`, `ordinal`, `relationship_name` | Required grouping roles |
| `metric_entity_key` | `metric_ref`, `ordinal`, `column` | Entity keys for first/last time aggregation |
| `metric_dependency` | `metric_ref`, `dependency_ref` | Direct metric dependency edges |
| `dimension` | `model`, `name`, `label_column` | Named dimension and display label |
| `dimension_key` | `model`, `ordinal`, `column` | Ordered keys derived from the source contract |
| `hierarchy_level` | `model`, `hierarchy`, `ordinal`, `column` | Named hierarchy's ordered levels |
| `relationship` | `asset_ref`, `name`, `target_asset_ref`, `cardinality`, `on_violation` | Source contract relationship |
| `relationship_key` | `asset_ref`, `relationship_name`, `ordinal`, `source_column`, `target_column` | Ordered source/target key pairs |

Relationships exist in both contexts. Dimension and metric metadata belongs to
the semantic context. Existing `metric_input` supplies argument order and column
bindings; `model.source_asset` and `asset.relation` supply the authored source.

Minimum-grain names refer to **relationship roles**, not dimension names. For
example, `store` and `billing_store` can point to the same dimension but require
different joins. Resolve each role through its metric's source contract:

```sql
-- catalog-example: grouping
SELECT m.ref, g.relationship_name, r.target_asset_ref,
       r.cardinality, r.on_violation, k.ordinal, k.source_column, k.target_column
FROM mart.meta.metric m
JOIN mart.meta.selection s ON s.context = m.context AND s.version = m.version
JOIN mart.meta.model model
  ON model.context = m.context AND model.version = m.version AND model.name = m.model
JOIN mart.meta.metric_minimum_grain g
  ON g.context = m.context AND g.version = m.version AND g.metric_ref = m.ref
JOIN mart.meta.relationship r
  ON r.context = m.context AND r.version = m.version
 AND r.asset_ref = model.source_asset AND r.name = g.relationship_name
JOIN mart.meta.relationship_key k
  ON k.context = r.context AND k.version = r.version
 AND k.asset_ref = r.asset_ref AND k.relationship_name = r.name
WHERE m.context = 'semantic' AND m.ref = 'sales.revenue'
ORDER BY g.ordinal, k.ordinal;
```

For a composite store key this returns both `tenant_id` and `store_id`, in order,
for each role. The consumer constructs its SQL using those mappings; Favn does
not execute dashboard queries or infer which physical serving copy to use.

List direct metric dependencies (a UI can traverse these edges):

```sql
-- catalog-example: dependencies
SELECT d.metric_ref, d.dependency_ref, dependency.description
FROM mart.meta.metric_dependency d
JOIN mart.meta.selection s ON s.context = d.context AND s.version = d.version
JOIN mart.meta.metric dependency
  ON dependency.context = d.context AND dependency.version = d.version
 AND dependency.ref = d.dependency_ref
WHERE d.context = 'semantic'
ORDER BY d.metric_ref, d.dependency_ref;
```

List hierarchy levels and the dimension's display label:

```sql
-- catalog-example: hierarchies
SELECT d.model, d.name, d.label_column, h.hierarchy, h.ordinal, h."column"
FROM mart.meta.dimension d
JOIN mart.meta.selection s ON s.context = d.context AND s.version = d.version
JOIN mart.meta.hierarchy_level h
  ON h.context = d.context AND h.version = d.version AND h.model = d.model
WHERE d.context = 'semantic'
ORDER BY d.model, h.hierarchy, h.ordinal;
```

For example, `country → tenant_id → store_id` is returned in that order. Read
`dimension_key` separately when building joins: its ordinal orders key columns,
and does not identify a corresponding hierarchy level. Entity keys likewise
come from `metric_entity_key`; they are not an additional measure input.
