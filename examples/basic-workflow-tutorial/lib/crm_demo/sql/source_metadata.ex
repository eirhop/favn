defmodule CrmDemo.SQL.SourceMetadata do
  @moduledoc """
  Projection helper for the Source technical columns.

  `use CrmDemo.SQL.SourceMetadata` in a Source asset, then call
  `source_metadata(...)` at the end of the select list. The projected columns
  and their order match `CrmDemo.Contracts.SourceMetadata` exactly.

      select
        raw."DealId" as deal_id,
        source_metadata(@landing_run_id, @extracted_at, md5(to_json(raw)))
      from raw

  `@landing_run_id` and `@extracted_at` come from the runtime-input resolver;
  `@favn_run_id` is bound by Favn.

  A shared helper is only appropriate for mechanics like this. Dataset-specific
  mapping, casting, and filtering stay in the asset's own SQL file.
  """

  use Favn.SQL

  @doc "Projects the four Source lineage columns."
  defsql source_metadata(landing_run_id, extracted_at, row_hash) do
    ~SQL"""
    @landing_run_id as _landing_run_id,
    @extracted_at as _extracted_at,
    @row_hash as _row_hash,
    @favn_run_id as _favn_run_id
    """
  end
end
