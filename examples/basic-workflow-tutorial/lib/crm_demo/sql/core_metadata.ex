defmodule CrmDemo.SQL.CoreMetadata do
  @moduledoc """
  Projection helper for the Core technical columns.

  Both values are owned and bound by Favn, so a Core model records when and by
  which run it was published without needing a runtime-input resolver:

      select
        deal.deal_id as opportunity_id,
        core_metadata(@favn_run_started_at)
      from source.deal as deal

  The projected order matches `CrmDemo.Contracts.CoreMetadata`.
  """

  use Favn.SQL

  @doc "Projects the two Core run columns."
  defsql core_metadata(processed_at) do
    ~SQL"""
    @processed_at as _processed_at,
    @favn_run_id as _favn_run_id
    """
  end
end
