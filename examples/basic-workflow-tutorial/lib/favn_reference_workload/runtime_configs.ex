defmodule FavnReferenceWorkload.RuntimeConfigs do
  @moduledoc """
  Reusable runtime requirements for the reference workload's external sources.

  This module contains unresolved environment mappings only. Assets receive
  resolved values through their execution context.
  """

  use Favn.RuntimeConfig

  bundle(:source_system,
    segment_id: env!("FAVN_REFERENCE_SOURCE_SEGMENT_ID"),
    token: secret_env!("FAVN_REFERENCE_SOURCE_TOKEN")
  )
end
