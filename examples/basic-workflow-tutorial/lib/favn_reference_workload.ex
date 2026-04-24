defmodule FavnReferenceWorkload do
  @moduledoc """
  Entry module for the reference workload tutorial project.

  This module mainly gives the project a clear root namespace
  (`FavnReferenceWorkload.*`). The actual ETL/ELT behavior lives in submodules:

  - `FavnReferenceWorkload.Connections.*` defines data-engine connections.
  - `FavnReferenceWorkload.Warehouse.*` defines source/raw/stg/gold/ops assets.
  - `FavnReferenceWorkload.Pipelines.*` defines runnable pipeline entrypoints.

  If you create your own project, this module can stay very small, just like
  here.
  """
end
