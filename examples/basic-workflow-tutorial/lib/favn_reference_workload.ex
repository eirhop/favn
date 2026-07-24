defmodule FavnReferenceWorkload do
  @moduledoc """
  Entry module for the temporary generic CRM UI example.

  This module mainly gives the project a clear root namespace
  (`FavnReferenceWorkload.*`). The actual ETL/ELT behavior lives in submodules:

  - `FavnReferenceWorkload.Connections.*` defines data-engine connections.
  - `FavnReferenceWorkload.Warehouse.*` defines Landing/Source/Core/Mart assets.
  - `FavnReferenceWorkload.Pipelines.*` defines runnable pipeline entrypoints.

  If you create your own project, this module can stay very small, just like
  here.
  """
end
