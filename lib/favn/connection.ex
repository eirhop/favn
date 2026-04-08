defmodule Favn.Connection do
  @moduledoc """
  Behaviour for connection definition providers.

  A provider module returns static connection metadata through `definition/0`.
  Runtime values are supplied through `config :favn, connections: [...]` and are
  merged and validated by `Favn.Connection.Loader` during application startup.
  """

  alias Favn.Connection.Definition

  @callback definition() :: Definition.t()
end
