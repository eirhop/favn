defmodule Favn.SQL.Session do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.Capabilities

  @enforce_keys [:adapter, :resolved, :conn, :capabilities]
  defstruct [:adapter, :resolved, :conn, :capabilities]

  @type t :: %__MODULE__{
          adapter: module(),
          resolved: Resolved.t(),
          conn: term(),
          capabilities: Capabilities.t()
        }
end
