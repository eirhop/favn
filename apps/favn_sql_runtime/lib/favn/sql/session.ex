defmodule Favn.SQL.Session do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, ConcurrencyPolicies, ConcurrencyPolicy}

  @enforce_keys [:adapter, :resolved, :conn, :capabilities]
  defstruct [
    :adapter,
    :resolved,
    :conn,
    :capabilities,
    :concurrency_policy,
    :concurrency_policies,
    :admission_lease
  ]

  @type t :: %__MODULE__{
          adapter: module(),
          resolved: Resolved.t(),
          conn: term(),
          capabilities: Capabilities.t(),
          concurrency_policy: ConcurrencyPolicy.t() | nil,
          concurrency_policies: ConcurrencyPolicies.t() | nil,
          admission_lease: term()
        }
end
