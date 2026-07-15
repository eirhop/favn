defmodule Favn.SQL.Session do
  @moduledoc false

  alias Favn.Connection.Resolved
  alias Favn.SQL.{Capabilities, ConcurrencyPolicies, ConcurrencyPolicy}
  alias Favn.SQL.SessionPool.Checkout

  @enforce_keys [:adapter, :resolved, :conn, :capabilities]
  defstruct [
    :adapter,
    :resolved,
    :conn,
    :capabilities,
    :concurrency_policy,
    :concurrency_policies,
    :admission_lease,
    :pool_checkout,
    required_catalogs: [],
    required_resources: []
  ]

  @type t :: %__MODULE__{
          adapter: module(),
          resolved: Resolved.t(),
          conn: term(),
           capabilities: Capabilities.t(),
           concurrency_policy: ConcurrencyPolicy.t() | nil,
           concurrency_policies: ConcurrencyPolicies.t() | nil,
           required_catalogs: [binary()],
           required_resources: [binary()],
           admission_lease: term(),
           pool_checkout: Checkout.t() | nil
         }
end
