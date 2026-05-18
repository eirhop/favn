defmodule Favn.SQL.SessionPool.Checkout do
  @moduledoc """
  Metadata proving exclusive checkout ownership for a pooled SQL session.
  """

  alias Favn.SQL.{PoolConfig, PoolKey}

  @enforce_keys [:key, :config, :token, :owner]
  defstruct [:key, :config, :token, :owner]

  @type t :: %__MODULE__{
          key: PoolKey.t(),
          config: PoolConfig.t(),
          token: reference(),
          owner: pid()
        }
end
