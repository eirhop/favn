defmodule Favn.Azure.PostgresAuthenticationError do
  @moduledoc """
  Redacted control-plane PostgreSQL managed-identity authentication failure.

  The error preserves only an allowlisted failure class and whether a later
  connection attempt may succeed. It never retains a token or identity input.
  """

  @enforce_keys [:class, :message]
  defexception [:class, :message, retryable?: false]

  @type class ::
          :invalid_config
          | :identity_unavailable
          | :identity_rejected
          | :token_timeout
          | :insufficient_validity
          | :provider_unavailable

  @type t :: %__MODULE__{
          class: class(),
          message: String.t(),
          retryable?: boolean()
        }
end

defimpl Inspect, for: Favn.Azure.PostgresAuthenticationError do
  import Inspect.Algebra

  def inspect(error, opts) do
    concat([
      "#Favn.Azure.PostgresAuthenticationError<",
      to_doc(
        [class: error.class, message: :redacted, retryable?: error.retryable?],
        opts
      ),
      ">"
    ])
  end
end
