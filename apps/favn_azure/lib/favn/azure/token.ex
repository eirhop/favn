defmodule Favn.Azure.Token do
  @moduledoc """
  Runtime Azure access token returned by Favn Azure token providers.

  Tokens are intended for immediate adapter bootstrap use. Callers must not
  persist, log, or expose `access_token` values.
  """

  @enforce_keys [:access_token]
  defstruct [:access_token, :expires_on]

  @type t :: %__MODULE__{access_token: String.t(), expires_on: term()}
end
