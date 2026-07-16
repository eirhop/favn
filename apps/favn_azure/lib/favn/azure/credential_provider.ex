defmodule Favn.Azure.CredentialProvider do
  @moduledoc """
  Source contract used by the Azure credential cache.

  Providers acquire one token. Caching, refresh coordination, and bounded
  concurrency are owned by `Favn.Azure.Credentials`.
  """

  alias Favn.Azure.Credentials.Request
  alias Favn.Azure.{Token, TokenError}

  @callback fetch_token(Request.t(), keyword()) ::
              {:ok, Token.t()} | {:error, TokenError.t()}
end
