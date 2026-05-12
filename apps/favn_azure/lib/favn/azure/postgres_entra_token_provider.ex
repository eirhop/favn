defmodule Favn.Azure.PostgresEntraTokenProvider do
  @moduledoc """
  Behaviour for Azure PostgreSQL Microsoft Entra token providers.
  """

  alias Favn.Azure.{Token, TokenError}

  @type auth :: keyword() | map()
  @type opts :: keyword()

  @callback fetch_token(auth(), opts()) :: {:ok, Token.t()} | {:error, TokenError.t()}
end
