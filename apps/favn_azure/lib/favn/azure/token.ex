defmodule Favn.Azure.Token do
  @moduledoc """
  Runtime Azure access token with a normalized UTC expiry.

  Tokens may be used by integrations but must never be persisted, logged, or
  returned in metadata. Inspect output is always redacted. Ambiguous Azure CLI
  `expiresOn` timestamps without a timezone are rejected; providers should
  prefer the UTC epoch `expires_on` field.
  """

  @enforce_keys [:access_token, :expires_at]
  defstruct [:access_token, :expires_at]

  @type t :: %__MODULE__{access_token: String.t(), expires_at: DateTime.t()}

  @doc false
  @spec new(String.t(), term()) :: {:ok, t()} | {:error, :invalid_token | :invalid_expiry}
  def new(access_token, expiry) when is_binary(access_token) and access_token != "" do
    case normalize_expiry(expiry) do
      {:ok, expires_at} -> {:ok, %__MODULE__{access_token: access_token, expires_at: expires_at}}
      :error -> {:error, :invalid_expiry}
    end
  end

  def new(_access_token, _expiry), do: {:error, :invalid_token}

  @doc false
  @spec valid_for?(t(), non_neg_integer(), DateTime.t()) :: boolean()
  def valid_for?(%__MODULE__{expires_at: expires_at}, seconds, now \\ DateTime.utc_now())
      when is_integer(seconds) and seconds >= 0 do
    DateTime.compare(expires_at, DateTime.add(now, seconds, :second)) == :gt
  end

  defp normalize_expiry(%DateTime{} = value), do: {:ok, DateTime.shift_zone!(value, "Etc/UTC")}

  defp normalize_expiry(value) when is_integer(value), do: from_unix(value)

  defp normalize_expiry(value) when is_binary(value) do
    case Integer.parse(value) do
      {unix, ""} -> from_unix(unix)
      _other -> parse_datetime(value)
    end
  end

  defp normalize_expiry(_value), do: :error

  defp from_unix(value) do
    case DateTime.from_unix(value) do
      {:ok, datetime} -> {:ok, datetime}
      {:error, _reason} -> :error
    end
  end

  defp parse_datetime(value) do
    normalized = String.replace(value, " ", "T")

    case DateTime.from_iso8601(normalized) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, _reason} -> :error
    end
  end
end

defimpl Inspect, for: Favn.Azure.Token do
  import Inspect.Algebra

  def inspect(token, opts) do
    concat([
      "#Favn.Azure.Token<",
      to_doc([access_token: :redacted, expires_at: token.expires_at], opts),
      ">"
    ])
  end
end
