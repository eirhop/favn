defmodule Favn.SQL.PoolKey do
  @moduledoc """
  Stable, redacted identity for pooling compatible SQL sessions.

  The key exposes only a SHA-256 hash. Raw connection configuration and adapter
  options are intentionally retained only inside the hashed term.
  """

  alias Favn.Connection.Resolved

  @enforce_keys [:hash]
  defstruct [:hash]

  @type t :: %__MODULE__{hash: binary()}

  @doc """
  Builds a pool key from resolved connection identity and runtime inputs.
  """
  @spec build(Resolved.t(), keyword(), [atom() | String.t()], term()) :: t()
  def build(%Resolved{} = resolved, adapter_opts, required_catalogs, adapter_fingerprint \\ nil)
      when is_list(adapter_opts) and is_list(required_catalogs) do
    payload = {
      resolved.name,
      resolved.adapter,
      resolved.config,
      adapter_opts |> Keyword.delete(:required_catalogs) |> Enum.sort(),
      normalize_catalogs(required_catalogs),
      adapter_fingerprint
    }

    hash =
      payload
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    %__MODULE__{hash: hash}
  end

  defp normalize_catalogs(catalogs) do
    catalogs
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end
end
