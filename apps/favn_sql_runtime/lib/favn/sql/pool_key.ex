defmodule Favn.SQL.PoolKey do
  @moduledoc """
  Stable, redacted identity for pooling compatible SQL sessions.

  The key exposes only SHA-256 hashes. `scope_hash` identifies the stable
  connection and session requirements, while `hash` also includes the adapter's
  runtime fingerprint. Raw connection configuration, adapter options, and
  runtime values are intentionally retained only inside the hashed terms.
  """

  alias Favn.Connection.Resolved

  @enforce_keys [:scope_hash, :hash]
  defstruct [:scope_hash, :hash]

  @type t :: %__MODULE__{scope_hash: binary(), hash: binary()}

  @doc """
  Builds a pool key from resolved connection identity and runtime inputs.
  """
  @spec build(Resolved.t(), keyword(), [atom() | String.t()], term()) :: t()
  def build(%Resolved{} = resolved, adapter_opts, required_catalogs, adapter_fingerprint)
      when is_list(adapter_opts) and is_list(required_catalogs) do
    build(resolved, adapter_opts, required_catalogs, [], adapter_fingerprint)
  end

  @spec build(
          Resolved.t(),
          keyword(),
          [atom() | String.t()],
          [atom() | String.t()],
          term()
        ) :: t()
  def build(
        %Resolved{} = resolved,
        adapter_opts,
        required_catalogs,
        required_resources,
        adapter_fingerprint
      )
      when is_list(adapter_opts) and is_list(required_catalogs) and is_list(required_resources) do
    scope_payload = {
      resolved.name,
      resolved.adapter,
      resolved.config,
      adapter_opts
      |> Keyword.drop([:required_catalogs, :required_resources])
      |> Enum.sort(),
      normalize_names(required_catalogs),
      normalize_names(required_resources)
    }

    scope_hash = digest(scope_payload)
    hash = digest({scope_payload, adapter_fingerprint})

    %__MODULE__{scope_hash: scope_hash, hash: hash}
  end

  defp digest(term) do
    term
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp normalize_names(names) do
    names
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end
end
