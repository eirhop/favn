defmodule Favn.Log.Identity do
  @moduledoc """
  Canonical, browser-safe identities for log dimensions.

  Log producers may use runtime tuples and structs, while persisted and live
  filters use strings. Converting both sides here keeps historical pages,
  replay, and PubSub filtering identical.
  """

  @max_identity_bytes 512

  @doc "Returns the canonical external identity for an asset reference."
  @spec asset_ref(Favn.Ref.t() | String.t()) :: {:ok, String.t()} | {:error, :invalid_asset_ref}
  def asset_ref({module, name}) when is_atom(module) and is_atom(name) do
    validate(
      "asset:" <> Atom.to_string(module) <> ":" <> Atom.to_string(name),
      :invalid_asset_ref
    )
  end

  def asset_ref(value) when is_binary(value), do: validate(value, :invalid_asset_ref)
  def asset_ref(_value), do: {:error, :invalid_asset_ref}

  @doc "Returns the canonical external identity for a planned node key."
  @spec node_key(term()) :: {:ok, String.t()} | {:error, :invalid_node_key}
  def node_key(value) when is_binary(value), do: validate(value, :invalid_node_key)

  def node_key(nil), do: {:error, :invalid_node_key}

  def node_key(value) do
    identity =
      value
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)
      |> then(&("node:" <> &1))

    {:ok, identity}
  rescue
    _error -> {:error, :invalid_node_key}
  end

  defp validate(value, error) do
    if value != "" and byte_size(value) <= @max_identity_bytes and String.valid?(value),
      do: {:ok, value},
      else: {:error, error}
  end
end
