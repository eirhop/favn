defmodule Favn.Dev.NodeControl do
  @moduledoc """
  Distributed-node helpers for local tooling.

  Local tooling uses explicit shortname-based distributed Erlang nodes and stores
  the fully-qualified node name (`name@host`) in `.favn/runtime.json`.
  """

  @spec ensure_local_node_started(String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_local_node_started(cookie, opts \\ []) when is_binary(cookie) and is_list(opts) do
    case Node.alive?() do
      true ->
        Node.set_cookie(String.to_atom(cookie))
        :ok

      false ->
        name =
          opts
          |> Keyword.get(:name, "favn_local_ctl_#{:erlang.unique_integer([:positive])}")
          |> String.to_atom()

        case Node.start(name, name_domain: :shortnames) do
          {:ok, _pid} ->
            Node.set_cookie(String.to_atom(cookie))
            :ok

          {:error, {:already_started, _pid}} ->
            Node.set_cookie(String.to_atom(cookie))
            :ok

          {:error, reason} ->
            {:error, {:node_start_failed, reason}}
        end
    end
  end

  @spec shortname_to_full(String.t()) :: {:ok, String.t()} | {:error, term()}
  def shortname_to_full(shortname) when is_binary(shortname) and shortname != "" do
    case :net_adm.localhost() do
      host when is_list(host) and host != [] ->
        {:ok, shortname <> "@" <> List.to_string(host)}

      other ->
        {:error, {:invalid_local_host, other}}
    end
  end
end
