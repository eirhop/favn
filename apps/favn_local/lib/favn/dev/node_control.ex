defmodule Favn.Dev.NodeControl do
  @moduledoc """
  Distributed-node helpers for local tooling.

  Local tooling uses explicit shortname-based distributed Erlang nodes and stores
  the fully-qualified node name (`name@host`) in `.favn/runtime.json`.
  """

  alias Favn.Dev.DistributedErlang

  @spec ensure_local_node_started(String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_local_node_started(cookie, opts \\ []) when is_binary(cookie) and is_list(opts) do
    with {:ok, cookie_atom} <- DistributedErlang.cookie_to_atom(cookie) do
      case Node.alive?() do
        true ->
          Node.set_cookie(cookie_atom)
          :ok

        false ->
          :ok = configure_loopback_distribution(opts)

          name = Keyword.get(opts, :name, "favn_local_ctl_#{:erlang.unique_integer([:positive])}")

          with {:ok, name_atom} <- DistributedErlang.short_node_name_to_atom(name) do
            case Node.start(name_atom, name_domain: :shortnames) do
              {:ok, _pid} ->
                Node.set_cookie(cookie_atom)
                :ok

              {:error, {:already_started, _pid}} ->
                Node.set_cookie(cookie_atom)
                :ok

              {:error, reason} ->
                {:error, {:shortname_host_unavailable, reason}}
            end
          end
      end
    end
  end

  defp configure_loopback_distribution(opts) do
    System.put_env("ERL_EPMD_ADDRESS", "127.0.0.1")
    Application.put_env(:kernel, :inet_dist_use_interface, {127, 0, 0, 1})

    case Keyword.get(opts, :distribution_port) do
      port when is_integer(port) and port > 0 ->
        Application.put_env(:kernel, :inet_dist_listen_min, port)
        Application.put_env(:kernel, :inet_dist_listen_max, port)

      _missing ->
        :ok
    end
  end

  @spec shortname_to_full(String.t()) :: {:ok, String.t()} | {:error, term()}
  def shortname_to_full(shortname) when is_binary(shortname) and shortname != "" do
    with :ok <- DistributedErlang.validate_short_node_name(shortname),
         {:ok, host} <- local_short_host() do
      {:ok, shortname <> "@" <> host}
    end
  end

  def shortname_to_full(shortname), do: DistributedErlang.validate_short_node_name(shortname)

  defp local_short_host do
    case node() do
      :nonode@nohost ->
        short_host_from_localhost()

      node_name when is_atom(node_name) ->
        node_name
        |> Atom.to_string()
        |> String.split("@", parts: 2)
        |> parse_short_host()
    end
  end

  defp short_host_from_localhost do
    case :net_adm.localhost() do
      host when is_list(host) and host != [] ->
        host
        |> List.to_string()
        |> String.trim()
        |> String.downcase()
        |> String.split(".", parts: 2)
        |> hd()
        |> normalize_short_host()

      _other ->
        {:error, :shortname_host_not_available}
    end
  end

  defp normalize_short_host(host) when is_binary(host) and host != "" do
    if DistributedErlang.valid_short_host?(host) do
      {:ok, host}
    else
      {:error, {:invalid_shortname_host, host}}
    end
  end

  defp normalize_short_host(_host), do: {:error, :shortname_host_not_available}

  defp parse_short_host([_name, host]) when is_binary(host) and host != "" do
    if DistributedErlang.valid_short_host?(host) do
      {:ok, host}
    else
      {:error, {:invalid_shortname_host, host}}
    end
  end

  defp parse_short_host(_parts), do: {:error, :shortname_host_not_available}
end
