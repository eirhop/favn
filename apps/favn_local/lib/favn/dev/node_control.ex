defmodule Favn.Dev.NodeControl do
  @moduledoc """
  Distributed-node helpers for local tooling.

  Local tooling uses explicit shortname-based distributed Erlang nodes and stores
  the fully-qualified node name (`name@host`) in `.favn/runtime.json`.
  """

  alias Favn.Dev.DistributedErlang
  alias Favn.Dev.LocalDistribution

  @spec ensure_local_node_started(String.t(), keyword()) :: :ok | {:error, term()}
  def ensure_local_node_started(cookie, opts \\ []) when is_binary(cookie) and is_list(opts) do
    with {:ok, cookie_atom} <- DistributedErlang.cookie_to_atom(cookie) do
      case node_alive?(opts) do
        true ->
          set_cookie(cookie_atom, opts)
          :ok

        false ->
          start_local_node(cookie_atom, opts)
      end
    end
  end

  defp start_local_node(cookie_atom, opts) do
    name = Keyword.get(opts, :name, "favn_local_ctl_#{:erlang.unique_integer([:positive])}")

    with {:ok, distribution} <- local_distribution(opts),
         :ok <- configure_loopback_distribution(distribution, opts),
         {:ok, name_atom} <- DistributedErlang.short_node_name_to_atom(name) do
      start_node(name_atom, cookie_atom, opts)
    end
  end

  defp local_distribution(opts) do
    case LocalDistribution.preflight(opts) do
      {:ok, distribution} ->
        {:ok, distribution}

      {:error, reason} ->
        {:error,
         {:local_distribution_preflight_failed, reason, LocalDistribution.format_error(reason)}}
    end
  end

  defp start_node(name_atom, cookie_atom, opts) do
    case node_start(name_atom, [name_domain: :shortnames], opts) do
      {:ok, _pid} ->
        set_cookie(cookie_atom, opts)
        :ok

      {:error, {:already_started, _pid}} ->
        set_cookie(cookie_atom, opts)
        :ok

      {:error, reason} ->
        {:error, {:shortname_host_unavailable, reason}}
    end
  end

  defp configure_loopback_distribution(distribution, opts) do
    put_system_env("ERL_EPMD_ADDRESS", distribution.bind_ip, opts)

    distribution
    |> LocalDistribution.application_env(Keyword.get(opts, :distribution_port))
    |> Enum.each(fn {key, value} -> put_application_env(:kernel, key, value, opts) end)
  end

  defp node_alive?(opts), do: Keyword.get(opts, :node_alive?, &Node.alive?/0).()

  defp node_start(name, node_opts, opts) do
    Keyword.get(opts, :node_start, &Node.start/2).(name, node_opts)
  end

  defp set_cookie(cookie, opts), do: Keyword.get(opts, :set_cookie, &Node.set_cookie/1).(cookie)

  defp put_system_env(key, value, opts) do
    Keyword.get(opts, :put_system_env, &System.put_env/2).(key, value)
  end

  defp put_application_env(app, key, value, opts) do
    Keyword.get(opts, :put_application_env, &Application.put_env/3).(app, key, value)
  end

  @spec shortname_to_full(String.t()) :: {:ok, String.t()} | {:error, term()}
  def shortname_to_full(shortname) when is_binary(shortname) and shortname != "" do
    with :ok <- DistributedErlang.validate_short_node_name(shortname),
         {:ok, host} <- LocalDistribution.local_short_host() do
      {:ok, shortname <> "@" <> host}
    end
  end

  def shortname_to_full(shortname), do: DistributedErlang.validate_short_node_name(shortname)
end
