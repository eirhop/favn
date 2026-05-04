defmodule Favn.Dev.LocalDistribution do
  @moduledoc false

  alias Favn.Dev.DistributedErlang

  @type bind_tuple :: {byte(), byte(), byte(), byte()}
  @type t :: %{bind_ip: String.t(), bind_tuple: bind_tuple(), short_host: String.t()}

  @spec preflight(keyword()) :: {:ok, t()} | {:error, term()}
  def preflight(opts \\ []) when is_list(opts) do
    opts = Keyword.get(opts, :local_distribution, opts)

    with {:ok, short_host} <- local_short_host(opts),
         {:ok, bind_tuple} <- loopback_bind_tuple(short_host, opts),
         bind_ip = tuple_to_ip(bind_tuple),
         :ok <- ensure_epmd(bind_ip, opts) do
      {:ok, %{bind_ip: bind_ip, bind_tuple: bind_tuple, short_host: short_host}}
    end
  end

  @spec application_env(t(), pos_integer() | nil) :: [{atom(), term()}]
  def application_env(%{bind_tuple: bind_tuple}, distribution_port \\ nil) do
    port_env =
      case distribution_port do
        port when is_integer(port) and port > 0 ->
          [inet_dist_listen_min: port, inet_dist_listen_max: port]

        _missing ->
          []
      end

    [inet_dist_use_interface: bind_tuple] ++ port_env
  end

  @spec erl_tuple_string(bind_tuple()) :: String.t()
  def erl_tuple_string({a, b, c, d}), do: "{#{a},#{b},#{c},#{d}}"

  @spec erl_flags(t(), pos_integer()) :: String.t()
  def erl_flags(%{bind_tuple: bind_tuple}, port) when is_integer(port) and port > 0 do
    "-kernel inet_dist_use_interface #{erl_tuple_string(bind_tuple)} " <>
      "-kernel inet_dist_listen_min #{port} -kernel inet_dist_listen_max #{port}"
  end

  @spec format_error(term()) :: String.t()
  def format_error(:epmd_executable_missing) do
    "local Erlang distribution requires epmd, but no epmd executable was found; " <>
      "install Erlang/OTP with epmd available on PATH and retry"
  end

  def format_error({:epmd_autostart_failed, reason}) do
    "local Erlang distribution could not start epmd on loopback; run `epmd -daemon` " <>
      "or check local Erlang/OTP networking, then retry (#{inspect(reason)})"
  end

  def format_error({:shortname_host_not_loopback, host, addresses}) do
    "local Erlang shortname host #{inspect(host)} must resolve to a loopback 127.* address; " <>
      "resolved addresses: #{inspect(addresses)}"
  end

  def format_error(:shortname_host_not_available) do
    "local Erlang shortname host is unavailable; verify local hostname setup and retry"
  end

  def format_error({:invalid_shortname_host, host}) do
    "local Erlang shortname host #{inspect(host)} is invalid; expected a short hostname"
  end

  def format_error(reason), do: inspect(reason)

  @spec local_short_host(keyword()) :: {:ok, String.t()} | {:error, term()}
  def local_short_host(opts \\ []) when is_list(opts) do
    if Keyword.has_key?(opts, :localhost) do
      short_host_from_localhost(opts)
    else
      case Keyword.get(opts, :node, node()) do
        :nonode@nohost -> short_host_from_localhost(opts)
        node_name when is_atom(node_name) -> node_name |> Atom.to_string() |> parse_short_host()
      end
    end
  end

  defp short_host_from_localhost(opts) do
    localhost = Keyword.get(opts, :localhost, &:net_adm.localhost/0)

    case localhost.() do
      host when is_list(host) and host != [] -> host |> List.to_string() |> normalize_short_host()
      host when is_binary(host) and host != "" -> normalize_short_host(host)
      _other -> {:error, :shortname_host_not_available}
    end
  end

  defp normalize_short_host(host) when is_binary(host) and host != "" do
    host = host |> String.trim() |> String.split(".", parts: 2) |> hd()

    if DistributedErlang.valid_short_host?(host) do
      {:ok, host}
    else
      {:error, {:invalid_shortname_host, host}}
    end
  end

  defp normalize_short_host(_host), do: {:error, :shortname_host_not_available}

  defp parse_short_host(node_name) do
    case String.split(node_name, "@", parts: 2) do
      [_name, host] when is_binary(host) and host != "" -> normalize_short_host(host)
      _parts -> {:error, :shortname_host_not_available}
    end
  end

  defp loopback_bind_tuple(short_host, opts) do
    resolver = Keyword.get(opts, :resolver, &:inet.gethostbyname/1)

    case resolver.(String.to_charlist(short_host)) do
      {:ok, {:hostent, _name, _aliases, :inet, 4, addresses}} ->
        choose_loopback(short_host, addresses)

      {:ok, %{h_addr_list: addresses}} ->
        choose_loopback(short_host, addresses)

      {:ok, addresses} when is_list(addresses) ->
        choose_loopback(short_host, addresses)

      {:error, reason} ->
        {:error, {:shortname_host_resolution_failed, short_host, reason}}
    end
  end

  defp choose_loopback(short_host, addresses) do
    case Enum.find(addresses, &loopback?/1) do
      nil -> {:error, {:shortname_host_not_loopback, short_host, addresses}}
      bind_tuple -> {:ok, bind_tuple}
    end
  end

  defp loopback?({127, _b, _c, _d}), do: true
  defp loopback?(_address), do: false

  defp ensure_epmd(bind_ip, opts) do
    case Keyword.fetch(opts, :epmd_executable) do
      {:ok, false} ->
        :ok

      {:ok, nil} ->
        {:error, :epmd_executable_missing}

      {:ok, executable} when is_binary(executable) ->
        ensure_epmd_running(executable, bind_ip, opts)

      :error ->
        case System.find_executable("epmd") do
          nil -> {:error, :epmd_executable_missing}
          executable -> ensure_epmd_running(executable, bind_ip, opts)
        end
    end
  end

  defp ensure_epmd_running(executable, bind_ip, opts) do
    names = Keyword.get(opts, :epmd_names, &epmd_names(executable, bind_ip, &1))
    daemon = Keyword.get(opts, :epmd_daemon, &epmd_daemon(executable, bind_ip, &1))

    case names.(opts) do
      :ok ->
        :ok

      {:error, _reason} ->
        with :ok <- daemon.(opts),
             :ok <- names.(opts) do
          :ok
        else
          {:error, reason} -> {:error, {:epmd_autostart_failed, reason}}
        end
    end
  end

  defp epmd_names(executable, bind_ip, _opts) do
    case System.cmd(executable, ["-names"],
           env: [{"ERL_EPMD_ADDRESS", bind_ip}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:epmd_names_failed, status, output}}
    end
  end

  defp epmd_daemon(executable, bind_ip, _opts) do
    case System.cmd(executable, ["-daemon"],
           env: [{"ERL_EPMD_ADDRESS", bind_ip}],
           stderr_to_stdout: true
         ) do
      {_output, 0} -> :ok
      {output, status} -> {:error, {:epmd_daemon_failed, status, output}}
    end
  end

  defp tuple_to_ip({a, b, c, d}), do: Enum.join([a, b, c, d], ".")
end
