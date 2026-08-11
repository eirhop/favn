defmodule FavnView.ReleaseHealth do
  @moduledoc """
  Container-local HTTP readiness probe for the View release.

  The probe opens one bounded loopback connection to the bind address frozen by
  production configuration. A 200 from the real readiness route proves that
  the Phoenix listener, View configuration, and bounded Orchestrator facade call
  are all ready.
  """

  @default_timeout_ms 3_000
  @persistent_key {__MODULE__, :probe}

  @type error ::
          :not_configured
          | :invalid_host
          | :invalid_port
          | :connect_failed
          | :request_failed
          | :not_ready

  @type probe_config :: %{required(:bind_host) => String.t(), required(:port) => term()}

  @doc false
  @spec configure(probe_config()) :: :ok | {:error, :invalid_host | :invalid_port}
  def configure(config) when is_map(config) do
    with {:ok, probe} <- normalize_probe(config) do
      :persistent_term.put(@persistent_key, probe)
      :ok
    end
  end

  @doc "Checks the frozen View HTTP readiness endpoint."
  @spec run() :: :ok | {:error, error()}
  def run do
    case :persistent_term.get(@persistent_key, :missing) do
      :missing -> {:error, :not_configured}
      probe -> probe(probe)
    end
  end

  @doc false
  @spec run(probe_config()) :: :ok | {:error, error()}
  def run(config) when is_map(config) do
    with {:ok, probe} <- normalize_probe(config), do: probe(probe)
  end

  @doc "Runs the readiness check and raises a bounded error for release scripts."
  @spec run!() :: :ok
  def run! do
    case run() do
      :ok -> :ok
      {:error, reason} -> raise "view readiness check failed: #{reason}"
    end
  end

  defp normalize_probe(config) do
    with {:ok, address, host} <- bind_address(Map.get(config, :bind_host)),
         {:ok, port} <- port(Map.get(config, :port)) do
      {:ok, %{address: address, host: host, port: port}}
    end
  end

  defp probe(%{address: address, host: host, port: port}) do
    with {:ok, socket} <- connect(address, port),
         :ok <- request(socket, host),
         :ok <- response(socket) do
      :ok
    end
  end

  defp port(value) when is_integer(value) and value in 1..65_535, do: {:ok, value}

  defp port(value) when is_binary(value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, :invalid_port}
    end
  end

  defp port(_value), do: {:error, :invalid_port}

  defp bind_address("0.0.0.0"), do: {:ok, {127, 0, 0, 1}, "127.0.0.1"}
  defp bind_address(_value), do: {:error, :invalid_host}

  defp connect(address, port) do
    case :gen_tcp.connect(
           address,
           port,
           [:binary, active: false, packet: :line, send_timeout: @default_timeout_ms],
           @default_timeout_ms
         ) do
      {:ok, socket} -> {:ok, socket}
      {:error, _reason} -> {:error, :connect_failed}
    end
  end

  defp request(socket, host) do
    case :gen_tcp.send(
           socket,
           "GET /api/web/v1/health/ready HTTP/1.1\r\nHost: #{host}\r\nConnection: close\r\n\r\n"
         ) do
      :ok -> :ok
      {:error, _reason} -> close(socket, :request_failed)
    end
  end

  defp response(socket) do
    result =
      case :gen_tcp.recv(socket, 0, @default_timeout_ms) do
        {:ok, "HTTP/1.1 200 " <> _rest} -> :ok
        {:ok, "HTTP/1.0 200 " <> _rest} -> :ok
        {:ok, _other} -> {:error, :not_ready}
        {:error, _reason} -> {:error, :not_ready}
      end

    _ = :gen_tcp.close(socket)
    result
  end

  defp close(socket, reason) do
    _ = :gen_tcp.close(socket)
    {:error, reason}
  end
end
