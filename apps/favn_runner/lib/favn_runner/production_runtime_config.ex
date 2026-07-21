defmodule FavnRunner.ProductionRuntimeConfig do
  @moduledoc """
  Production distributed-node configuration for the separate runner release.

  Packaged releases require validated long node names, one fixed distribution
  port, and a high-entropy cookie supplied by the environment. Mix development
  and tests may omit the production contract entirely; supplying any production
  node variable opts into full validation.
  """

  @required_names [
    "FAVN_RUNNER_NODE",
    "FAVN_CONTROL_PLANE_NODE",
    "FAVN_DISTRIBUTION_COOKIE",
    "FAVN_BEAM_DISTRIBUTION_PORT"
  ]

  @default_shutdown_drain_timeout_ms 120_000

  @type config :: %{
          topology: :beam_node,
          runner_node: String.t(),
          expected_control_plane_node: String.t(),
          distribution_port: pos_integer(),
          epmd_port: pos_integer(),
          shutdown_drain_timeout_ms: pos_integer(),
          cookie_configured?: true
        }

  @doc "Applies production config only for a release or an explicitly configured node."
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, map()}
  def apply_from_env_if_configured(env \\ System.get_env()) when is_map(env) do
    if production_release?() or Enum.any?(@required_names, &Map.has_key?(env, &1)) do
      apply_from_env(env)
    else
      :ok
    end
  end

  @doc "Validates and freezes runner production environment configuration."
  @spec apply_from_env(map()) :: :ok | {:error, map()}
  def apply_from_env(env \\ System.get_env()) when is_map(env) do
    with {:ok, config} <- validate(env) do
      Application.put_env(:favn_runner, :production_runtime_config, config)
      Application.put_env(:favn_runner, :production_runtime_diagnostics, diagnostics(config))

      Application.put_env(
        :favn_runner,
        :shutdown_drain_timeout_ms,
        config.shutdown_drain_timeout_ms
      )

      :ok
    end
  end

  @doc "Validates the runner distributed-node contract without mutating application state."
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env \\ System.get_env()) when is_map(env) do
    with {:ok, runner_node} <- node_name(env, "FAVN_RUNNER_NODE"),
         {:ok, control_plane_node} <- node_name(env, "FAVN_CONTROL_PLANE_NODE"),
         :ok <- distinct_nodes(runner_node, control_plane_node),
         {:ok, cookie} <- required(env, "FAVN_DISTRIBUTION_COOKIE"),
         :ok <- distribution_cookie(cookie),
         :ok <- current_distribution_cookie(cookie),
         {:ok, distribution_port} <- required_port(env, "FAVN_BEAM_DISTRIBUTION_PORT"),
         {:ok, epmd_port} <- optional_port(env, "ERL_EPMD_PORT", 4_369),
         {:ok, shutdown_drain_timeout_ms} <-
           optional_integer(
             env,
             "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS",
             @default_shutdown_drain_timeout_ms,
             1_000,
             3_600_000
           ),
         :ok <- current_node_matches(runner_node) do
      {:ok,
       %{
         topology: :beam_node,
         runner_node: runner_node,
         expected_control_plane_node: control_plane_node,
         distribution_port: distribution_port,
         epmd_port: epmd_port,
         shutdown_drain_timeout_ms: shutdown_drain_timeout_ms,
         cookie_configured?: true
       }}
    else
      {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
    end
  end

  @doc "Returns bounded configuration diagnostics without the distribution cookie."
  @spec diagnostics(config()) :: map()
  def diagnostics(config) when is_map(config) do
    %{
      status: :ok,
      runner: %{
        topology: Map.fetch!(config, :topology),
        runner_node: Map.fetch!(config, :runner_node),
        expected_control_plane_node: Map.fetch!(config, :expected_control_plane_node),
        distribution_port: Map.fetch!(config, :distribution_port),
        epmd_port: Map.fetch!(config, :epmd_port),
        shutdown_drain_timeout_ms: Map.fetch!(config, :shutdown_drain_timeout_ms),
        cookie_configured?: true
      }
    }
  end

  defp production_release? do
    case System.get_env("RELEASE_NAME") do
      value when is_binary(value) -> String.trim(value) != ""
      _missing -> false
    end
  end

  defp node_name(env, name) do
    with {:ok, value} <- required(env, name),
         [local_name, host] <- String.split(value, "@", parts: 2),
         true <- valid_node_part?(local_name),
         true <- valid_node_host?(host) do
      {:ok, local_name <> "@" <> host}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_env, name, "long name@private-dns-name"}}
    end
  end

  defp valid_node_part?(value) do
    byte_size(value) in 1..255 and Regex.match?(~r/^[A-Za-z0-9_.-]+$/, value)
  end

  defp valid_node_host?(host) do
    normalized = String.downcase(host)

    valid_node_part?(host) and
      normalized not in ["localhost", "nohost", "127.0.0.1", "::1"] and
      not String.ends_with?(normalized, ".localhost") and
      not loopback_host?(host)
  end

  defp loopback_host?(host) do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, {127, _b, _c, _d}} -> true
      {:ok, {0, 0, 0, 0, 0, 0, 0, 1}} -> true
      _other -> false
    end
  end

  defp distinct_nodes(node, node),
    do: {:error, {:invalid_env, "FAVN_CONTROL_PLANE_NODE", "different from runner node"}}

  defp distinct_nodes(_runner_node, _control_plane_node), do: :ok

  defp current_node_matches(runner_node) do
    if Node.alive?() and Atom.to_string(node()) != runner_node do
      {:error, {:invalid_env, "FAVN_RUNNER_NODE", "equal to the running release node"}}
    else
      :ok
    end
  end

  defp distribution_cookie(cookie) do
    unique_bytes = cookie |> :binary.bin_to_list() |> MapSet.new() |> MapSet.size()

    if byte_size(cookie) in 32..255 and unique_bytes >= 12 and
         not Regex.match?(~r/\s/, cookie) do
      :ok
    else
      {:error, {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :insufficient_entropy}}
    end
  end

  defp current_distribution_cookie(cookie) do
    if Node.alive?() and Atom.to_string(Node.get_cookie()) != cookie do
      {:error, {:invalid_secret_env, "FAVN_DISTRIBUTION_COOKIE", :running_cookie_mismatch}}
    else
      :ok
    end
  end

  defp required_port(env, name) do
    with {:ok, value} <- required(env, name), do: parse_port(name, value)
  end

  defp optional_port(env, name, default) do
    case fetch(env, name) do
      {:ok, value} -> parse_port(name, value)
      :error -> {:ok, default}
    end
  end

  defp parse_port(name, value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _invalid -> {:error, {:invalid_env, name, "1..65535"}}
    end
  end

  defp optional_integer(env, name, default, minimum, maximum) do
    case fetch(env, name) do
      {:ok, value} ->
        case Integer.parse(value) do
          {integer, ""} when integer >= minimum and integer <= maximum -> {:ok, integer}
          _invalid -> {:error, {:invalid_env, name, "#{minimum}..#{maximum}"}}
        end

      :error ->
        {:ok, default}
    end
  end

  defp required(env, name) do
    case fetch(env, name) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_env, name}}
    end
  end

  defp fetch(env, name) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: :error, else: {:ok, value}

      _other ->
        :error
    end
  end

  defp redact({:missing_env, name}), do: {:missing_env, name}
  defp redact({:invalid_env, name, expected}), do: {:invalid_env, name, expected}
  defp redact({:invalid_secret_env, name, reason}), do: {:invalid_secret_env, name, reason}
end
