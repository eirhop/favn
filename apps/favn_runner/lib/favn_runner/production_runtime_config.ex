defmodule FavnRunner.ProductionRuntimeConfig do
  @moduledoc """
  Production distributed-node configuration for the separate runner release.

  Packaged runners use OTP dynamic node names on a stable host alias. This makes
  every runner a hidden, non-listening client that can only establish its
  configured outbound control-plane connection. A high-entropy cookie and
  mutual TLS are both mandatory in production.
  """

  @default_shutdown_drain_timeout_ms 120_000

  @type config :: %{
          topology: :beam_node,
          runner_node: String.t(),
          runner_node_host_alias: String.t(),
          expected_control_plane_node: String.t(),
          epmd_port: pos_integer(),
          transport: :tls,
          mutual_tls?: true,
          shutdown_drain_timeout_ms: pos_integer(),
          cookie_configured?: true
        }

  @doc "Applies production config only for a release or an explicitly configured node."
  @spec apply_from_env_if_configured(map()) :: :ok | {:error, map()}
  def apply_from_env_if_configured(env) when is_map(env) do
    if production_release?(env) or Map.has_key?(env, "FAVN_RUNNER_NODE_HOST_ALIAS") do
      apply_from_env(env)
    else
      :ok
    end
  end

  @doc "Validates and freezes runner production environment configuration."
  @spec apply_from_env(map()) :: :ok | {:error, map()}
  def apply_from_env(env) when is_map(env) do
    with {:ok, config} <- validate(env) do
      with :ok <- Favn.DistributionTLS.validate_running_transport(env),
           :ok <- validate_running_dynamic_node(config.runner_node_host_alias) do
        Application.put_env(:favn_runner, :production_runtime_config, config)
        Application.put_env(:favn_runner, :production_runtime_diagnostics, diagnostics(config))

        Application.put_env(
          :favn_runner,
          :shutdown_drain_timeout_ms,
          config.shutdown_drain_timeout_ms
        )

        :ok
      else
        {:error, reason} -> {:error, %{status: :invalid, error: redact(reason)}}
      end
    end
  end

  @doc "Validates the runner distributed-node contract without mutating application state."
  @spec validate(map()) :: {:ok, config()} | {:error, map()}
  def validate(env) when is_map(env) do
    with {:ok, runner_node_host_alias} <-
           node_host_alias(env, "FAVN_RUNNER_NODE_HOST_ALIAS"),
         {:ok, control_plane_node} <- node_name(env, "FAVN_CONTROL_PLANE_NODE"),
         {:ok, cookie} <- required(env, "FAVN_DISTRIBUTION_COOKIE"),
         :ok <- distribution_cookie(cookie),
         :ok <- current_distribution_cookie(cookie),
         {:ok, epmd_port} <- optional_port(env, "ERL_EPMD_PORT", 4_369),
         {:ok, tls} <- Favn.DistributionTLS.validate(env),
         {:ok, shutdown_drain_timeout_ms} <-
           optional_integer(
             env,
             "FAVN_SHUTDOWN_DRAIN_TIMEOUT_MS",
             @default_shutdown_drain_timeout_ms,
             1_000,
             3_600_000
           ) do
      {:ok,
       %{
         topology: :beam_node,
         runner_node: "undefined@" <> runner_node_host_alias,
         runner_node_host_alias: runner_node_host_alias,
         expected_control_plane_node: control_plane_node,
         epmd_port: epmd_port,
         transport: tls.transport,
         mutual_tls?: tls.mutual_tls?,
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
        runner_node_host_alias: Map.fetch!(config, :runner_node_host_alias),
        expected_control_plane_node: Map.fetch!(config, :expected_control_plane_node),
        epmd_port: Map.fetch!(config, :epmd_port),
        transport: Map.fetch!(config, :transport),
        mutual_tls?: Map.fetch!(config, :mutual_tls?),
        shutdown_drain_timeout_ms: Map.fetch!(config, :shutdown_drain_timeout_ms),
        cookie_configured?: true
      }
    }
  end

  defp production_release?(env) do
    case Map.get(env, "RELEASE_NAME") do
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

  defp node_host_alias(env, name) do
    with {:ok, host} <- required(env, name),
         true <- valid_node_host?(host) do
      {:ok, host}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_env, name, "stable private DNS host alias"}}
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

  defp validate_running_dynamic_node(host_alias) do
    init_argument(:name, "undefined@" <> host_alias)
  end

  defp init_argument(name, expected) do
    values =
      case :init.get_argument(name) do
        {:ok, groups} ->
          for group when is_list(group) <- groups,
              value when is_list(value) <- group,
              do: List.to_string(value)

        :error ->
          []
      end

    if expected in values,
      do: :ok,
      else: {:error, {:invalid_env, "ERL_FLAGS", "-#{name} #{expected}"}}
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
