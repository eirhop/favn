defmodule Favn.DistributionTLS do
  @moduledoc """
  Validates the mutual-TLS contract used by production distributed BEAM nodes.

  The cookie remains an additional cluster secret; TLS authenticates nodes and
  encrypts task payloads, runtime inputs, logs, and results in transit.
  """

  @env_name "FAVN_DISTRIBUTION_TLS_OPTIONS_FILE"

  @type config :: %{transport: :tls, options_file: Path.t(), mutual_tls?: true}

  @spec validate(map()) :: {:ok, config()} | {:error, term()}
  def validate(env) when is_map(env) do
    with {:ok, path} <- options_file(env),
         {:ok, terms} <- consult(path),
         {:ok, server} <- section(terms, :server),
         {:ok, client} <- section(terms, :client),
         :ok <- require_option(server, :verify, :verify_peer),
         :ok <- require_option(server, :fail_if_no_peer_cert, true),
         :ok <- require_option(client, :verify, :verify_peer),
         :ok <- require_credentials(server),
         :ok <- require_credentials(client) do
      {:ok, %{transport: :tls, options_file: path, mutual_tls?: true}}
    end
  end

  @doc """
  Verifies the transport the current VM actually started with.

  This is intentionally independent of release environment markers: production
  runtime configuration must never validate a TLS file while the VM continues
  to use plaintext distribution.
  """
  @spec validate_running_transport(map()) :: :ok | {:error, term()}
  def validate_running_transport(env) when is_map(env) do
    with :ok <- init_argument(:proto_dist, "inet_tls"),
         {:ok, expected} <- options_file(env),
         :ok <- init_argument(:ssl_dist_optfile, expected) do
      :ok
    end
  end

  defp options_file(env) do
    case Map.get(env, @env_name) do
      value when is_binary(value) ->
        path = value |> String.trim() |> Path.expand()

        if Path.type(path) == :absolute and File.regular?(path),
          do: {:ok, path},
          else: {:error, {:invalid_env, @env_name, "absolute readable TLS options file"}}

      _missing ->
        {:error, {:missing_env, @env_name}}
    end
  end

  defp consult(path) do
    case :file.consult(String.to_charlist(path)) do
      {:ok, [terms]} when is_list(terms) -> {:ok, terms}
      _invalid -> {:error, {:invalid_env, @env_name, "valid Erlang TLS options term"}}
    end
  end

  defp section(terms, name) do
    case Keyword.fetch(terms, name) do
      {:ok, options} when is_list(options) -> {:ok, options}
      _missing -> {:error, {:invalid_env, @env_name, "server and client TLS sections"}}
    end
  end

  defp require_option(options, key, expected) do
    if Keyword.get(options, key) == expected,
      do: :ok,
      else: {:error, {:invalid_env, @env_name, "#{key}=#{expected}"}}
  end

  defp require_credentials(options) do
    Enum.reduce_while([:certfile, :keyfile, :cacertfile], :ok, fn key, :ok ->
      case Keyword.get(options, key) do
        path when is_list(path) -> validate_credential_path(List.to_string(path), key)
        path when is_binary(path) -> validate_credential_path(path, key)
        _missing -> {:halt, {:error, {:invalid_env, @env_name, "#{key} readable"}}}
      end
    end)
  end

  defp validate_credential_path(path, key) do
    if Path.type(path) == :absolute and File.regular?(path),
      do: {:cont, :ok},
      else: {:halt, {:error, {:invalid_env, @env_name, "#{key} absolute readable"}}}
  end

  defp init_argument(name, expected) do
    values =
      case :init.get_argument(name) do
        {:ok, groups} -> groups |> List.flatten() |> Enum.map(&List.to_string/1)
        :error -> []
      end

    if expected in values,
      do: :ok,
      else: {:error, {:invalid_env, "ERL_FLAGS", "-#{name} #{expected}"}}
  end
end
