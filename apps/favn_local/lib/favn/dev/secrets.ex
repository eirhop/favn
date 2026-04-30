defmodule Favn.Dev.Secrets do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.State

  @type root_opt :: [root_dir: Path.t()]

  @spec resolve(Config.t(), root_opt()) :: {:ok, map()} | {:error, term()}
  def resolve(%Config{} = config, opts \\ []) when is_list(opts) do
    with {:ok, stored} <- read_stored(opts) do
      secrets = %{
        "service_token" => config.service_token || stored["service_token"] || random_secret(24),
        "web_session_secret" =>
          config.web_session_secret || stored["web_session_secret"] || random_secret(32),
        "rpc_cookie" => normalize_rpc_cookie(stored["rpc_cookie"]) || random_rpc_cookie(24),
        "local_operator_username" => stored["local_operator_username"] || "favn-local-operator",
        "local_operator_password" => stored["local_operator_password"] || random_secret(24)
      }

      case State.write_secrets(secrets, opts) do
        :ok -> {:ok, secrets}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp read_stored(opts) do
    case State.read_secrets(opts) do
      {:ok, secrets} -> {:ok, secrets}
      {:error, :not_found} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec random_secret(pos_integer()) :: String.t()
  defp random_secret(size) when is_integer(size) and size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp random_rpc_cookie(size) when is_integer(size) and size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :upper)
  end

  defp normalize_rpc_cookie(cookie) when is_binary(cookie) and cookie != "" do
    if String.match?(cookie, ~r/^[A-Za-z0-9_]+$/) do
      cookie
    else
      nil
    end
  end

  defp normalize_rpc_cookie(_cookie), do: nil
end
