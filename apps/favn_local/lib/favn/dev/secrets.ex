defmodule Favn.Dev.Secrets do
  @moduledoc false

  alias Favn.Dev.Config

  @local_rpc_cookie "FAVN_LOCAL_DEV_RPC_COOKIE"

  @type root_opt :: [root_dir: Path.t()]

  @spec resolve(Config.t(), root_opt()) :: {:ok, map()} | {:error, term()}
  def resolve(%Config{} = config, opts \\ []) when is_list(opts) do
    _ = opts

    {:ok,
     %{
       "service_token" => config.service_token || random_secret(24),
       "web_session_secret" => config.web_session_secret || random_secret(32),
       "rpc_cookie" => @local_rpc_cookie
      }}
  end

  @spec random_secret(pos_integer()) :: String.t()
  defp random_secret(size) when is_integer(size) and size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

end
