defmodule Favn.Dev.Secrets do
  @moduledoc false

  alias Favn.Dev.Config
  alias Favn.Dev.State

  @schema_version 2

  @type root_opt :: [root_dir: Path.t()]

  @spec resolve(Config.t(), root_opt()) :: {:ok, map()} | {:error, term()}
  def resolve(%Config{}, opts \\ []) when is_list(opts) do
    with :ok <- State.ensure_layout(opts),
         {:ok, stored} <- read_or_initialize(opts),
         :ok <- validate(stored),
         :ok <- persist(stored, opts) do
      {:ok, Map.drop(stored, ["schema_version"])}
    end
  end

  defp read_or_initialize(opts) do
    case State.read_secrets(opts) do
      {:ok, %{"schema_version" => @schema_version} = secrets} ->
        {:ok, complete_secrets(secrets)}

      {:ok, %{"schema_version" => 1} = secrets} ->
        {:ok, secrets |> Map.put("schema_version", @schema_version) |> complete_secrets()}

      {:ok, _invalid} ->
        {:error, :invalid_local_secrets}

      {:error, :not_found} ->
        {:ok, new_secrets()}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp new_secrets do
    %{
      "schema_version" => @schema_version,
      "service_token" => random_secret(24),
      "web_session_secret" => random_secret(48),
      "rpc_cookie" => random_cookie(32),
      "runtime_input_pin_key" => runtime_input_pin_key(),
      "postgres_admin_password" => random_secret(32),
      "postgres_runtime_password" => random_secret(32),
      "bootstrap_password" => random_secret(32)
    }
  end

  defp complete_secrets(secrets) do
    secrets
    |> Map.put_new_lazy("runtime_input_pin_key", &runtime_input_pin_key/0)
    |> Map.put_new_lazy("postgres_admin_password", fn -> random_secret(32) end)
    |> Map.put_new_lazy("postgres_runtime_password", fn -> random_secret(32) end)
    |> Map.put_new_lazy("bootstrap_password", fn -> random_secret(32) end)
  end

  defp validate(secrets) do
    with token when is_binary(token) and token != "" <- secrets["service_token"],
         session when is_binary(session) and byte_size(session) >= 32 <-
           secrets["web_session_secret"],
         {:ok, pin_key} <- Base.decode64(secrets["runtime_input_pin_key"]),
         32 <- byte_size(pin_key),
         admin when is_binary(admin) and byte_size(admin) >= 32 <-
           secrets["postgres_admin_password"],
         runtime when is_binary(runtime) and byte_size(runtime) >= 32 <-
           secrets["postgres_runtime_password"],
         bootstrap when is_binary(bootstrap) and byte_size(bootstrap) >= 15 <-
           secrets["bootstrap_password"],
         cookie when is_binary(cookie) <- secrets["rpc_cookie"],
         true <- valid_cookie?(cookie) do
      :ok
    else
      _invalid -> {:error, :invalid_local_secrets}
    end
  end

  defp valid_cookie?(cookie) do
    byte_size(cookie) in 1..255 and String.match?(cookie, ~r/\A[A-Za-z0-9_]+\z/)
  end

  defp persist(secrets, opts) do
    case State.read_secrets(opts) do
      {:ok, ^secrets} -> :ok
      _other -> State.write_secrets(secrets, opts)
    end
  end

  defp random_cookie(size) do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.encode32(padding: false)
  end

  defp random_secret(size) when is_integer(size) and size > 0 do
    size
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp runtime_input_pin_key do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.encode64()
  end
end
