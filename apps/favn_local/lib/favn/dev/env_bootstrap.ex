defmodule Favn.Dev.EnvBootstrap do
  @moduledoc false

  alias Favn.Dev.EnvFile
  alias Favn.Dev.Paths

  @token_env "FAVN_INTERNAL_ENV_BOOTSTRAP"
  @schema_version 1
  @max_token_bytes 65_536
  @max_path_bytes 4_096
  @max_loaded_keys 512
  @max_key_bytes 256
  @key_pattern ~r/^[A-Za-z_][A-Za-z0-9_]*$/
  @commands [:dev, :inspect, :query, :reload]
  @command_names Enum.map(@commands, &Atom.to_string/1)
  @configured_tasks %{
    dev: "favn.dev.configured",
    inspect: "favn.inspect.configured",
    query: "favn.query.configured",
    reload: "favn.reload.configured"
  }
  @bootstrap_control_env [@token_env, "FAVN_ENV_FILE", "MIX_ENV"]

  defstruct [:env_file_path, loaded: %{}]

  @type command :: :dev | :inspect | :query | :reload
  @type t :: %__MODULE__{
          env_file_path: Path.t(),
          loaded: %{optional(String.t()) => String.t()}
        }

  @spec exec(command(), [String.t()], keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def exec(command, args, opts)
      when command in @commands and is_list(args) and is_list(opts) do
    command_runner =
      Keyword.get(opts, :env_bootstrap_command_runner, &System.cmd/3)

    opts = Keyword.delete(opts, :env_bootstrap_command_runner)

    with {:ok, token} <- prepare_token(command, opts),
         {:ok, mix} <- mix_executable(),
         {:ok, status} <- run_configured(command_runner, mix, command, args, token) do
      {:ok, status}
    end
  end

  @doc false
  @spec install_for_current_process(command(), keyword()) :: :ok | {:error, term()}
  def install_for_current_process(command, opts)
      when command in @commands and is_list(opts) do
    System.delete_env(@token_env)

    with {:ok, token} <- prepare_token(command, opts) do
      System.put_env(@token_env, token)
      :ok
    end
  end

  @spec consume(command(), keyword()) :: {:ok, keyword()} | {:error, term()}
  def consume(command, opts) when command in @commands and is_list(opts) do
    token = System.get_env(@token_env)
    System.delete_env(@token_env)

    with {:ok, payload} <- decode_token(token),
         :ok <- validate_payload(payload, command, opts),
         {:ok, loaded} <- collect_loaded_env(payload["loaded_keys"]) do
      bootstrap = %__MODULE__{
        env_file_path: payload["env_file_path"],
        loaded: loaded
      }

      {:ok,
       opts
       |> Keyword.put(:env_bootstrap, bootstrap)
       |> Keyword.put(:env_file_loaded, loaded)}
    end
  end

  @spec ensure_loaded(keyword()) :: {:ok, keyword()} | {:error, term()}
  def ensure_loaded(opts) when is_list(opts) do
    case Keyword.get(opts, :env_bootstrap) do
      %__MODULE__{loaded: loaded} when is_map(loaded) ->
        {:ok, Keyword.put(opts, :env_file_loaded, loaded)}

      nil ->
        with {:ok, env_file} <- EnvFile.load(opts) do
          bootstrap = %__MODULE__{
            env_file_path: Path.expand(env_file.path),
            loaded: env_file.effective
          }

          {:ok,
           opts
           |> Keyword.put(:env_bootstrap, bootstrap)
           |> Keyword.put(:env_file_loaded, env_file.effective)}
        end

      _other ->
        {:error, {:invalid_env_bootstrap, :invalid_loaded_state}}
    end
  end

  defp prepare_token(command, opts) do
    with {:ok, env_file} <- load_env_file(opts) do
      payload = %{
        "schema_version" => @schema_version,
        "command" => Atom.to_string(command),
        "root_dir" => opts |> Paths.root_dir() |> Path.expand(),
        "env_file_path" => Path.expand(env_file.path),
        "loaded_keys" => env_file.effective |> Map.keys() |> Enum.sort()
      }

      with :ok <- validate_payload_shape(payload),
           {:ok, token} <- encode_payload(payload) do
        {:ok, token}
      end
    end
  end

  defp load_env_file(opts) do
    previous_control_env =
      Map.new(@bootstrap_control_env, fn key -> {key, System.get_env(key)} end)

    System.delete_env(@token_env)
    result = EnvFile.load(opts)

    Enum.each(previous_control_env, fn
      {@token_env, _value} -> System.delete_env(@token_env)
      {key, nil} -> System.delete_env(key)
      {key, value} -> System.put_env(key, value)
    end)

    case result do
      {:ok, env_file} ->
        {:ok,
         %{
           env_file
           | loaded: Map.drop(env_file.loaded, @bootstrap_control_env),
             effective: Map.drop(env_file.effective, @bootstrap_control_env)
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp encode_payload(payload) do
    token = payload |> :erlang.term_to_binary() |> Base.url_encode64(padding: false)

    if byte_size(token) <= @max_token_bytes do
      {:ok, token}
    else
      {:error, {:invalid_env_bootstrap, :token_too_large}}
    end
  end

  defp run_configured(command_runner, mix, command, args, token)
       when is_function(command_runner, 3) do
    command_args = [Map.fetch!(@configured_tasks, command) | args]

    command_opts = [
      env: [{@token_env, token}, {"MIX_ENV", Atom.to_string(Mix.env())}],
      stderr_to_stdout: true,
      into: IO.stream(:stdio, :line)
    ]

    case command_runner.(mix, command_args, command_opts) do
      {_output, status} when is_integer(status) and status >= 0 ->
        {:ok, status}

      other ->
        {:error, {:env_bootstrap_command_failed, other}}
    end
  rescue
    error -> {:error, {:env_bootstrap_command_failed, Exception.message(error)}}
  end

  defp mix_executable do
    case System.find_executable("mix") do
      nil -> {:error, {:missing_tool, "mix"}}
      executable -> {:ok, executable}
    end
  end

  defp decode_token(nil), do: {:error, :env_bootstrap_required}

  defp decode_token(token) when is_binary(token) and byte_size(token) <= @max_token_bytes do
    with {:ok, binary} <- Base.url_decode64(token, padding: false) do
      decode_payload(binary)
    else
      :error -> {:error, {:invalid_env_bootstrap, :invalid_encoding}}
    end
  rescue
    ArgumentError -> {:error, {:invalid_env_bootstrap, :invalid_payload}}
  end

  defp decode_token(_token), do: {:error, {:invalid_env_bootstrap, :token_too_large}}

  defp decode_payload(<<131, 80, _compressed::binary>>),
    do: {:error, {:invalid_env_bootstrap, :compressed_payload}}

  defp decode_payload(binary), do: {:ok, :erlang.binary_to_term(binary, [:safe])}

  defp validate_payload(payload, command, opts) do
    expected_root = opts |> Paths.root_dir() |> Path.expand()
    expected_env_file = opts |> EnvFile.env_file_path() |> Path.expand()

    with :ok <- validate_payload_shape(payload),
         true <- payload["command"] == Atom.to_string(command),
         true <- payload["root_dir"] == expected_root,
         true <- payload["env_file_path"] == expected_env_file do
      :ok
    else
      {:error, _reason} = error -> error
      false -> {:error, {:invalid_env_bootstrap, :context_mismatch}}
    end
  end

  defp validate_payload_shape(
         %{
           "schema_version" => @schema_version,
           "command" => command,
           "root_dir" => root_dir,
           "env_file_path" => env_file_path,
           "loaded_keys" => loaded_keys
         } = payload
       )
       when map_size(payload) == 5 and command in @command_names and
              is_binary(root_dir) and byte_size(root_dir) <= @max_path_bytes and
              is_binary(env_file_path) and byte_size(env_file_path) <= @max_path_bytes and
              is_list(loaded_keys) and length(loaded_keys) <= @max_loaded_keys do
    if loaded_keys == Enum.sort(Enum.uniq(loaded_keys)) and Enum.all?(loaded_keys, &valid_key?/1) do
      :ok
    else
      {:error, {:invalid_env_bootstrap, :invalid_loaded_keys}}
    end
  end

  defp validate_payload_shape(_payload),
    do: {:error, {:invalid_env_bootstrap, :invalid_payload}}

  defp valid_key?(key) when is_binary(key) and byte_size(key) <= @max_key_bytes,
    do: Regex.match?(@key_pattern, key)

  defp valid_key?(_key), do: false

  defp collect_loaded_env(keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, loaded} ->
      case System.fetch_env(key) do
        {:ok, value} -> {:cont, {:ok, Map.put(loaded, key, value)}}
        :error -> {:halt, {:error, {:invalid_env_bootstrap, {:missing_loaded_env, key}}}}
      end
    end)
  end
end
