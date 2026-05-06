defmodule Favn.Dev.ConsumerConfigTransport do
  @moduledoc false

  alias Favn.Dev.Paths

  @supported_keys [
    :connection_modules,
    :connections,
    :runner_plugins,
    :duckdb_in_process_client,
    :duckdb_adbc
  ]

  @sensitive_top_level_keys [
    :connections,
    :runner_plugins,
    :duckdb_in_process_client,
    :duckdb_adbc
  ]
  @schema_version 1
  @max_payload_bytes 1_048_576
  @max_collection_items 2_000
  @max_decode_depth 32
  @max_module_atom_bytes 255
  @max_local_atom_bytes 128
  @module_atom_pattern ~r/^Elixir(\.[A-Z][A-Za-z0-9_]*)+$/
  @local_atom_pattern ~r/^[A-Za-z_][A-Za-z0-9_]*[?!]?$/
  @transport_key_atoms %{
    "connection_modules" => :connection_modules,
    "connections" => :connections,
    "runner_plugins" => :runner_plugins,
    "duckdb_in_process_client" => :duckdb_in_process_client,
    "duckdb_adbc" => :duckdb_adbc
  }

  @type transport_error :: :invalid_base64 | :invalid_payload | {:unsupported_key, term()}

  @spec supported_keys() :: [atom()]
  def supported_keys, do: @supported_keys

  @spec collect(keyword()) :: keyword()
  def collect(opts) when is_list(opts) do
    root_dir = Paths.root_dir(opts)

    @supported_keys
    |> Enum.flat_map(fn key ->
      case Application.fetch_env(:favn, key) do
        {:ok, value} -> [{key, normalize_config(key, value, root_dir)}]
        :error -> []
      end
    end)
  end

  @spec encode(keyword()) :: String.t()
  def encode(config) when is_list(config) do
    config
    |> schema_payload()
    |> :erlang.term_to_binary()
    |> Base.encode64()
  end

  @spec collect_and_encode(keyword()) :: String.t()
  def collect_and_encode(opts) when is_list(opts), do: opts |> collect() |> encode()

  @spec decode(String.t()) :: {:ok, keyword()} | {:error, transport_error()}
  def decode(encoded) when is_binary(encoded) do
    with {:ok, binary} <- decode64(encoded),
         {:ok, payload} <- safe_binary_to_term(binary) do
      decode_payload(payload)
    end
  end

  @spec apply_encoded(String.t()) :: :ok | {:error, transport_error()}
  def apply_encoded(encoded) when is_binary(encoded) do
    with {:ok, config} <- decode(encoded) do
      Enum.each(config, fn {key, value} -> Application.put_env(:favn, key, value) end)
      :ok
    end
  end

  @spec redact(term()) :: term()
  def redact(config) when is_list(config) do
    Enum.map(config, fn
      {key, _value} when key in @sensitive_top_level_keys -> {key, "[REDACTED]"}
      {key, value} -> {key, redact(value)}
      other -> redact(other)
    end)
  end

  def redact(config) when is_map(config) do
    Map.new(config, fn {key, value} -> {key, redact_map_value(key, value)} end)
  end

  def redact(config) when is_tuple(config) do
    config |> Tuple.to_list() |> Enum.map(&redact/1) |> List.to_tuple()
  end

  def redact(config), do: config

  @spec bootstrap_eval_snippet() :: String.t()
  def bootstrap_eval_snippet do
    """
    defmodule Favn.Dev.ConsumerConfigBootstrap do
      @max_payload_bytes 1_048_576
      @max_collection_items 2_000
      @max_decode_depth 32
      @max_module_atom_bytes 255
      @max_local_atom_bytes 128
      @module_atom_pattern ~r/^Elixir(\.[A-Z][A-Za-z0-9_]*)+$/
      @local_atom_pattern ~r/^[A-Za-z_][A-Za-z0-9_]*[?!]?$/
      @transport_key_atoms %{
        "connection_modules" => :connection_modules,
        "connections" => :connections,
        "runner_plugins" => :runner_plugins,
        "duckdb_in_process_client" => :duckdb_in_process_client,
        "duckdb_adbc" => :duckdb_adbc
      }

      def apply_from_env do
        case System.get_env("FAVN_DEV_CONSUMER_FAVN_CONFIG", "") do
          "" ->
            :ok

          encoded ->
            with {:ok, binary} <- decode64(encoded),
                  {:ok, payload} <- safe_binary_to_term(binary),
                  {:ok, config} <- decode_payload(payload) do
              Enum.each(config, fn {key, value} -> Application.put_env(:favn, key, value) end)
              :ok
            else
              {:error, reason} -> {:error, reason}
              _other -> {:error, :invalid_payload}
            end
        end
      end

      defp decode64(encoded) do
        case Base.decode64(encoded) do
          {:ok, binary} when byte_size(binary) <= @max_payload_bytes -> {:ok, binary}
          {:ok, _binary} -> {:error, :invalid_payload}
          :error -> {:error, :invalid_base64}
        end
      end

      defp safe_binary_to_term(binary) do
        {:ok, :erlang.binary_to_term(binary, [:safe])}
      rescue
        _error -> {:error, :invalid_payload}
      end

      defp decode_payload(%{"schema_version" => 1, "entries" => entries}) when is_list(entries) do
        if length(entries) <= map_size(@transport_key_atoms) do
          decode_entries(entries, [])
        else
          {:error, :invalid_payload}
        end
      end

      defp decode_payload(_payload), do: {:error, :invalid_payload}

      defp decode_entries([], acc), do: {:ok, Enum.reverse(acc)}

      defp decode_entries([%{"key" => key, "value" => value} | rest], acc) when is_binary(key) do
        with {:ok, key_atom} <- fetch_transport_key(key),
             {:ok, decoded} <- decode_value(value) do
          decode_entries(rest, [{key_atom, decoded} | acc])
        end
      end

      defp decode_entries([%{"key" => key} | _rest], _acc) when is_binary(key),
        do: {:error, {:unsupported_key, key}}

      defp decode_entries(_entries, _acc), do: {:error, :invalid_payload}

      defp fetch_transport_key(key) do
        case Map.fetch(@transport_key_atoms, key) do
          {:ok, key_atom} -> {:ok, key_atom}
          :error -> {:error, {:unsupported_key, key}}
        end
      end

      defp decode_value(value), do: decode_value(value, @max_decode_depth)

      defp decode_value(_value, depth) when depth < 0, do: {:error, :invalid_payload}

      defp decode_value(%{"$type" => "atom", "kind" => "module", "value" => value}, _depth)
           when is_binary(value) do
        if module_atom_name?(value) do
          {:ok, String.to_atom(value)}
        else
          {:error, :invalid_payload}
        end
      end

      defp decode_value(%{"$type" => "atom", "kind" => "local", "value" => value}, _depth)
           when is_binary(value) do
        if local_atom_name?(value) do
          {:ok, String.to_atom(value)}
        else
          {:error, :invalid_payload}
        end
      end

      defp decode_value(%{"$type" => "atom", "kind" => "existing", "value" => value}, _depth)
           when is_binary(value) do
        if byte_size(value) <= @max_module_atom_bytes do
          {:ok, String.to_existing_atom(value)}
        else
          {:error, :invalid_payload}
        end
      rescue
        ArgumentError -> {:error, :invalid_payload}
      end

      defp decode_value(%{"$type" => "tuple", "items" => items}, depth) when is_list(items) do
        with :ok <- validate_collection_size(items),
             {:ok, decoded} <- decode_list(items, depth - 1) do
          {:ok, List.to_tuple(decoded)}
        end
      end

      defp decode_value(%{"$type" => "list", "items" => items}, depth) when is_list(items) do
        with :ok <- validate_collection_size(items), do: decode_list(items, depth - 1)
      end

      defp decode_value(%{"$type" => "map", "entries" => entries}, depth) when is_list(entries) do
        with :ok <- validate_collection_size(entries) do
          entries
          |> Enum.reduce_while({:ok, []}, fn
            %{"key" => key, "value" => value}, {:ok, acc} ->
              with {:ok, decoded_key} <- decode_value(key, depth - 1),
                   {:ok, decoded_value} <- decode_value(value, depth - 1) do
                {:cont, {:ok, [{decoded_key, decoded_value} | acc]}}
              else
                {:error, reason} -> {:halt, {:error, reason}}
              end

            _entry, _acc ->
              {:halt, {:error, :invalid_payload}}
          end)
          |> case do
            {:ok, pairs} -> {:ok, Map.new(pairs)}
            {:error, reason} -> {:error, reason}
          end
        end
      end

      defp decode_value(value, _depth)
           when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
        do: {:ok, value}

      defp decode_value(_value, _depth), do: {:error, :invalid_payload}

      defp decode_list(values, depth) do
        Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
          case decode_value(value, depth) do
            {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
          {:error, reason} -> {:error, reason}
        end
      end

      defp validate_collection_size(values) when length(values) <= @max_collection_items, do: :ok
      defp validate_collection_size(_values), do: {:error, :invalid_payload}

      defp module_atom_name?(value) do
        byte_size(value) in 1..@max_module_atom_bytes and String.match?(value, @module_atom_pattern)
      end

      defp local_atom_name?(value) do
        byte_size(value) in 1..@max_local_atom_bytes and String.match?(value, @local_atom_pattern)
      end
    end

    case Favn.Dev.ConsumerConfigBootstrap.apply_from_env() do
      :ok -> :ok
      {:error, reason} -> raise "invalid FAVN_DEV_CONSUMER_FAVN_CONFIG: \#{inspect(reason)}"
    end
    """
    |> String.trim()
  end

  defp schema_payload(config) do
    entries =
      Enum.flat_map(config, fn
        {key, value} when key in @supported_keys ->
          [%{"key" => Atom.to_string(key), "value" => encode_value(value)}]

        {_key, _value} ->
          []
      end)

    %{"schema_version" => @schema_version, "entries" => entries}
  end

  defp encode_value(value) when is_boolean(value) or is_nil(value), do: value

  defp encode_value(value) when is_atom(value) do
    value = Atom.to_string(value)

    %{"$type" => "atom", "kind" => atom_value_kind(value), "value" => value}
  end

  defp encode_value(value) when is_tuple(value) do
    %{"$type" => "tuple", "items" => value |> Tuple.to_list() |> Enum.map(&encode_value/1)}
  end

  defp encode_value(value) when is_list(value) do
    %{"$type" => "list", "items" => Enum.map(value, &encode_value/1)}
  end

  defp encode_value(value) when is_map(value) do
    entries =
      Enum.map(value, fn {key, map_value} ->
        %{"key" => encode_value(key), "value" => encode_value(map_value)}
      end)

    %{"$type" => "map", "entries" => entries}
  end

  defp encode_value(value)
       when is_binary(value) or is_number(value),
       do: value

  defp decode64(encoded) do
    case Base.decode64(encoded) do
      {:ok, binary} when byte_size(binary) <= @max_payload_bytes -> {:ok, binary}
      {:ok, _binary} -> {:error, :invalid_payload}
      :error -> {:error, :invalid_base64}
    end
  end

  defp safe_binary_to_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    _error -> {:error, :invalid_payload}
  end

  defp decode_payload(%{"schema_version" => @schema_version, "entries" => entries})
       when is_list(entries) do
    if length(entries) <= map_size(@transport_key_atoms) do
      decode_entries(entries, [])
    else
      {:error, :invalid_payload}
    end
  end

  defp decode_payload(_payload), do: {:error, :invalid_payload}

  defp decode_entries([], acc), do: {:ok, Enum.reverse(acc)}

  defp decode_entries([%{"key" => key, "value" => value} | rest], acc) when is_binary(key) do
    with {:ok, key_atom} <- fetch_transport_key(key),
         {:ok, decoded} <- decode_value(value) do
      decode_entries(rest, [{key_atom, decoded} | acc])
    end
  end

  defp decode_entries([%{"key" => key} | _rest], _acc) when is_binary(key),
    do: {:error, {:unsupported_key, key}}

  defp decode_entries(_entries, _acc), do: {:error, :invalid_payload}

  defp fetch_transport_key(key) do
    case Map.fetch(@transport_key_atoms, key) do
      {:ok, key_atom} -> {:ok, key_atom}
      :error -> {:error, {:unsupported_key, key}}
    end
  end

  defp decode_value(value), do: decode_value(value, @max_decode_depth)

  defp decode_value(_value, depth) when depth < 0, do: {:error, :invalid_payload}

  defp decode_value(%{"$type" => "atom", "kind" => "module", "value" => value}, _depth)
       when is_binary(value) do
    if module_atom_name?(value) do
      {:ok, String.to_atom(value)}
    else
      {:error, :invalid_payload}
    end
  end

  defp decode_value(%{"$type" => "atom", "kind" => "local", "value" => value}, _depth)
       when is_binary(value) do
    if local_atom_name?(value) do
      {:ok, String.to_atom(value)}
    else
      {:error, :invalid_payload}
    end
  end

  defp decode_value(%{"$type" => "atom", "kind" => "existing", "value" => value}, _depth)
       when is_binary(value) do
    if byte_size(value) <= @max_module_atom_bytes do
      {:ok, String.to_existing_atom(value)}
    else
      {:error, :invalid_payload}
    end
  rescue
    ArgumentError -> {:error, :invalid_payload}
  end

  defp decode_value(%{"$type" => "tuple", "items" => items}, depth) when is_list(items) do
    with :ok <- validate_collection_size(items),
         {:ok, decoded} <- decode_list(items, depth - 1) do
      {:ok, List.to_tuple(decoded)}
    end
  end

  defp decode_value(%{"$type" => "list", "items" => items}, depth) when is_list(items) do
    with :ok <- validate_collection_size(items), do: decode_list(items, depth - 1)
  end

  defp decode_value(%{"$type" => "map", "entries" => entries}, depth) when is_list(entries) do
    with :ok <- validate_collection_size(entries) do
      entries
      |> Enum.reduce_while({:ok, []}, fn
        %{"key" => key, "value" => value}, {:ok, acc} ->
          with {:ok, decoded_key} <- decode_value(key, depth - 1),
               {:ok, decoded_value} <- decode_value(value, depth - 1) do
            {:cont, {:ok, [{decoded_key, decoded_value} | acc]}}
          else
            {:error, reason} -> {:halt, {:error, reason}}
          end

        _entry, _acc ->
          {:halt, {:error, :invalid_payload}}
      end)
      |> case do
        {:ok, pairs} -> {:ok, Map.new(pairs)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp decode_value(value, _depth)
       when is_binary(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: {:ok, value}

  defp decode_value(_value, _depth), do: {:error, :invalid_payload}

  defp decode_list(values, depth) do
    values
    |> Enum.reduce_while({:ok, []}, fn value, {:ok, acc} ->
      case decode_value(value, depth) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_collection_size(values) when length(values) <= @max_collection_items, do: :ok
  defp validate_collection_size(_values), do: {:error, :invalid_payload}

  defp atom_value_kind(value) do
    cond do
      module_atom_name?(value) -> "module"
      local_atom_name?(value) -> "local"
      true -> "existing"
    end
  end

  defp module_atom_name?(value) do
    byte_size(value) in 1..@max_module_atom_bytes and String.match?(value, @module_atom_pattern)
  end

  defp local_atom_name?(value) do
    byte_size(value) in 1..@max_local_atom_bytes and String.match?(value, @local_atom_pattern)
  end

  defp normalize_config(:connections, connections, root_dir) do
    cond do
      Keyword.keyword?(connections) ->
        Keyword.new(connections, fn {name, config} ->
          {name, absolutize_connection_paths(config, root_dir)}
        end)

      is_map(connections) ->
        Map.new(connections, fn {name, config} ->
          {name, absolutize_connection_paths(config, root_dir)}
        end)

      true ->
        connections
    end
  end

  defp normalize_config(_key, value, _root_dir), do: value

  defp absolutize_connection_paths(config, root_dir) when is_list(config) do
    if Keyword.has_key?(config, :database) do
      Keyword.update!(config, :database, &expand_relative_path(&1, root_dir))
    else
      config
    end
  end

  defp absolutize_connection_paths(config, root_dir) when is_map(config) do
    if Map.has_key?(config, :database) do
      Map.update!(config, :database, &expand_relative_path(&1, root_dir))
    else
      config
    end
  end

  defp absolutize_connection_paths(config, _root_dir), do: config

  defp expand_relative_path(path, root_dir) when is_binary(path) do
    case Path.type(path) do
      :relative -> Path.expand(path, root_dir)
      _other -> path
    end
  end

  defp expand_relative_path(path, _root_dir), do: path

  defp redact_map_value(key, value) do
    if sensitive_key?(key), do: "[REDACTED]", else: redact(value)
  end

  defp sensitive_key?(key) when is_atom(key), do: key |> Atom.to_string() |> sensitive_key?()

  defp sensitive_key?(key) when is_binary(key) do
    normalized = String.downcase(key)

    Enum.any?(
      ~w(password passwd pwd token secret database_url connection_string url),
      &String.contains?(normalized, &1)
    )
  end

  defp sensitive_key?(_key), do: false
end
