defmodule Favn.Dev.Build.RunnerConfig do
  @moduledoc false

  @config_keys [
    :connection_modules,
    :connections,
    :execution_pools,
    :runner_plugins,
    :duckdb_in_process_client,
    :duckdb_adbc
  ]

  @type compile_env_entry :: {atom(), atom(), term()}
  @type t :: %{favn: keyword(), compile_env: [compile_env_entry()]}

  @spec collect(keyword()) :: {:ok, t()} | {:error, term()}
  def collect(opts) when is_list(opts) do
    root_dir = Keyword.get(opts, :root_dir, File.cwd!()) |> Path.expand()

    config =
      Enum.flat_map(@config_keys, fn key ->
        case Application.fetch_env(:favn, key) do
          {:ok, value} -> [{key, value}]
          :error -> []
        end
      end)

    with {:ok, config} <- validate(config, root_dir) do
      {:ok, %{favn: config, compile_env: []}}
    end
  end

  @spec finalize(t(), [map()], keyword()) :: {:ok, t()} | {:error, term()}
  def finalize(%{favn: favn} = config, applications, opts)
      when is_list(applications) and is_list(opts) do
    root_dir = Keyword.get(opts, :root_dir, File.cwd!()) |> Path.expand()

    with {:ok, compile_env} <- compile_env_entries(applications),
         {:ok, compile_env} <- validate_compile_env(compile_env, root_dir) do
      {:ok, %{config | favn: favn, compile_env: compile_env}}
    end
  end

  @spec fingerprint(t()) :: String.t()
  def fingerprint(%{favn: favn, compile_env: compile_env}) do
    canonical = %{favn: favn, compile_env: compile_env}

    canonical
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp compile_env_entries(applications) do
    applications
    |> Enum.map(&String.to_atom(&1.application))
    |> Enum.flat_map(&application_compile_env/1)
    |> Enum.reduce_while({:ok, %{}}, fn
      {app, [key | _path], {:ok, _compiled_value}}, {:ok, entries} when is_atom(key) ->
        case Application.fetch_env(app, key) do
          {:ok, value} -> {:cont, {:ok, Map.put(entries, {app, key}, value)}}
          :error -> {:halt, {:error, {:compile_env_unavailable, app, key}}}
        end

      {_app, _path, :error}, result ->
        {:cont, result}

      {app, _path, _value}, _result ->
        {:halt, {:error, {:invalid_compile_env_record, app}}}
    end)
    |> case do
      {:ok, entries} ->
        {:ok,
         entries
         |> Enum.map(fn {{app, key}, value} -> {app, key, value} end)
         |> Enum.sort_by(fn {app, key, _value} -> {Atom.to_string(app), Atom.to_string(key)} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp application_compile_env(app) do
    app_file =
      Path.join([Mix.Project.build_path(), "lib", Atom.to_string(app), "ebin", "#{app}.app"])

    case :file.consult(String.to_charlist(app_file)) do
      {:ok, [{:application, ^app, properties}]} ->
        Enum.map(Keyword.get(properties, :compile_env, []), fn {owner, path, value} ->
          {owner, path, value}
        end)

      _invalid ->
        []
    end
  end

  defp validate_compile_env(entries, root_dir) do
    case Enum.find_value(entries, fn {app, key, value} ->
           unsafe_config(value, [key, app], root_dir)
         end) do
      nil -> {:ok, entries}
      reason -> {:error, reason}
    end
  rescue
    _error -> {:error, :unsupported_runner_config_term}
  end

  defp validate(config, root_dir) do
    case Enum.find_value(config, fn {key, value} -> unsafe_config(value, [key], root_dir) end) do
      nil -> {:ok, config}
      reason -> {:error, reason}
    end
  rescue
    _error -> {:error, :unsupported_runner_config_term}
  end

  defp unsafe_config(%Favn.RuntimeConfig.Ref{}, _path, _root_dir), do: nil

  defp unsafe_config(%Favn.RuntimeValue.Ref{request: request}, path, root_dir),
    do: unsafe_config(request, [:request | path], root_dir)

  defp unsafe_config(value, path, root_dir) when is_binary(value) do
    cond do
      String.contains?(value, root_dir) -> {:non_relocatable_runner_config, Enum.reverse(path)}
      sensitive_key?(hd(path)) -> {:secret_literal_in_runner_config, Enum.reverse(path)}
      true -> nil
    end
  end

  defp unsafe_config(value, path, root_dir) when is_list(value) do
    Enum.find_value(value, fn
      {key, child} -> unsafe_config(child, [key | path], root_dir)
      child -> unsafe_config(child, path, root_dir)
    end)
  end

  defp unsafe_config(value, path, root_dir) when is_map(value) do
    Enum.find_value(value, fn {key, child} -> unsafe_config(child, [key | path], root_dir) end)
  end

  defp unsafe_config(value, path, root_dir) when is_tuple(value) do
    value |> Tuple.to_list() |> unsafe_config(path, root_dir)
  end

  defp unsafe_config(value, _path, _root_dir)
       when is_atom(value) or is_number(value) or is_boolean(value) or is_nil(value),
       do: nil

  defp unsafe_config(_value, path, _root_dir),
    do: {:unsupported_runner_config_term, Enum.reverse(path)}

  defp sensitive_key?(key) do
    normalized = key |> to_string() |> String.downcase()

    Enum.any?(
      ["password", "secret", "token", "credential", "api_key", "access_key", "private_key"],
      &String.contains?(normalized, &1)
    )
  end
end
