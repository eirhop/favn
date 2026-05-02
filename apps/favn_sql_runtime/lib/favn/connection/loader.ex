defmodule Favn.Connection.Loader do
  @moduledoc false

  alias Favn.Connection.Definition
  alias Favn.Connection.Error
  alias Favn.Connection.Validator

  @spec load() :: {:ok, %{atom() => Favn.Connection.Resolved.t()}} | {:error, [Error.t()]}
  def load do
    with {:ok, modules} <- configured_modules(),
         {:ok, runtime_connections} <- configured_runtime_connections(),
         {:ok, definitions} <- load_definitions(modules),
         :ok <- validate_duplicate_names(definitions),
         :ok <- validate_unknown_runtime_connection_names(definitions, runtime_connections) do
      resolve_connections(definitions, runtime_connections)
    end
  end

  @spec resolve_required([atom()]) :: {:ok, %{atom() => Favn.Connection.Resolved.t()}} | {:error, [Error.t()]}
  def resolve_required(names) when is_list(names) do
    required_names = names |> Enum.filter(&is_atom/1) |> Enum.uniq() |> Enum.sort()

    with {:ok, modules} <- configured_modules(),
         {:ok, runtime_connections} <- configured_required_runtime_connections(required_names),
         {:ok, definitions} <- load_definitions(modules),
         selected_definitions <- select_required_definitions(definitions, required_names),
         :ok <- validate_missing_required_definitions(selected_definitions, required_names),
         :ok <- validate_duplicate_names(selected_definitions) do
      resolve_connections(selected_definitions, runtime_connections)
    end
  end

  @spec configured_modules() :: {:ok, [module()]} | {:error, [Error.t()]}
  def configured_modules do
    case Application.get_env(:favn, :connection_modules, []) do
      modules when is_list(modules) ->
        {:ok, Enum.uniq(modules)}

      other ->
        {:error,
         [%Error{type: :invalid_connection_modules, message: invalid_modules_message(other)}]}
    end
  end

  @spec configured_runtime_connections() :: {:ok, map()} | {:error, [Error.t()]}
  def configured_runtime_connections do
    case Application.get_env(:favn, :connections, []) do
      entries when is_list(entries) ->
        if Keyword.keyword?(entries) do
          normalize_keyword_connections(entries)
        else
          {:error,
           [
             %Error{
               type: :invalid_connections_config,
               message: "config :favn, :connections list must be a keyword list"
             }
           ]}
        end

      entries when is_map(entries) ->
        normalize_map_connections(entries)

      other ->
        {:error,
         [%Error{type: :invalid_connections_config, message: invalid_connections_message(other)}]}
    end
  end

  defp configured_required_runtime_connections(required_names) do
    required = MapSet.new(required_names)

    case Application.get_env(:favn, :connections, []) do
      entries when is_list(entries) ->
        if Keyword.keyword?(entries) do
          entries
          |> Enum.filter(fn {name, _values} -> MapSet.member?(required, name) end)
          |> normalize_keyword_connections()
        else
          {:error,
           [
             %Error{
               type: :invalid_connections_config,
               message: "config :favn, :connections list must be a keyword list"
             }
           ]}
        end

      entries when is_map(entries) ->
        entries
        |> Enum.filter(fn {name, _values} -> MapSet.member?(required, name) end)
        |> Map.new()
        |> normalize_map_connections()

      other ->
        {:error,
         [%Error{type: :invalid_connections_config, message: invalid_connections_message(other)}]}
    end
  end

  @spec load_definitions([module()]) :: {:ok, [Definition.t()]} | {:error, [Error.t()]}
  def load_definitions(modules) when is_list(modules) do
    {definitions, errors} =
      Enum.reduce(modules, {[], []}, fn module, {defs, errs} ->
        case load_definition(module) do
          {:ok, definition} -> {[definition | defs], errs}
          {:error, module_errors} -> {defs, module_errors ++ errs}
        end
      end)

    if errors == [], do: {:ok, Enum.reverse(definitions)}, else: {:error, Enum.reverse(errors)}
  end

  defp load_definition(module) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :definition, 0),
         :ok <- validate_behaviour(module) do
      definition = module.definition()

      case definition do
        %Definition{} = defn ->
          normalized = %Definition{defn | module: module}

          case Validator.validate_definition(normalized) do
            :ok -> {:ok, normalized}
            {:error, errors} -> {:error, errors}
          end

        _other ->
          {:error,
           [
             %Error{
               type: :invalid_definition,
               module: module,
               message:
                 "connection module #{inspect(module)} must return %Favn.Connection.Definition{}"
             }
           ]}
      end
    else
      {:error, reason} when is_atom(reason) ->
        {:error,
         [
           %Error{
             type: :invalid_module,
             module: module,
             message: "connection module #{inspect(module)} could not be loaded"
           }
         ]}

      false ->
        {:error,
         [
           %Error{
             type: :invalid_module,
             module: module,
             message: "connection module #{inspect(module)} must export definition/0"
           }
         ]}

      {:error, _errors} = error ->
        error
    end
  end

  defp load_definition(module) do
    {:error,
     [
       %Error{
         type: :invalid_module,
         message: "connection module entry must be an atom, got: #{inspect(module)}"
       }
     ]}
  end

  defp validate_behaviour(module) do
    behaviours =
      module
      |> module_attributes(:behaviour)
      |> List.flatten()

    if Favn.Connection in behaviours do
      :ok
    else
      {:error,
       [
         %Error{
           type: :invalid_module,
           module: module,
           message: "connection module #{inspect(module)} must declare @behaviour Favn.Connection"
         }
       ]}
    end
  end

  defp module_attributes(module, key) do
    module.module_info(:attributes)
    |> Keyword.get(key, [])
  rescue
    _ -> []
  end

  defp validate_unknown_runtime_connection_names(definitions, runtime_connections) do
    known_names = definitions |> Enum.map(& &1.name) |> MapSet.new()
    runtime_names = runtime_connections |> Map.keys() |> MapSet.new()

    unknown_names =
      runtime_names |> MapSet.difference(known_names) |> MapSet.to_list() |> Enum.sort()

    if unknown_names == [] do
      :ok
    else
      {:error,
       Enum.map(unknown_names, fn name ->
         %Error{
           type: :invalid_connections_config,
           connection: name,
           message: "runtime connection #{inspect(name)} has no matching provider definition"
         }
       end)}
    end
  end

  defp resolve_connections(definitions, runtime_connections) do
    {resolved_entries, errors} =
      Enum.reduce(definitions, {[], []}, fn definition, {entries, errs} ->
        runtime_values = Map.get(runtime_connections, definition.name, %{})

        case Validator.resolve(definition, runtime_values) do
          {:ok, resolved} -> {[{definition.name, resolved} | entries], errs}
          {:error, resolver_errors} -> {entries, resolver_errors ++ errs}
        end
      end)

    if errors == [] do
      {:ok, Map.new(resolved_entries)}
    else
      {:error, Enum.reverse(errors)}
    end
  end

  defp select_required_definitions(definitions, required_names) do
    required = MapSet.new(required_names)
    Enum.filter(definitions, &MapSet.member?(required, &1.name))
  end

  defp validate_missing_required_definitions(definitions, required_names) do
    found_names = definitions |> Enum.map(& &1.name) |> MapSet.new()

    missing_names =
      required_names
      |> Enum.reject(&MapSet.member?(found_names, &1))
      |> Enum.sort()

    if missing_names == [] do
      :ok
    else
      {:error,
       Enum.map(missing_names, fn name ->
         %Error{
           type: :missing_connection,
           connection: name,
           message: "connection definition not found for #{inspect(name)}"
         }
       end)}
    end
  end

  defp normalize_keyword_connections(entries) do
    duplicate_errors =
      entries
      |> duplicate_keyword_keys()
      |> Enum.map(fn key ->
        %Error{
          type: :invalid_connections_config,
          connection: key,
          message: "duplicate runtime connection name in keyword config: #{inspect(key)}"
        }
      end)

    {map, errors} =
      Enum.reduce(entries, {%{}, duplicate_errors}, fn
        {key, values}, {acc, errs} when is_atom(key) ->
          case normalize_runtime_values(values, key) do
            {:ok, normalized} -> {Map.put(acc, key, normalized), errs}
            {:error, runtime_errors} -> {acc, runtime_errors ++ errs}
          end

        invalid_entry, {acc, errs} ->
          {acc,
           [
             %Error{
               type: :invalid_connections_config,
               message: "invalid runtime connection entry: #{inspect(invalid_entry)}"
             }
             | errs
           ]}
      end)

    if errors == [], do: {:ok, map}, else: {:error, Enum.reverse(errors)}
  end

  defp normalize_map_connections(entries) do
    {map, errors} =
      Enum.reduce(entries, {%{}, []}, fn
        {key, values}, {acc, errs} when is_atom(key) ->
          case normalize_runtime_values(values, key) do
            {:ok, normalized} -> {Map.put(acc, key, normalized), errs}
            {:error, runtime_errors} -> {acc, runtime_errors ++ errs}
          end

        {key, _values}, {acc, errs} ->
          {acc,
           [
             %Error{
               type: :invalid_connections_config,
               message: "runtime connection name must be an atom, got: #{inspect(key)}"
             }
             | errs
           ]}
      end)

    if errors == [], do: {:ok, map}, else: {:error, Enum.reverse(errors)}
  end

  defp normalize_runtime_values(values, _name) when is_map(values) do
    if Enum.all?(Map.keys(values), &is_atom/1) do
      {:ok, values}
    else
      {:error,
       [
         %Error{
           type: :invalid_connections_config,
           message: "connection runtime value maps must use atom keys"
         }
       ]}
    end
  end

  defp normalize_runtime_values(values, _name) when values == [], do: {:ok, %{}}

  defp normalize_runtime_values(values, _name) when is_list(values) do
    if Keyword.keyword?(values) do
      case duplicate_keyword_keys(values) do
        [] ->
          {:ok, Map.new(values)}

        duplicate_keys ->
          {:error,
           Enum.map(duplicate_keys, fn key ->
             %Error{
               type: :invalid_connections_config,
               message: "duplicate runtime config key in keyword config: #{inspect(key)}"
             }
           end)}
      end
    else
      {:error,
       [
         %Error{
           type: :invalid_connections_config,
           message: "connection runtime values must be keyword/map"
         }
       ]}
    end
  end

  defp normalize_runtime_values(_other, name) do
    {:error,
     [
       %Error{
         type: :invalid_connections_config,
         connection: name,
         message: "runtime connection entry for #{inspect(name)} must be keyword/map"
       }
     ]}
  end

  defp validate_duplicate_names(definitions) do
    duplicate_names =
      definitions
      |> Enum.group_by(& &1.name)
      |> Enum.filter(fn {_name, defs} -> length(defs) > 1 end)

    if duplicate_names == [] do
      :ok
    else
      errors =
        Enum.flat_map(duplicate_names, fn {name, defs} ->
          Enum.map(defs, fn defn ->
            %Error{
              type: :duplicate_name,
              module: defn.module,
              connection: name,
              message: "duplicate connection name registered: #{inspect(name)}"
            }
          end)
        end)

      {:error, errors}
    end
  end

  defp invalid_modules_message(other) do
    "config :favn, :connection_modules must be a list, got: #{inspect(other)}"
  end

  defp invalid_connections_message(other) do
    "config :favn, :connections must be a keyword/map, got: #{inspect(other)}"
  end

  defp duplicate_keyword_keys(keyword) do
    keyword
    |> Enum.map(&elem(&1, 0))
    |> Enum.frequencies()
    |> Enum.filter(fn {_key, count} -> count > 1 end)
    |> Enum.map(&elem(&1, 0))
  end
end
