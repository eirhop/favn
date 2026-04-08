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
         :ok <- validate_duplicate_names(definitions) do
      resolve_connections(definitions, runtime_connections)
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
        {:ok, normalize_connection_entries(entries)}

      entries when is_map(entries) ->
        {:ok, entries}

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
    if function_exported?(module, :definition, 0) do
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
      {:error,
       [
         %Error{
           type: :invalid_module,
           module: module,
           message: "connection module #{inspect(module)} must export definition/0"
         }
       ]}
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

  defp resolve_connections(definitions, runtime_connections) do
    {resolved_entries, errors} =
      Enum.reduce(definitions, {[], []}, fn definition, {entries, errs} ->
        runtime_values = Map.get(runtime_connections, definition.name, %{})

        case normalize_runtime_values(runtime_values, definition.name) do
          {:ok, values} ->
            case Validator.resolve(definition, values) do
              {:ok, resolved} -> {[{definition.name, resolved} | entries], errs}
              {:error, resolver_errors} -> {entries, resolver_errors ++ errs}
            end

          {:error, runtime_errors} ->
            {entries, runtime_errors ++ errs}
        end
      end)

    if errors == [] do
      {:ok, Map.new(resolved_entries)}
    else
      {:error, Enum.reverse(errors)}
    end
  end

  defp normalize_runtime_values(values, _name) when is_map(values), do: {:ok, values}
  defp normalize_runtime_values(values, _name) when values == [], do: {:ok, %{}}

  defp normalize_runtime_values(values, _name) when is_list(values) do
    if Keyword.keyword?(values) do
      {:ok, Map.new(values)}
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

  defp normalize_connection_entries(entries) do
    entries
    |> Enum.reduce(%{}, fn
      {key, values}, acc when is_atom(key) -> Map.put(acc, key, values)
      _, acc -> acc
    end)
  end

  defp invalid_modules_message(other) do
    "config :favn, :connection_modules must be a list, got: #{inspect(other)}"
  end

  defp invalid_connections_message(other) do
    "config :favn, :connections must be a keyword/map, got: #{inspect(other)}"
  end
end
