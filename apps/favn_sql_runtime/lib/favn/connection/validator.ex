defmodule Favn.Connection.Validator do
  @moduledoc false

  alias Favn.Connection.Definition
  alias Favn.Connection.Error
  alias Favn.Connection.Resolved
  alias Favn.RuntimeConfig.Resolver, as: RuntimeConfigResolver

  @reserved_runtime_keys [:write_concurrency]

  @spec validate_definition(Definition.t()) :: :ok | {:error, [Error.t()]}
  def validate_definition(%Definition{} = definition) do
    errors =
      []
      |> validate_name(definition)
      |> validate_adapter(definition)
      |> validate_schema(definition)

    if errors == [], do: :ok, else: {:error, Enum.reverse(errors)}
  end

  @spec resolve(Definition.t(), map()) :: {:ok, Resolved.t()} | {:error, [Error.t()]}
  def resolve(%Definition{} = definition, runtime_values) when is_map(runtime_values) do
    with :ok <- validate_definition(definition),
         {:ok, config, required_keys, secret_fields, schema_keys} <-
           build_config(definition, runtime_values) do
      {:ok,
       %Resolved{
         name: definition.name,
         adapter: definition.adapter,
         module: definition.module,
         config: config,
         required_keys: required_keys,
         secret_fields: secret_fields,
         schema_keys: schema_keys,
         metadata: definition.metadata
       }}
    end
  end

  defp build_config(definition, runtime_values) do
    schema_keys = Enum.map(definition.config_schema, & &1.key)
    defaults = defaults_from_schema(definition.config_schema)
    known_keys = schema_keys ++ @reserved_runtime_keys
    unknown = Map.keys(runtime_values) -- known_keys

    merged = Map.merge(defaults, runtime_values)

    with [] <- maybe_add_unknown_keys_error([], definition, unknown),
         {:ok, resolved_values} <- resolve_runtime_refs(definition, merged) do
      errors =
        []
        |> validate_required(definition, resolved_values)
        |> validate_types(definition, resolved_values)

      if errors == [] do
        required_keys =
          definition.config_schema
          |> Enum.filter(&Map.get(&1, :required, false))
          |> Enum.map(& &1.key)

        secret_fields = secret_fields(definition.config_schema, merged)

        {:ok, resolved_values, required_keys, secret_fields, schema_keys}
      else
        {:error, Enum.reverse(errors)}
      end
    else
      errors when is_list(errors) -> {:error, Enum.reverse(errors)}
      {:error, errors} when is_list(errors) -> {:error, Enum.reverse(errors)}
    end
  end

  defp resolve_runtime_refs(definition, values) when is_map(values) do
    values
    |> Enum.reduce_while({:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case RuntimeConfigResolver.resolve_value(value, scope: definition.name, field: key) do
        {:ok, resolved} ->
          {:cont, {:ok, Map.put(acc, key, resolved)}}

        {:error, error} ->
          {:halt, {:error, [connection_runtime_config_error(definition, key, error)]}}
      end
    end)
  end

  defp connection_runtime_config_error(definition, key, error) do
    %Error{
      type: error.type,
      module: definition.module,
      connection: definition.name,
      details: %{key: key, provider: error.provider, env: error.key, secret?: error.secret?},
      message: error.message
    }
  end

  defp defaults_from_schema(schema) do
    Enum.reduce(schema, %{}, fn field, acc ->
      if Map.has_key?(field, :default), do: Map.put(acc, field.key, field.default), else: acc
    end)
  end

  defp secret_fields(schema, values) do
    schema_secret_fields =
      schema
      |> Enum.filter(&Map.get(&1, :secret, false))
      |> Enum.map(& &1.key)

    runtime_secret_fields =
      values
      |> Enum.filter(fn {_key, value} -> nested_secret_ref?(value) end)
      |> Enum.map(&elem(&1, 0))

    (schema_secret_fields ++ runtime_secret_fields)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp nested_secret_ref?(%Favn.RuntimeConfig.Ref{secret?: true}), do: true
  defp nested_secret_ref?(%Favn.RuntimeConfig.Ref{}), do: false

  defp nested_secret_ref?(value) when is_map(value) do
    Enum.any?(value, fn {_key, child} -> nested_secret_ref?(child) end)
  end

  defp nested_secret_ref?(value) when is_list(value), do: Enum.any?(value, &nested_secret_ref?/1)

  defp nested_secret_ref?(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.any?(&nested_secret_ref?/1)
  end

  defp nested_secret_ref?(_value), do: false

  defp validate_name(errors, %Definition{name: name, module: module}) do
    if is_atom(name) do
      errors
    else
      [
        %Error{
          type: :invalid_definition,
          module: module,
          message: "connection definition name must be an atom"
        }
        | errors
      ]
    end
  end

  defp validate_adapter(errors, %Definition{adapter: adapter, module: module, name: name}) do
    if is_atom(adapter) do
      errors
    else
      [
        %Error{
          type: :invalid_adapter,
          module: module,
          connection: name,
          message: "connection adapter must be a module atom"
        }
        | errors
      ]
    end
  end

  defp validate_schema(errors, %Definition{config_schema: schema, module: module, name: name}) do
    if not is_list(schema) or schema == [] do
      [
        %Error{
          type: :invalid_definition,
          module: module,
          connection: name,
          message: "connection config_schema must be a non-empty list"
        }
        | errors
      ]
    else
      field_errors =
        schema
        |> Enum.with_index()
        |> Enum.flat_map(fn {field, index} ->
          validate_schema_field(field, index, module, name)
        end)

      duplicate_errors =
        schema
        |> Enum.map(&Map.get(&1, :key))
        |> Enum.reject(&is_nil/1)
        |> duplicates()
        |> Enum.map(fn key ->
          %Error{
            type: :invalid_definition,
            module: module,
            connection: name,
            details: %{key: key},
            message: "connection config_schema defines duplicate key #{inspect(key)}"
          }
        end)

      Enum.reverse(field_errors ++ duplicate_errors, errors)
    end
  end

  defp validate_schema_field(field, index, module, connection_name) when is_map(field) do
    key = Map.get(field, :key)

    errors =
      []
      |> maybe_invalid_schema_key(key, module, connection_name, index)
      |> maybe_invalid_field_boolean(field, :required, module, connection_name, index)
      |> maybe_invalid_field_boolean(field, :secret, module, connection_name, index)
      |> maybe_invalid_field_type(field, module, connection_name, index)

    Enum.reverse(errors)
  end

  defp validate_schema_field(_field, index, module, connection_name) do
    [
      %Error{
        type: :invalid_definition,
        module: module,
        connection: connection_name,
        details: %{index: index},
        message: "connection config_schema entry at index #{index} must be a map"
      }
    ]
  end

  defp maybe_invalid_schema_key(errors, key, module, connection_name, index) do
    if is_atom(key) do
      errors
    else
      [
        %Error{
          type: :invalid_definition,
          module: module,
          connection: connection_name,
          details: %{index: index},
          message: "connection config_schema entry at index #{index} must define atom :key"
        }
        | errors
      ]
    end
  end

  defp maybe_invalid_field_boolean(errors, field, key, module, connection_name, index) do
    value = Map.get(field, key)

    if is_nil(value) or is_boolean(value) do
      errors
    else
      [
        %Error{
          type: :invalid_definition,
          module: module,
          connection: connection_name,
          details: %{index: index, key: key},
          message:
            "connection config_schema key #{inspect(key)} at index #{index} must be boolean"
        }
        | errors
      ]
    end
  end

  defp maybe_invalid_field_type(errors, field, module, connection_name, index) do
    case Map.get(field, :type) do
      nil ->
        errors

      type ->
        if valid_type?(type) do
          errors
        else
          [
            %Error{
              type: :invalid_definition,
              module: module,
              connection: connection_name,
              details: %{index: index},
              message: "connection config_schema type at index #{index} is invalid"
            }
            | errors
          ]
        end
    end
  end

  defp maybe_add_unknown_keys_error(errors, _definition, []), do: errors

  defp maybe_add_unknown_keys_error(errors, definition, unknown) do
    [
      %Error{
        type: :unknown_keys,
        module: definition.module,
        connection: definition.name,
        details: %{keys: Enum.sort(unknown)},
        message: "connection #{inspect(definition.name)} contains unknown runtime keys"
      }
      | errors
    ]
  end

  defp validate_required(errors, definition, values) do
    definition.config_schema
    |> Enum.filter(&Map.get(&1, :required, false))
    |> Enum.reduce(errors, fn field, acc ->
      case Map.fetch(values, field.key) do
        {:ok, value} when not is_nil(value) ->
          acc

        _ ->
          [
            %Error{
              type: :missing_required,
              module: definition.module,
              connection: definition.name,
              details: %{key: field.key},
              message:
                "connection #{inspect(definition.name)} is missing required key #{inspect(field.key)}"
            }
            | acc
          ]
      end
    end)
  end

  defp validate_types(errors, definition, values) do
    Enum.reduce(definition.config_schema, errors, fn field, acc ->
      case Map.fetch(values, field.key) do
        {:ok, value} -> validate_field_value(acc, definition, field, value)
        :error -> acc
      end
    end)
  end

  defp validate_field_value(errors, definition, field, value) do
    case Map.get(field, :type) do
      nil ->
        errors

      type ->
        case type_valid?(type, value) do
          :ok ->
            errors

          {:error, reason} ->
            [validation_type_error(definition, field, reason) | errors]
        end
    end
  end

  defp validation_type_error(definition, field, reason) do
    %Error{
      type: :invalid_type,
      module: definition.module,
      connection: definition.name,
      details: %{key: field.key, reason: reason},
      message:
        "connection #{inspect(definition.name)} has invalid value for #{inspect(field.key)}"
    }
  end

  defp valid_type?(type)
       when type in [:string, :atom, :boolean, :integer, :float, :path, :module],
       do: true

  defp valid_type?({:in, values}), do: is_list(values)
  defp valid_type?({:custom, fun}), do: is_function(fun, 1)
  defp valid_type?(_), do: false

  defp type_valid?(:string, value),
    do: if(is_binary(value), do: :ok, else: {:error, :expected_string})

  defp type_valid?(:atom, value), do: if(is_atom(value), do: :ok, else: {:error, :expected_atom})

  defp type_valid?(:boolean, value),
    do: if(is_boolean(value), do: :ok, else: {:error, :expected_boolean})

  defp type_valid?(:integer, value),
    do: if(is_integer(value), do: :ok, else: {:error, :expected_integer})

  defp type_valid?(:float, value),
    do: if(is_float(value), do: :ok, else: {:error, :expected_float})

  defp type_valid?(:path, value),
    do: if(is_binary(value), do: :ok, else: {:error, :expected_path})

  defp type_valid?(:module, value),
    do: if(is_atom(value), do: :ok, else: {:error, :expected_module})

  defp type_valid?({:in, values}, value) do
    if value in values, do: :ok, else: {:error, {:expected_in, values}}
  end

  defp type_valid?({:custom, fun}, value) do
    case fun.(value) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_custom_validator_return}
    end
  end

  defp duplicates(values) do
    values
    |> Enum.frequencies()
    |> Enum.filter(fn {_key, count} -> count > 1 end)
    |> Enum.map(&elem(&1, 0))
  end
end
