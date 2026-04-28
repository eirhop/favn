defmodule Favn.RuntimeConfig.Redactor do
  @moduledoc """
  Redacts values that correspond to declared secret runtime config fields.
  """

  alias Favn.RuntimeConfig.Ref
  alias Favn.RuntimeConfig.Requirements

  @redacted :redacted

  @spec redact(term(), Requirements.declarations(), map()) :: term()
  def redact(value, declarations, resolved_config \\ %{}) when is_map(declarations) do
    secret_fields = secret_fields(declarations)
    secret_scopes = secret_scopes(declarations)
    secret_values = secret_values(secret_scopes, resolved_config)

    if MapSet.size(secret_fields) == 0 and secret_values == [] do
      value
    else
      redact_value(value, secret_fields, secret_scopes, secret_values)
    end
  end

  defp secret_fields(declarations) do
    declarations
    |> Enum.flat_map(fn {_scope, fields} ->
      fields
      |> Enum.filter(fn {_field, ref} -> match?(%Ref{secret?: true}, ref) end)
      |> Enum.map(&elem(&1, 0))
    end)
    |> MapSet.new()
  end

  defp secret_scopes(declarations) do
    Map.new(declarations, fn {scope, fields} ->
      secret_field_set =
        fields
        |> Enum.filter(fn {_field, ref} -> match?(%Ref{secret?: true}, ref) end)
        |> Enum.map(&elem(&1, 0))
        |> MapSet.new()

      {scope, secret_field_set}
    end)
  end

  defp secret_values(secret_scopes, resolved_config) when is_map(resolved_config) do
    secret_scopes
    |> Enum.flat_map(fn {scope, fields} ->
      case Map.get(resolved_config, scope) do
        values when is_map(values) ->
          fields
          |> Enum.map(&Map.get(values, &1))
          |> Enum.filter(&redactable_secret?/1)

        _other ->
          []
      end
    end)
    |> Enum.uniq()
  end

  defp redactable_secret?(value), do: is_binary(value) and value != ""

  defp redact_value(%DateTime{} = value, _secret_fields, _secret_scopes, _secret_values),
    do: value

  defp redact_value(%_{} = value, secret_fields, secret_scopes, secret_values) do
    struct = value.__struct__

    value
    |> Map.from_struct()
    |> redact_value(secret_fields, secret_scopes, secret_values)
    |> Map.put(:__struct__, struct)
  end

  defp redact_value(value, secret_fields, secret_scopes, secret_values) when is_map(value) do
    value
    |> Enum.map(fn {key, child} ->
      cond do
        MapSet.member?(secret_fields, normalize_key(key)) ->
          {key, @redacted}

        Map.has_key?(secret_scopes, normalize_key(key)) and is_map(child) ->
          {key,
           redact_scope(
             child,
             Map.fetch!(secret_scopes, normalize_key(key)),
             secret_fields,
             secret_scopes,
             secret_values
           )}

        true ->
          {key, redact_value(child, secret_fields, secret_scopes, secret_values)}
      end
    end)
    |> Map.new()
  end

  defp redact_value(values, secret_fields, secret_scopes, secret_values) when is_list(values) do
    Enum.map(values, &redact_value(&1, secret_fields, secret_scopes, secret_values))
  end

  defp redact_value(value, secret_fields, secret_scopes, secret_values) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&redact_value(&1, secret_fields, secret_scopes, secret_values))
    |> List.to_tuple()
  end

  defp redact_value(value, _secret_fields, _secret_scopes, secret_values) when is_binary(value) do
    Enum.reduce(secret_values, value, &String.replace(&2, &1, Atom.to_string(@redacted)))
  end

  defp redact_value(value, _secret_fields, _secret_scopes, _secret_values), do: value

  defp redact_scope(value, scope_secret_fields, secret_fields, secret_scopes, secret_values) do
    value
    |> Enum.map(fn {key, child} ->
      if MapSet.member?(scope_secret_fields, normalize_key(key)) do
        {key, @redacted}
      else
        {key, redact_value(child, secret_fields, secret_scopes, secret_values)}
      end
    end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: safe_existing_atom(key)
  defp normalize_key(key), do: key

  defp safe_existing_atom(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end
end
