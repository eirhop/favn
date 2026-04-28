defmodule Favn.RuntimeConfig.Requirements do
  @moduledoc """
  Helpers for manifest-safe runtime configuration declarations.

  This module is internal support for the public `Favn.Asset.source_config/2`
  DSL. Most users should read `Favn.Asset` and `Favn.RuntimeConfig.Ref` instead
  of calling this module directly.
  """

  alias Favn.RuntimeConfig.Ref

  @type scope :: atom()
  @type field :: atom()
  @type declarations :: %{scope() => %{field() => Ref.t()}}

  @spec normalize!(keyword() | map()) :: declarations()
  @doc false
  def normalize!(declarations) when is_list(declarations) do
    if Keyword.keyword?(declarations) do
      declarations |> Map.new() |> normalize!()
    else
      raise ArgumentError, "runtime config declarations must be a keyword list or map"
    end
  end

  def normalize!(declarations) when is_map(declarations) do
    declarations = normalize_declaration_order(declarations)

    Map.new(declarations, fn {scope, fields} ->
      unless is_atom(scope) do
        raise ArgumentError, "runtime config scope must be an atom, got: #{inspect(scope)}"
      end

      {scope, normalize_fields!(scope, fields)}
    end)
  end

  def normalize!(declarations) do
    raise ArgumentError,
          "runtime config declarations must be a keyword list or map, got: #{inspect(declarations)}"
  end

  defp normalize_declaration_order(declarations) do
    Map.new(declarations, fn
      {scope, fields} when is_atom(scope) ->
        {scope, fields}

      {fields, scope} when is_atom(scope) ->
        {scope, fields}

      {scope, _fields} ->
        raise ArgumentError, "runtime config scope must be an atom, got: #{inspect(scope)}"
    end)
  end

  @spec redact(declarations()) :: declarations()
  @doc false
  def redact(declarations) when is_map(declarations) do
    Map.new(declarations, fn {scope, fields} ->
      redacted_fields =
        Map.new(fields, fn
          {field, %Ref{secret?: true} = ref} -> {field, %{ref | key: "[REDACTED]"}}
          {field, value} -> {field, value}
        end)

      {scope, redacted_fields}
    end)
  end

  defp normalize_fields!(scope, fields) when is_list(fields) do
    if Keyword.keyword?(fields) do
      fields |> Map.new() |> normalize_fields!(scope)
    else
      raise ArgumentError,
            "runtime config fields for #{inspect(scope)} must be a keyword list or map"
    end
  end

  defp normalize_fields!(fields, scope) when is_map(fields) and is_atom(scope) do
    normalize_fields!(scope, fields)
  end

  defp normalize_fields!(scope, fields) when is_map(fields) do
    Map.new(fields, fn {field, ref} ->
      unless is_atom(field) do
        raise ArgumentError,
              "runtime config field for #{inspect(scope)} must be an atom, got: #{inspect(field)}"
      end

      unless match?(%Ref{}, ref) do
        raise ArgumentError,
              "runtime config #{inspect(scope)}.#{field} must be a Favn.RuntimeConfig.Ref"
      end

      {field, Ref.validate!(ref)}
    end)
  end

  defp normalize_fields!(scope, fields) do
    raise ArgumentError,
          "runtime config fields for #{inspect(scope)} must be a keyword list or map, got: #{inspect(fields)}"
  end
end
