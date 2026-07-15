defmodule Favn.RuntimeConfig.Requirements do
  @moduledoc """
  Helpers for manifest-safe runtime configuration declarations.

  This module owns normalization and conflict-safe merging for runtime config
  bundles and direct asset declarations.
  """

  alias Favn.RuntimeConfig.Bundle
  alias Favn.RuntimeConfig.Ref

  @type scope :: atom()
  @type field :: atom()
  @type declarations :: %{scope() => %{field() => Ref.t()}}

  @doc """
  Merges two declaration maps field by field.

  Identical references deduplicate. Conflicting references for the same scope
  and field raise instead of silently overriding one another.
  """
  @spec merge!(declarations(), declarations()) :: declarations()
  def merge!(left, right) do
    merge_all!([left, right])
  end

  @doc """
  Flattens declaration maps and bundles in least-specific to most-specific order.

  Bundle provenance is included in conflict diagnostics but is not returned in
  the flattened manifest-safe result.
  """
  @spec merge_all!([declarations() | Bundle.t()], keyword()) :: declarations()
  def merge_all!(entries, opts \\ []) when is_list(entries) and is_list(opts) do
    consumer = Keyword.get(opts, :consumer)

    entries
    |> Enum.reduce({%{}, %{}, consumer}, &merge_entry!/2)
    |> elem(0)
  end

  defp merge_entry!(%Bundle{} = bundle, state) do
    bundle = Bundle.validate!(bundle)
    merge_declarations!(bundle.declarations, bundle.origin, state)
  end

  defp merge_entry!(declarations, state) when is_map(declarations) or is_list(declarations) do
    merge_declarations!(normalize!(declarations), nil, state)
  end

  defp merge_entry!(entry, _state) do
    raise ArgumentError,
          "runtime config merge entry must be declarations or a Favn.RuntimeConfig.Bundle, got: #{inspect(entry)}"
  end

  defp merge_declarations!(declarations, origin, {merged, origins, consumer}) do
    Enum.reduce(declarations, {merged, origins, consumer}, fn {scope, fields}, state ->
      Enum.reduce(fields, state, fn {field, ref}, {current, current_origins, current_consumer} ->
        path = {scope, field}

        case get_in(current, [scope, field]) do
          nil ->
            {
              Map.update(current, scope, %{field => ref}, &Map.put(&1, field, ref)),
              Map.put(current_origins, path, origin),
              current_consumer
            }

          ^ref ->
            {current, current_origins, current_consumer}

          existing ->
            raise_conflict!(
              current_consumer,
              scope,
              field,
              existing,
              Map.get(current_origins, path),
              ref,
              origin
            )
        end
      end)
    end)
  end

  defp raise_conflict!(consumer, scope, field, left, left_origin, right, right_origin) do
    raise ArgumentError,
          "conflicting runtime config #{inspect(scope)}.#{field}#{describe_consumer(consumer)}: " <>
            "#{describe_ref(left)} declared at #{describe_origin(left_origin)} conflicts with " <>
            "#{describe_ref(right)} declared at #{describe_origin(right_origin)}"
  end

  defp describe_ref(%Ref{} = ref) do
    "env #{inspect(ref.key)} (secret?: #{ref.secret?}, required?: #{ref.required?})"
  end

  defp describe_origin(nil), do: "an unknown origin"

  defp describe_origin(%{module: module, file: file, line: line}) do
    "#{inspect(module)} (#{file}:#{line})"
  end

  defp describe_consumer(nil), do: ""
  defp describe_consumer(module) when is_atom(module), do: " for #{inspect(module)}"

  @spec normalize!(keyword() | map()) :: declarations()
  @doc false
  def normalize!(declarations) when is_list(declarations) do
    if Keyword.keyword?(declarations) do
      Enum.reduce(declarations, %{}, fn {scope, fields}, normalized ->
        scope = validate_scope!(scope)
        put_scope_fields!(normalized, scope, normalize_fields!(scope, fields))
      end)
    else
      raise ArgumentError, "runtime config declarations must be a keyword list or map"
    end
  end

  def normalize!(declarations) when is_map(declarations) do
    declarations = normalize_declaration_order(declarations)

    Map.new(declarations, fn {scope, fields} ->
      scope = validate_scope!(scope)
      {scope, normalize_fields!(scope, fields)}
    end)
  end

  def normalize!(_declarations) do
    raise ArgumentError, "runtime config declarations must be a keyword list or map"
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
      Enum.reduce(fields, %{}, fn {field, ref}, normalized ->
        {field, ref} = normalize_field!(scope, field, ref)
        put_field!(normalized, scope, field, ref)
      end)
    else
      raise ArgumentError,
            "runtime config fields for #{inspect(scope)} must be a keyword list or map"
    end
  end

  defp normalize_fields!(scope, fields) when is_map(fields) do
    Map.new(fields, fn {field, ref} ->
      normalize_field!(scope, field, ref)
    end)
  end

  defp normalize_fields!(scope, _fields) do
    raise ArgumentError,
          "runtime config fields for #{inspect(scope)} must be a keyword list or map"
  end

  defp validate_scope!(scope) when is_atom(scope), do: scope

  defp validate_scope!(scope) do
    raise ArgumentError, "runtime config scope must be an atom, got: #{inspect(scope)}"
  end

  defp normalize_field!(scope, field, ref) do
    unless is_atom(field) do
      raise ArgumentError,
            "runtime config field for #{inspect(scope)} must be an atom, got: #{inspect(field)}"
    end

    unless match?(%Ref{}, ref) do
      raise ArgumentError,
            "runtime config #{inspect(scope)}.#{field} must be a Favn.RuntimeConfig.Ref"
    end

    {field, Ref.validate!(ref)}
  end

  defp put_scope_fields!(declarations, scope, fields) do
    Map.update(declarations, scope, fields, fn existing ->
      Enum.reduce(fields, existing, fn {field, ref}, merged ->
        put_field!(merged, scope, field, ref)
      end)
    end)
  end

  defp put_field!(fields, scope, field, ref) do
    case Map.fetch(fields, field) do
      :error ->
        Map.put(fields, field, ref)

      {:ok, ^ref} ->
        fields

      {:ok, existing} ->
        raise_conflict!(nil, scope, field, existing, nil, ref, nil)
    end
  end
end
