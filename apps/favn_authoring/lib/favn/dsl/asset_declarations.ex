defmodule Favn.DSL.AssetDeclarations do
  @moduledoc false

  alias Favn.DSL.Compiler, as: DSLCompiler

  @legacy_attributes [
    :asset,
    :config,
    :custom,
    :defaults,
    :depends,
    :description,
    :execution_pool,
    :extra,
    :freshness,
    :materialized,
    :meta,
    :relation,
    :resources,
    :rest,
    :retry,
    :runtime_config,
    :runtime_inputs,
    :settings,
    :title,
    :window
  ]

  @declarations [
    :settings,
    :meta,
    :depends,
    :window,
    :freshness,
    :retry,
    :execution_pool,
    :relation,
    :runtime_config
  ]

  @dsl_kind_attribute :favn_dsl_kind

  @type dsl_kind :: :namespace | :asset | :multi_asset | :sql_asset | :source

  @spec claim_module!(module(), dsl_kind(), Path.t(), pos_integer()) :: :ok
  def claim_module!(module, kind, file, line)
      when is_atom(module) and is_atom(kind) and is_integer(line) do
    unless Module.has_attribute?(module, @dsl_kind_attribute) do
      Module.register_attribute(module, @dsl_kind_attribute, persist: false)
    end

    case Module.get_attribute(module, @dsl_kind_attribute) do
      nil ->
        Module.put_attribute(module, @dsl_kind_attribute, kind)

      existing ->
        DSLCompiler.compile_error!(
          file,
          line,
          "one module cannot combine #{dsl_name(existing)} with #{dsl_name(kind)}; " <>
            "Favn.Namespace modules are structural and asset/source modules inherit from ancestor namespaces automatically"
        )
    end
  end

  @spec register!(module(), [atom()]) :: :ok
  def register!(module, declarations \\ @declarations) when is_atom(module) do
    Enum.each(declarations, fn declaration ->
      Module.register_attribute(module, attribute(declaration), accumulate: true, persist: false)
    end)
  end

  @spec values(module(), atom()) :: list()
  def values(module, declaration) do
    module
    |> Module.get_attribute(attribute(declaration))
    |> List.wrap()
    |> Enum.reverse()
  end

  @spec take(module(), atom()) :: list()
  def take(module, declaration) do
    values = values(module, declaration)
    Module.delete_attribute(module, attribute(declaration))
    values
  end

  @spec put(module(), atom(), term()) :: :ok
  def put(module, declaration, value) do
    Module.put_attribute(module, attribute(declaration), value)
  end

  @spec reject_legacy_attributes!(module(), Path.t(), pos_integer()) :: :ok
  def reject_legacy_attributes!(module, file, line) do
    case Enum.find(@legacy_attributes, &Module.has_attribute?(module, &1)) do
      nil ->
        :ok

      attribute ->
        DSLCompiler.compile_error!(
          file,
          line,
          "@#{attribute} is not supported; use the #{attribute} DSL macro without @"
        )
    end
  end

  @spec attribute(atom()) :: atom()
  def attribute(declaration), do: String.to_atom("favn_declaration_#{declaration}")

  defmacro settings(values) do
    put_declaration(:settings, values)
  end

  defmacro meta(values) do
    put_declaration(:meta, values)
  end

  defmacro depends(value) do
    put_declaration(:depends, value)
  end

  defmacro window(value) do
    put_declaration(:window, value)
  end

  defmacro freshness(value) do
    put_declaration(:freshness, value)
  end

  defmacro retry(value) do
    put_declaration(:retry, value)
  end

  defmacro execution_pool(value) do
    put_declaration(:execution_pool, value)
  end

  defmacro relation(value) do
    put_declaration(:relation, value)
  end

  defmacro runtime_config(bundle) do
    quote do
      Favn.DSL.AssetDeclarations.put(
        __MODULE__,
        :runtime_config,
        Favn.RuntimeConfig.Bundle.validate!(unquote(bundle))
      )
    end
  end

  defmacro runtime_config(scope, fields) do
    caller = __CALLER__
    file = Favn.DSL.Compiler.normalize_file(caller.file)

    quote do
      Favn.DSL.AssetDeclarations.put(
        __MODULE__,
        :runtime_config,
        Favn.RuntimeConfig.Bundle.inline!(unquote(scope), unquote(fields),
          module: unquote(caller.module),
          file: unquote(file),
          line: unquote(caller.line)
        )
      )
    end
  end

  defmacro env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.env!(unquote(key), unquote(opts))
    end
  end

  defmacro secret_env!(key, opts \\ []) do
    quote do
      Favn.RuntimeConfig.Ref.secret_env!(unquote(key), unquote(opts))
    end
  end

  defp put_declaration(name, value) do
    quote do
      Favn.DSL.AssetDeclarations.put(__MODULE__, unquote(name), unquote(value))
    end
  end

  defp dsl_name(:namespace), do: "Favn.Namespace"
  defp dsl_name(:asset), do: "Favn.Asset"
  defp dsl_name(:multi_asset), do: "Favn.MultiAsset"
  defp dsl_name(:sql_asset), do: "Favn.SQLAsset"
  defp dsl_name(:source), do: "Favn.Source"
end
