defmodule Favn.Source do
  @moduledoc """
  External relational source DSL for declaring upstream data relations.

  Represents a relation in an external data warehouse that Favn can read from
  and reason about, but does not materialize or execute.

      defmodule MyApp.Raw.Stripe.Charges do
        use Favn.Source

        @relation true
      end

  The `@relation` attribute is required and follows the same inference rules as `Favn.Asset`.

  Supported attributes:
  - `@doc` - documentation
  - `@meta` - metadata such as owner, category, tags
  - `@relation` - the external relation identity (required)
  """

  alias Favn.Namespace
  alias Favn.Ref
  alias Favn.RelationRef

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :meta, persist: false)
      Module.register_attribute(__MODULE__, :relation, accumulate: true)
      Module.register_attribute(__MODULE__, :favn_source_raw, persist: false)

      @on_definition Favn.Source
      @before_compile Favn.Source
    end
  end

  @doc false
  def __on_definition__(env, kind, name, args, _guards, _body) do
    arity = length(args || [])

    cond do
      kind in [:def, :defp] and name == :source and arity == 1 ->
        compile_error!(
          env.file,
          env.line,
          "Favn.Source does not define a source/1 function; use @relation to declare the external relation"
        )

      kind in [:def, :defp] ->
        validate_no_stray_source_attributes!(env, kind, name, arity)

      true ->
        :ok
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    relation = Module.get_attribute(env.module, :relation) |> Enum.reverse()

    if relation == [] do
      compile_error!(
        env.file,
        env.line,
        "Favn.Source modules require @relation attribute"
      )
    end

    raw_sources = build_sources!(env.module, relation)

    quote do
      @doc false
      @spec __favn_assets__() :: [Favn.Asset.t()]
      def __favn_assets__, do: unquote(Macro.escape(raw_sources))

      @doc false
      def __favn_assets_raw__, do: unquote(Macro.escape(raw_sources))

      @doc false
      def __favn_single_asset__, do: true

      @doc false
      def __favn_source__, do: true
    end
  end

  defp build_sources!(module, [relation]) do
    defaults = Namespace.resolve(module)
    inferred_name = inferred_relation_name(module)

    relation_ref =
      case relation do
        true ->
          RelationRef.new!(Map.put(defaults, :name, inferred_name))

        attrs when is_list(attrs) ->
          if Keyword.keyword?(attrs) do
            attrs
            |> Map.new()
            |> merge_relation_attrs(defaults, inferred_name)
            |> RelationRef.new!()
          else
            raise ArgumentError,
                  "invalid @relation value #{inspect(attrs)}; expected true, a keyword list, or a map"
          end

        attrs when is_map(attrs) ->
          attrs
          |> merge_relation_attrs(defaults, inferred_name)
          |> RelationRef.new!()

        other ->
          raise ArgumentError,
                "invalid @relation value #{inspect(other)}; expected true, a keyword list, or a map"
      end

    %Favn.Asset{
      module: module,
      name: :source,
      ref: Ref.new(module, :source),
      arity: 0,
      type: :source,
      title: nil,
      doc: nil,
      file: nil,
      line: 0,
      meta: %{},
      depends_on: [],
      dependencies: [],
      window_spec: nil,
      relation: relation_ref,
      materialization: nil,
      relation_inputs: [],
      diagnostics: []
    }
  end

  defp inferred_relation_name(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end

  defp merge_relation_attrs(attrs, defaults, inferred_name) when is_map(attrs) do
    attrs =
      if has_explicit_name?(attrs) do
        attrs
      else
        Map.put(attrs, :name, inferred_name)
      end

    defaults
    |> maybe_drop_default_key(attrs, [:catalog], [:database, "database"])
    |> maybe_drop_default_key(attrs, [:name], [:table, "table", :name, "name"])
    |> Map.merge(attrs)
  end

  defp has_explicit_name?(attrs) do
    Enum.any?([:name, "name", :table, "table"], &Map.has_key?(attrs, &1))
  end

  defp maybe_drop_default_key(defaults, attrs, canonical_keys, authored_keys) do
    if Enum.any?(authored_keys, &Map.has_key?(attrs, &1)) do
      Enum.reduce(canonical_keys, defaults, &Map.delete(&2, &1))
    else
      defaults
    end
  end

  defp validate_no_stray_source_attributes!(env, kind, name, arity) do
    meta = Module.get_attribute(env.module, :meta)
    relation = Module.get_attribute(env.module, :relation)

    if not is_nil(meta) or relation != [] do
      Module.delete_attribute(env.module, :meta)
      Module.delete_attribute(env.module, :relation)

      compile_error!(
        env.file,
        env.line,
        "invalid #{kind} #{name}/#{arity} in Favn.Source module; only @doc and @relation allowed"
      )
    else
      :ok
    end
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end
end
