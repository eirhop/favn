defmodule Favn.SQL.ContractFragment do
  @moduledoc """
  Public DSL for reusable, column-only SQL output-contract fragments.

  A fragment removes repeated column declarations while keeping composition
  explicit in the asset. Assets include it inside a `contract do` block;
  the compiler flattens its ordered columns into the canonical manifest and
  records separate composition provenance.

  ## Example

      defmodule MyApp.Contracts.AuditMetadata do
        use Favn.SQL.ContractFragment

        column :_processed_at, :datetime, null: false
        column :_favn_run_id, :string, null: false
      end

  Declare ordered columns in the fragment. Keep grain, keys, row counts, and
  checks local to the consuming asset contract.
  """

  alias Favn.DSL.Compiler, as: DSLCompiler
  alias Favn.SQL.Contract.Fragment

  @column_options [:null, :description, :tags, :from, :via, :renamed_from]

  @doc false
  defmacro __using__(_opts) do
    quote do
      Module.register_attribute(__MODULE__, :favn_sql_contract_fragment_columns, accumulate: true)

      @before_compile Favn.SQL.ContractFragment

      import Favn.SQL.ContractFragment, only: [column: 2, column: 3]
    end
  end

  @doc "Declares one ordered column in a reusable SQL output-contract fragment."
  defmacro column(name_ast, type_ast, opts_ast \\ []) do
    name = literal!(name_ast, __CALLER__, "contract fragment column name")
    type = literal!(type_ast, __CALLER__, "contract fragment column type")
    opts = keyword!(opts_ast, __CALLER__)

    raw = %{
      name: name,
      type: type,
      opts: opts,
      file: DSLCompiler.normalize_file(__CALLER__.file),
      line: __CALLER__.line
    }

    quote bind_quoted: [raw: Macro.escape(raw)] do
      @favn_sql_contract_fragment_columns raw
      :ok
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    raw_columns =
      env.module
      |> Module.get_attribute(:favn_sql_contract_fragment_columns)
      |> List.wrap()
      |> Enum.reverse()

    fragment = build_fragment!(env.module, raw_columns, env)

    quote do
      @doc false
      defmacro __favn_sql_contract_dependency__, do: :ok

      @doc false
      @spec __favn_sql_contract_fragment__() :: Favn.SQL.Contract.Fragment.t()
      def __favn_sql_contract_fragment__, do: unquote(Macro.escape(fragment))
    end
  end

  defp build_fragment!(module, raw_columns, env) do
    Fragment.new!(module, raw_columns)
  rescue
    error in ArgumentError ->
      {file, line} = fragment_error_location(raw_columns, env)
      DSLCompiler.compile_error!(file, line, error.message)
  end

  defp fragment_error_location([first | _rest], _env), do: {first.file, first.line}
  defp fragment_error_location([], env), do: {env.file, env.line}

  defp keyword!(ast, env) do
    value = literal!(ast, env, "contract fragment column options")

    unless is_list(value) and Keyword.keyword?(value),
      do: DSLCompiler.compile_error!(env.file, env.line, "column options must be a keyword list")

    duplicate_keys =
      value
      |> Keyword.keys()
      |> Enum.frequencies()
      |> Enum.filter(&(elem(&1, 1) > 1))

    if duplicate_keys != [] do
      keys = duplicate_keys |> Enum.map(&elem(&1, 0)) |> Enum.map_join(", ", &inspect/1)
      DSLCompiler.compile_error!(env.file, env.line, "duplicate column options: #{keys}")
    end

    case Enum.find(Keyword.keys(value), &(&1 not in @column_options)) do
      nil ->
        value

      key ->
        DSLCompiler.compile_error!(env.file, env.line, "unknown column option #{inspect(key)}")
    end
  end

  defp literal!(ast, env, label) do
    expanded =
      Macro.prewalk(ast, fn
        {:__aliases__, _meta, _parts} = alias_ast -> Macro.expand(alias_ast, env)
        node -> node
      end)

    if Macro.quoted_literal?(expanded) do
      {value, _binding} = Code.eval_quoted(expanded, [], env)
      value
    else
      DSLCompiler.compile_error!(env.file, env.line, "#{label} must be literal")
    end
  end
end
