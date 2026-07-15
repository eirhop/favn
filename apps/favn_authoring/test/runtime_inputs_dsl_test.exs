defmodule Favn.RuntimeInputsDSLTest do
  use ExUnit.Case, async: false

  alias Favn.RuntimeInputResolver.Ref
  alias Favn.SQLAsset.RuntimeInputs.Result

  defmodule Resolver do
    @behaviour Favn.SQLAsset.RuntimeInputs

    @impl true
    def resolve(_context) do
      {:ok, %Result{params: %{external_id: 42}, identity: "snapshot:42"}}
    end
  end

  defmodule SQLHelpers do
    use Favn.SQL

    defsql selected(value) do
      ~SQL"coalesce(@value, @value)"
    end
  end

  defmodule ResolvedAsset do
    use Favn.Namespace, relation: [connection: :warehouse, schema: "test"]
    use SQLHelpers
    use Favn.SQLAsset

    @runtime_inputs Resolver
    @materialized :table

    query do
      ~SQL"select selected(@external_id) as id"
    end
  end

  test "compiles one behaviour resolver into a typed definition reference" do
    definition = ResolvedAsset.__favn_sql_asset_definition__()

    assert definition.runtime_inputs == %Ref{module: Resolver}
    assert definition.template.requires.query_params == MapSet.new(["external_id"])
  end

  test "redacts sensitive values from result inspection" do
    result = %Result{
      params: %{signed_url: "https://secret.example", count: 2},
      identity: "manifest:1",
      sensitive_params: [:signed_url]
    }

    inspected = inspect(result)
    assert inspected =~ "signed_url: :redacted"
    assert inspected =~ "count: 2"
    refute inspected =~ "secret.example"
  end

  test "rejects resolver modules that do not explicitly implement the behaviour" do
    resolver = dynamic_module("MissingBehaviour")

    Code.compile_string("""
    defmodule #{inspect(resolver)} do
      def resolve(_context), do: :ok
    end
    """)

    assert_raise CompileError,
                 ~r/must explicitly declare @behaviour Favn.SQLAsset.RuntimeInputs/,
                 fn ->
                   compile_asset!("@runtime_inputs #{inspect(resolver)}")
                 end
  end

  test "rejects resolver modules without public resolve/1" do
    resolver = dynamic_module("MissingCallback")

    Code.compile_string("""
    defmodule #{inspect(resolver)} do
      @behaviour Favn.SQLAsset.RuntimeInputs
    end
    """)

    assert_raise CompileError, ~r/must export public resolve\/1/, fn ->
      compile_asset!("@runtime_inputs #{inspect(resolver)}")
    end
  end

  test "rejects unsupported, duplicate, and late resolver declarations" do
    assert_raise CompileError, ~r/expected @runtime_inputs MyApp.Inputs/, fn ->
      compile_asset!("@runtime_inputs {Resolver, :resolve}")
    end

    assert_raise CompileError, ~r/multiple @runtime_inputs attributes are not allowed/, fn ->
      compile_asset!("""
      @runtime_inputs #{inspect(Resolver)}
      @runtime_inputs #{inspect(Resolver)}
      """)
    end

    module = dynamic_module("Late")

    assert_raise CompileError, ~r/@runtime_inputs must be declared before query/, fn ->
      Code.compile_string(
        """
        defmodule #{inspect(module)} do
          use Favn.Namespace, relation: [connection: :warehouse, schema: "test"]
          use Favn.SQLAsset

          @materialized :table
          query do
            ~SQL"select 1"
          end

          @runtime_inputs #{inspect(Resolver)}
        end
        """,
        "test/runtime_inputs_dsl_test.exs"
      )
    end
  end

  defp compile_asset!(runtime_inputs_declaration) do
    module = dynamic_module("Asset")

    Code.compile_string(
      """
      defmodule #{inspect(module)} do
        use Favn.Namespace, relation: [connection: :warehouse, schema: "test"]
        use Favn.SQLAsset

        #{runtime_inputs_declaration}
        @materialized :table

        query do
          ~SQL"select 1"
        end
      end
      """,
      "test/runtime_inputs_dsl_test.exs"
    )

    module.__favn_sql_asset_definition__()
  end

  defp dynamic_module(label) do
    Module.concat([__MODULE__, "#{label}#{System.unique_integer([:positive])}"])
  end
end
