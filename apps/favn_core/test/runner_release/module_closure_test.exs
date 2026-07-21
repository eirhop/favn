defmodule Favn.RunnerRelease.ModuleClosureTest do
  use ExUnit.Case, async: false

  alias Favn.RunnerRelease.ModuleClosure
  alias Favn.RunnerRelease.RuntimeRoots

  test "selects roots, explicit dynamic modules, and project-local transitive imports" do
    modules =
      compile_modules("""
      defmodule FavnClosureLeaf do
        def value, do: 42
      end

      defmodule FavnClosureHelper do
        def value, do: FavnClosureLeaf.value()
      end

      defmodule FavnClosureRoot do
        def value, do: FavnClosureHelper.value()
      end

      defmodule FavnClosureDynamic do
        def value, do: Enum.join(["dynamic"])
      end

      defmodule FavnClosureUnrelated do
        def value, do: :unrelated
      end
      """)

    assert {:ok, closure} =
             ModuleClosure.build(
               [FavnClosureRoot],
               modules,
               extra_modules: [FavnClosureDynamic]
             )

    assert closure.root_modules == ["Elixir.FavnClosureDynamic", "Elixir.FavnClosureRoot"]
    assert closure.extra_applications == []

    assert Enum.map(closure.modules, & &1.module) == [
             "Elixir.FavnClosureDynamic",
             "Elixir.FavnClosureHelper",
             "Elixir.FavnClosureLeaf",
             "Elixir.FavnClosureRoot"
           ]

    refute Enum.any?(closure.modules, &(&1.module == "Elixir.FavnClosureUnrelated"))
    refute Enum.any?(closure.modules, &(&1.module == "Elixir.Enum"))

    unload_all(modules)
  end

  test "includes project-local implementations when a selected protocol is imported" do
    modules =
      compile_modules("""
      defprotocol FavnClosureProtocol do
        def value(input)
      end

      defimpl FavnClosureProtocol, for: Integer do
        def value(input), do: input + 1
      end

      defmodule FavnProtocolRoot do
        def value, do: FavnClosureProtocol.value(1)
      end
      """)

    assert {:ok, closure} = ModuleClosure.build([FavnProtocolRoot], modules)

    assert Enum.map(closure.modules, & &1.module) == [
             "Elixir.FavnClosureProtocol",
             "Elixir.FavnClosureProtocol.Integer",
             "Elixir.FavnProtocolRoot"
           ]

    unload_all(modules)
  end

  test "conservatively includes local implementations of dependency protocols" do
    protocol_modules =
      compile_modules("""
      defprotocol FavnDependencyProtocol do
        def value(input)
      end
      """)

    project_modules =
      compile_modules("""
      defimpl FavnDependencyProtocol, for: Integer do
        def value(input), do: input + 1
      end

      defmodule FavnDependencyProtocolRoot do
        def value, do: :root
      end
      """)

    refute Map.has_key?(project_modules, FavnDependencyProtocol)
    assert {:ok, closure} = ModuleClosure.build([FavnDependencyProtocolRoot], project_modules)

    assert Enum.map(closure.modules, & &1.module) == [
             "Elixir.FavnDependencyProtocol.Integer",
             "Elixir.FavnDependencyProtocolRoot"
           ]

    unload_all(project_modules)
    unload_all(protocol_modules)
  end

  test "normalizes categorized roots and carries explicit dynamic applications" do
    modules =
      compile_modules("""
      defmodule FavnCategorizedAsset, do: def(value(), do: :asset)
      defmodule FavnCategorizedResolver, do: def(value(), do: :resolver)
      defmodule FavnCategorizedDynamic, do: def(value(), do: :dynamic)
      """)

    assert {:ok, roots} =
             RuntimeRoots.new(%{
               asset_modules: [FavnCategorizedAsset],
               runtime_input_resolver_modules: [FavnCategorizedResolver],
               extra_modules: [FavnCategorizedDynamic, FavnCategorizedDynamic],
               extra_applications: [:my_runtime_app, :crypto, :crypto]
             })

    assert RuntimeRoots.module_roots(roots) == [
             "Elixir.FavnCategorizedAsset",
             "Elixir.FavnCategorizedDynamic",
             "Elixir.FavnCategorizedResolver"
           ]

    assert roots.extra_applications == ["crypto", "my_runtime_app"]
    assert {:ok, closure} = ModuleClosure.build(roots, modules)
    assert closure.extra_applications == ["crypto", "my_runtime_app"]

    assert {:error, {:invalid_runner_release_field, :extra_applications, :expected_list}} =
             RuntimeRoots.new(%{extra_applications: :crypto})

    unload_all(modules)
  end

  test "fails when a required root is absent" do
    assert {:error, {:missing_runtime_root_module, "Elixir.Missing.Dynamic"}} =
             ModuleClosure.build([], %{}, extra_modules: [Missing.Dynamic])
  end

  test "rejects a declared module name that does not match its BEAM" do
    modules = compile_modules("defmodule FavnClosureActual, do: def(value(), do: :ok)")
    beam = Map.fetch!(modules, FavnClosureActual)

    assert {:error,
            {:beam_module_name_mismatch, "Elixir.FavnClosureDeclared", "Elixir.FavnClosureActual"}} =
             ModuleClosure.build([FavnClosureDeclared], %{FavnClosureDeclared => beam})

    unload_all(modules)
  end

  test "does not reject an invalid unrelated BEAM outside the selected closure" do
    modules = compile_modules("defmodule FavnClosureValidRoot, do: def(value(), do: :ok)")

    assert {:ok, closure} =
             ModuleClosure.build(
               [FavnClosureValidRoot],
               Map.put(modules, "Elixir.UnrelatedInvalid", "not a beam")
             )

    assert Enum.map(closure.modules, & &1.module) == ["Elixir.FavnClosureValidRoot"]
    unload_all(modules)
  end

  test "rejects an invalid project-local protocol implementation even when it is dynamically selected" do
    protocol_modules =
      compile_modules("""
      defprotocol FavnInvalidDependencyProtocol do
        def value(input)
      end
      """)

    project_modules =
      compile_modules("""
      defimpl FavnInvalidDependencyProtocol, for: Integer do
        def value(_input), do: "/tmp/favn-runtime-path"
      end

      defmodule FavnInvalidProtocolRoot, do: def(value(), do: :root)
      """)

    assert {:error,
            {:invalid_runtime_module, "Elixir.FavnInvalidDependencyProtocol.Integer",
             {:invalid_beam, {:absolute_path_literal, _index}}}} =
             ModuleClosure.build([FavnInvalidProtocolRoot], project_modules)

    unload_all(project_modules)
    unload_all(protocol_modules)
  end

  test "normalizes root order deterministically" do
    modules =
      compile_modules("""
      defmodule FavnClosureA, do: def(value(), do: :a)
      defmodule FavnClosureB, do: def(value(), do: :b)
      """)

    assert {:ok, first} = ModuleClosure.build([FavnClosureB, FavnClosureA], modules)
    assert {:ok, second} = ModuleClosure.build([FavnClosureA, FavnClosureB], modules)
    assert first == second

    unload_all(modules)
  end

  defp compile_modules(source) do
    source
    |> Code.compile_string("closure_fixture.ex")
    |> Map.new()
  end

  defp unload_all(modules) do
    Enum.each(Map.keys(modules), fn module ->
      :code.purge(module)
      :code.delete(module)
    end)
  end
end
