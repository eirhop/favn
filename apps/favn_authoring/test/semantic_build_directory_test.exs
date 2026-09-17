defmodule FavnAuthoring.SemanticBuildDirectoryTest do
  use ExUnit.Case, async: true
  alias FavnAuthoring.Semantic.BuildDirectory

  test "requires dedicated output and rejects overlap with active runner paths" do
    assert {:error, :semantic_build_path_required} = BuildDirectory.validate(nil, "/project")
    assert {:error, :semantic_build_path_shared} = BuildDirectory.validate("_build", "/project")

    assert {:error, :semantic_build_path_shared} =
             BuildDirectory.validate("_build/dev", "/project")

    assert {:error, :semantic_build_path_shared} =
             BuildDirectory.validate("output", "/project", ["output/dev/lib/customer/ebin"])

    assert :ok =
             BuildDirectory.validate("_build_semantic", "/project", [
               "_build/dev/lib/customer/ebin"
             ])
  end

  test "rejects symlinks without touching their contents" do
    root = Path.join(System.tmp_dir!(), "semantic-path-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(root, "_build"))
    File.write!(Path.join(root, "_build/sentinel"), "unchanged")
    File.ln_s!(Path.join(root, "_build"), Path.join(root, "semantic"))
    on_exit(fn -> File.rm_rf!(root) end)
    assert {:error, :semantic_build_path_symlink} = BuildDirectory.validate("semantic", root)
    assert File.read!(Path.join(root, "_build/sentinel")) == "unchanged"
  end
end
