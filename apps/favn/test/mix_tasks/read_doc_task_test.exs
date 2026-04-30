defmodule Mix.Tasks.Favn.ReadDocTaskTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Favn.ReadDoc, as: ReadDocTask

  test "prints module docs" do
    output =
      capture_io(fn ->
        ReadDocTask.run(["Enum"])
      end)

    assert output =~ "Module: Enum"
    assert output =~ "Moduledoc:"
  end

  test "prints all public arities for a function name" do
    output =
      capture_io(fn ->
        ReadDocTask.run(["Enum", "map"])
      end)

    assert output =~ "Function: map"
    assert output =~ "map/2"
  end

  test "prints AI entrypoint docs for Favn" do
    output =
      capture_io(fn ->
        ReadDocTask.run(["Favn.AI"])
      end)

    assert output =~ "Module: Favn.AI"
    assert output =~ "AI-oriented documentation entrypoint"
    assert output =~ "Recommended Consumer Shape"
    assert output =~ "Favn.Namespace"
    assert output =~ "What To Read"
  end

  test "prints public Favn function docs" do
    output =
      capture_io(fn ->
        ReadDocTask.run(["Favn", "generate_manifest"])
      end)

    assert output =~ "Function: generate_manifest"
    assert output =~ "canonical `%Favn.Manifest{}`"
    assert output =~ "stable graph payload"
  end

  test "raises on unknown module" do
    assert_raise Mix.Error, ~r/is not available on the current code path/, fn ->
      ReadDocTask.run(["FavnNoSuchModule"])
    end
  end

  test "raises on invalid argument count" do
    assert_raise Mix.Error, ~r/usage: mix favn.read_doc ModuleName \[function_name\]/, fn ->
      ReadDocTask.run([])
    end
  end
end
