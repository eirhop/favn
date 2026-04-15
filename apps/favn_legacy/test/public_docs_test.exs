defmodule Favn.PublicDocsTest do
  use ExUnit.Case, async: true

  @modules [
    Favn,
    Favn.AgentGuide,
    Favn.Asset,
    Favn.SQLAsset,
    Favn.MultiAsset,
    Favn.Assets,
    Favn.Namespace,
    Favn.SQL,
    Favn.Pipeline,
    Favn.Triggers.Schedules,
    Favn.Window,
    Favn.Connection,
    Favn.Source
  ]

  test "important public modules have moduledocs" do
    Enum.each(@modules, fn module ->
      assert moduledoc(module), "expected #{inspect(module)} to have a public moduledoc"
    end)
  end

  test "important public DSL entrypoints have docs" do
    assert doc?(Favn.SQLAsset, :macro, :query, 1)
    assert doc?(Favn.SQL, :macro, :sigil_SQL, 2)
    assert doc?(Favn.SQL, :macro, :defsql, 2)
    assert doc?(Favn.MultiAsset, :macro, :defaults, 1)
    assert doc?(Favn.MultiAsset, :macro, :asset, 2)
    assert doc?(Favn.Pipeline, :macro, :pipeline, 2)
    assert doc?(Favn.Pipeline, :macro, :asset, 1)
    assert doc?(Favn.Pipeline, :macro, :select, 1)
    assert doc?(Favn.Triggers.Schedules, :macro, :schedule, 2)
  end

  defp moduledoc(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _, _, _, %{"en" => doc}, _, _} when is_binary(doc) -> String.trim(doc) != ""
      _ -> false
    end
  end

  defp doc?(module, kind, name, arity) do
    case Code.fetch_docs(module) do
      {:docs_v1, _, _, _, _, _, docs} ->
        Enum.any?(docs, fn
          {{entry_kind, entry_name, entry_arity}, _, _, %{"en" => doc}, _}
          when entry_kind == kind and entry_name == name and entry_arity == arity and
                 is_binary(doc) ->
            String.trim(doc) != ""

          _ ->
            false
        end)

      _ ->
        false
    end
  end
end
