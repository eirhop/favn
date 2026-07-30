defmodule FavnView.Dev.DesignSystemTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest, only: [rendered_to_string: 1]

  alias FavnView.Dev.DesignSystem
  alias FavnView.Dev.DesignSystem.Audit
  alias FavnView.Dev.DesignSystem.Catalogue
  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Generated
  alias FavnView.Dev.DesignSystem.Query
  alias FavnView.Dev.DesignSystem.Render

  describe "catalogue" do
    test "discovers the element library, the sections, and the pages" do
      groups = DesignSystem.entries() |> Enum.map(& &1.group) |> Enum.uniq() |> Enum.sort()

      assert groups == [:component, :element, :page]
    end

    test "entry ids are unique" do
      ids = Enum.map(DesignSystem.entries(), & &1.id)

      assert ids == Enum.uniq(ids)
    end

    test "reads the contract from the component itself" do
      {:ok, entry} = Catalogue.fetch("badge/status_badge")

      assert entry.group == :element
      assert Entry.label(entry) == "FavnView.UI.Badge.status_badge/1"
      assert Enum.sort(Entry.required_attrs(entry)) == [:label, :tone]
      assert Entry.attr_values(entry, :size) == [:xs, :sm, :md]
      assert Entry.declared_defaults(entry)[:size] == :sm
    end

    test "a component with no example is still an entry" do
      assert {:ok, %Entry{}} = Catalogue.fetch("badge/count_badge")
    end

    test "filters by group, substring, and id" do
      entries = DesignSystem.entries()

      assert Catalogue.filter(entries, group: :element) |> Enum.all?(&(&1.group == :element))

      assert Catalogue.filter(entries, query: "badge")
             |> Enum.all?(&String.contains?(&1.id, "badge"))

      assert Catalogue.filter(entries, ids: ["button/button", "badge/badge"])
             |> Enum.map(& &1.id) == ["button/button", "badge/badge"]
    end

    test "an unknown id is dropped rather than rendered as nothing" do
      assert Catalogue.filter(DesignSystem.entries(), ids: ["nope/nope"]) == []
    end
  end

  describe "curated examples" do
    test "every curated example renders" do
      failures =
        for entry <- DesignSystem.entries(),
            example <- DesignSystem.curated(entry),
            {:error, message} <- [Render.to_safe(entry, example)] do
          "#{entry.id}/#{example.id}: #{message}"
        end

      assert failures == [], "these examples raised:\n\n" <> Enum.join(failures, "\n\n")
    end

    test "the element library is covered" do
      coverage = DesignSystem.coverage()

      uncovered =
        (coverage.defaults_only ++ coverage.needs_example)
        |> Enum.filter(&(&1.group == :element))
        |> Enum.map(& &1.id)

      # Not an assertion that coverage is total: `count_badge` and the layout
      # primitives render acceptably from their own defaults. This guards the
      # elements whose whole purpose is colour, where defaults prove nothing.
      assert "badge/badge" not in uncovered
      assert "button/button" not in uncovered
      assert "state/notice" not in uncovered
    end

    test "every page component has an example for each state a LiveView reaches" do
      {:ok, entry} = Catalogue.fetch("asset_catalogue_page/asset_catalogue_page")
      ids = DesignSystem.curated(entry) |> Enum.map(& &1.id)

      assert "empty" in ids
      assert "loading" in ids
      assert "error" in ids
    end
  end

  describe "generated modes" do
    test "defaults applies the component's own declared defaults" do
      {:ok, entry} = Catalogue.fetch("badge/status_badge")
      example = Generated.defaults(entry)

      assert example.source == :defaults
      assert example.unavailable == []
      assert Render.assigns_for(entry, example.attrs)[:size] == :sm
      assert {:ok, _safe} = Render.to_safe(entry, example)
    end

    test "matrix walks the values the attr declares" do
      {:ok, entry} = Catalogue.fetch("badge/status_badge")

      assert Generated.matrix(entry, :size) |> Enum.map(& &1.id) ==
               ["size=xs", "size=sm", "size=md"]
    end

    test "matrix over an attr with no declared values renders nothing" do
      {:ok, entry} = Catalogue.fetch("badge/status_badge")

      assert Generated.matrix(entry, :label) == []
    end

    test "a required attr that cannot be invented is reported, not guessed" do
      found =
        Enum.find_value(DesignSystem.entries(), fn entry ->
          example = Generated.defaults(entry)
          if example.unavailable != [], do: {entry, example}
        end)

      assert {_entry, example} = found,
             "no component has a required attr that resists sampling, which is suspicious"

      refute Example.renderable?(example)
    end

    test "every generated defaults example either renders or says why not" do
      broken =
        for entry <- DesignSystem.entries(),
            example = Generated.defaults(entry),
            example.unavailable == [],
            {:error, message} <- [Render.to_safe(entry, example)] do
          "#{entry.id}: #{message}"
        end

      assert broken == [],
             "these components claim to be renderable from defaults but raise:\n\n" <>
               Enum.join(broken, "\n\n")
    end
  end

  describe "render" do
    test "supplies globals and slots so a dynamic invocation does not crash" do
      {:ok, entry} = Catalogue.fetch("surface/panel")
      assigns = Render.assigns_for(entry, %{})

      assert assigns[:rest] == %{}
      assert assigns[:header] == []
      assert assigns[:padding] == :md
    end

    test "contains a raising example instead of failing the page" do
      {:ok, entry} = Catalogue.fetch("badge/status_badge")
      example = Example.attrs(:broken, %{tone: :info})

      assert {:error, message} = Render.to_safe(entry, example)
      assert message =~ "label"
    end
  end

  describe "query" do
    test "defaults to every entry, curated examples, and the dark theme" do
      query = Query.parse(%{})

      assert query.ids == []
      assert query.mode == :examples
      assert query.theme == "favn-dark"
      assert query.scale == 1.0
      assert query.chrome?
      assert query.format == :html
      assert query.warnings == []
    end

    test "accepts repeated and comma-separated ids" do
      assert Query.parse(%{"id" => ["badge/badge", "button/button,icon/icon"]}).ids ==
               ["badge/badge", "button/button", "icon/icon"]
    end

    test "parses the presentation parameters" do
      query =
        Query.parse(%{
          "theme" => "light",
          "width" => "1440",
          "scale" => "2.0",
          "chrome" => "0",
          "format" => "json"
        })

      assert query.theme == "favn-light"
      assert query.width == 1440
      assert query.scale == 2.0
      refute query.chrome?
      assert query.format == :json
    end

    test "reports an unusable value instead of failing the request" do
      query = Query.parse(%{"mode" => "sideways", "width" => "wide"})

      assert query.mode == :examples
      assert query.width == nil
      assert length(query.warnings) == 2
    end

    test "an axis that names no attr anywhere is a warning, not a new atom" do
      query = Query.parse(%{"axis" => "definitely_not_an_attr_anywhere"})

      assert query.axis == nil
      assert query.warnings != []
    end
  end

  describe "plan" do
    test "renders only what was asked for" do
      plan = DesignSystem.plan(Query.parse(%{"id" => "badge/badge", "example" => "tones"}))

      assert [%{entry: entry, examples: [example]}] = plan.items
      assert entry.id == "badge/badge"
      assert example.id == "tones"
    end

    test "a matrix uses the design system's preferred axis by default" do
      plan = DesignSystem.plan(Query.parse(%{"id" => "badge/badge", "mode" => "matrix"}))

      assert [%{examples: examples}] = plan.items
      assert Enum.all?(examples, &String.starts_with?(&1.id, "tone="))
    end

    test "an unknown id warns and points at the catalogue" do
      plan = DesignSystem.plan(Query.parse(%{"id" => "badge/nope"}))

      assert plan.items == []
      assert [warning] = plan.warnings
      assert warning =~ "/design-system"
    end
  end

  describe "pages" do
    test "the index renders every entry and reports the gaps" do
      html =
        FavnView.Dev.DesignSystemHTML.index(%{
          query: Query.parse(%{}),
          entries: DesignSystem.entries(),
          total: length(DesignSystem.entries()),
          coverage: DesignSystem.coverage(),
          rules: Audit.rules()
        })
        |> rendered_to_string()

      assert html =~ "Favn design system"
      assert html =~ "badge/status_badge"
      assert html =~ "FavnView.UI.Badge.status_badge/1"
    end

    test "the render page carries the audit rules and marks every example" do
      query = Query.parse(%{"id" => "badge/badge", "example" => "tones"})

      html =
        FavnView.Dev.DesignSystemHTML.show(%{
          plan: DesignSystem.plan(query),
          query: query,
          rules: Audit.rules_payload()
        })
        |> rendered_to_string()

      assert html =~ ~s|id="favn-design-system"|
      assert html =~ ~s|data-favn-example="badge/badge/tones"|
      assert html =~ "text_contrast"
      assert html =~ "/design-system/audit.js"
    end

    test "chrome=0 removes the labels but not the examples" do
      query = Query.parse(%{"id" => "badge/badge", "example" => "tones", "chrome" => "0"})

      html =
        FavnView.Dev.DesignSystemHTML.show(%{
          plan: DesignSystem.plan(query),
          query: query,
          rules: Audit.rules_payload()
        })
        |> rendered_to_string()

      assert html =~ ~s|data-favn-example="badge/badge/tones"|
      refute html =~ "FavnView.UI.Badge.badge/1</span>"
    end
  end

  describe "audit script" do
    test "reads its thresholds from the page instead of hard-coding them" do
      js = FavnView.Dev.DesignSystem.AuditScript.js()

      assert js =~ "data-audit-rules"
      refute js =~ "4.5"
    end

    test "exposes one call that returns both verdicts and boxes" do
      js = FavnView.Dev.DesignSystem.AuditScript.js()

      assert js =~ "window.favn.audit"
      assert js =~ "window.favn.summary"
    end
  end
end
