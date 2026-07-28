defmodule FavnView.Dev.DesignSystem do
  @moduledoc """
  The Favn design-system browser: a development-only view of every component.

  It replaces Storybook with something smaller and addressable. Three things
  differ, and each one exists because the story-based version made a specific
  job harder:

    * **The catalogue is reflected, not written.** Entries come from
      `Phoenix.Component.__components__/0` through
      `FavnView.Dev.DesignSystem.Catalogue`, so a component cannot be missing
      from the index and an attr list cannot describe a contract the code no
      longer has. A component with no example is listed as a gap.

    * **The URL is the query.** `?id=badge/badge&mode=matrix&axis=tone` renders
      exactly those badges and nothing else, so a screenshot of the page is a
      screenshot of the subject. See `FavnView.Dev.DesignSystem.Query`.

    * **Verdicts come before pixels.** `FavnView.Dev.DesignSystem.Audit` owns
      the thresholds; the browser measures the rendered result and reports
      contrast, target size, accessible names, and clipping as pass, fail, or
      skipped — together with the bounding box of every example, so a screenshot
      can be cropped to one component afterwards without a second lookup.

  ## Where it lives

  Everything under `apps/favn_view/dev/` is outside `elixirc_paths` for `:prod`,
  so none of it is compiled into a release. `FavnView.MixProject.elixirc_paths/1`
  and `test/favn_view/design_system_isolation_test.exs` enforce that together.

  ## Examples and fixtures

  Curated examples live in `FavnView.Dev.DesignSystem.Examples`. Page examples
  reuse the same view-model fixtures the component tests use, so a page rendered
  here and a page asserted in a test are the same page.
  """

  alias FavnView.Dev.DesignSystem.Catalogue
  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Examples
  alias FavnView.Dev.DesignSystem.Generated
  alias FavnView.Dev.DesignSystem.Query

  @preferred_axes [:tone, :variant, :size, :state]

  @type item :: %{entry: Entry.t(), examples: [Example.t()]}
  @type plan :: %{items: [item()], warnings: [String.t()], query: Query.t()}

  @doc """
  Every catalogue entry.
  """
  @spec entries() :: [Entry.t()]
  def entries, do: Catalogue.entries()

  @doc """
  Resolves a query into the entries to render and the examples to render for each.
  """
  @spec plan(Query.t()) :: plan()
  def plan(%Query{} = query) do
    selected =
      Catalogue.filter(entries(),
        ids: query.ids,
        group: query.group,
        query: query.search
      )

    unknown = query.ids -- Enum.map(selected, & &1.id)

    items =
      for entry <- selected do
        %{entry: entry, examples: examples_for(entry, query)}
      end

    %{items: items, warnings: query.warnings ++ unknown_warnings(unknown), query: query}
  end

  @doc """
  The examples to render for one entry under one query.
  """
  @spec examples_for(Entry.t(), Query.t()) :: [Example.t()]
  def examples_for(%Entry{} = entry, %Query{} = query) do
    entry
    |> collect(query.mode, query.axis)
    |> filter_examples(query.example)
  end

  @doc """
  Curated examples for an entry, ignoring mode.
  """
  @spec curated(Entry.t()) :: [Example.t()]
  def curated(%Entry{id: id}), do: Examples.for_entry(id)

  @doc """
  How well the catalogue is covered, in three buckets.

  This is the honest replacement for "every component has a story": nothing
  enforces coverage, so the index reports what is missing instead of implying
  nothing is.

  The distinction that matters is the second bucket versus the third. A component
  with no curated example that still renders from its own declared defaults is
  documented well enough to look at — most of them are small parts composed
  inside a larger component. One that cannot render at all shows nothing until
  somebody writes an example, and that is the real gap.
  """
  @spec coverage() :: %{
          covered: [Entry.t()],
          defaults_only: [Entry.t()],
          needs_example: [Entry.t()]
        }
  def coverage do
    buckets = Enum.group_by(entries(), &bucket/1)

    %{
      covered: Map.get(buckets, :covered, []),
      defaults_only: Map.get(buckets, :defaults_only, []),
      needs_example: Map.get(buckets, :needs_example, [])
    }
  end

  defp bucket(entry) do
    cond do
      curated(entry) != [] -> :covered
      Example.renderable?(Generated.defaults(entry)) -> :defaults_only
      true -> :needs_example
    end
  end

  @doc """
  The attr a matrix walks when the query does not name one.

  Prefers the axes that carry meaning in this design system, then falls back to
  whichever attr declares `values`.
  """
  @spec default_axis(Entry.t()) :: atom() | nil
  def default_axis(%Entry{} = entry) do
    axes = Generated.axes(entry)

    Enum.find(@preferred_axes, &(&1 in axes)) || List.first(axes)
  end

  defp collect(entry, :examples, _axis), do: curated(entry)
  defp collect(entry, :defaults, _axis), do: [Generated.defaults(entry)]

  defp collect(entry, :matrix, axis) do
    case axis || default_axis(entry) do
      nil -> []
      axis -> Generated.matrix(entry, axis)
    end
  end

  defp collect(entry, :all, axis) do
    matrix = Enum.flat_map(matrix_axes(entry, axis), &Generated.matrix(entry, &1))

    curated(entry) ++ [Generated.defaults(entry)] ++ matrix
  end

  defp matrix_axes(entry, nil), do: Generated.axes(entry)
  defp matrix_axes(_entry, axis), do: [axis]

  defp filter_examples(examples, nil), do: examples

  defp filter_examples(examples, needle) do
    Enum.filter(examples, &String.contains?(&1.id, needle))
  end

  defp unknown_warnings([]), do: []

  defp unknown_warnings(ids) do
    Enum.map(ids, &"no entry with id #{inspect(&1)}; check /design-system for the list")
  end
end
