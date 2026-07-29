defmodule FavnView.Dev.DesignSystemHTML do
  @moduledoc """
  The two design-system pages.

  Both are complete documents rather than layouts, because the render page must
  contain nothing but the components under inspection: no application shell, no
  LiveView socket, no navigation. What the audit measures and what a screenshot
  shows are then the component and the page background, and nothing else.

  Chrome — the label above each example — is deliberately outside the
  `data-favn-example` element, so measuring an example never measures the
  viewer's own furniture.

  Each example element takes layout containment, which makes it the containing
  block for its own absolutely *and* fixed positioned descendants and its own
  stacking context. Without it a mode rail floated over whichever example
  happened to be beside it and an open dialog covered the page, because both
  resolved against the page instead of the example they belong to.
  """

  use FavnView, :html

  alias FavnView.Dev.DesignSystem
  alias FavnView.Dev.DesignSystem.Catalogue
  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Render

  @params [
    {"id=badge/badge,button/button", "one or more entry ids, comma separated"},
    {"group=element", "element, component, or page"},
    {"q=badge", "substring match on the entry id"},
    {"mode=examples", "examples, defaults, matrix, or all"},
    {"axis=tone", "the attr a matrix walks"},
    {"example=tones", "substring match on the example id"},
    {"theme=dark", "dark or light"},
    {"width=1440", "content width in CSS pixels"},
    {"scale=2", "page zoom, for crisp screenshots"},
    {"chrome=0", "hide the labels and warnings"},
    {"format=json", "the selection as data instead of HTML"}
  ]

  @doc """
  The index: every component, grouped, with its contract and its examples.

  The catalogue comes first — this page exists to be browsed. The querying
  reference and the coverage gap lists live at the bottom behind anchors, so
  they are one click away without costing a screen of scrolling.
  """
  def index(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en" data-theme="favn-dark">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Favn design system</title>
        <link rel="stylesheet" href="/assets/css/app.css" />
      </head>
      <body class="favn-shell-backdrop min-h-screen text-base-content">
        <main class="mx-auto max-w-6xl space-y-8 p-6">
          <header class="space-y-3">
            <.eyebrow>Development only</.eyebrow>
            <.page_title>Favn design system</.page_title>
            <p class="max-w-3xl text-sm text-base-content/70">
              Every function component in <code class="font-mono">FavnView.UI</code>
              and <code class="font-mono">FavnView.Components</code>, discovered by reflection
              rather than registered by hand.
            </p>
            <.inline gap={:sm}>
              <.badge tone={:neutral} variant={:outline}>{@total} components</.badge>
              <.badge tone={:success} variant={:outline}>
                {length(@coverage.covered)} with an example
              </.badge>
              <a :if={@coverage.needs_example != []} href="#needs-example">
                <.badge tone={:warning} variant={:outline}>
                  {length(@coverage.needs_example)} need one
                </.badge>
              </a>
              <a href="#querying">
                <.badge tone={:neutral} variant={:outline}>querying &amp; audit</.badge>
              </a>
            </.inline>
          </header>

          <.toolbar query={@query} entries={@entries} total={@total} />

          <p :if={@entries == []} class="text-sm text-base-content/60">
            Nothing matches. <a class="link" href="/design-system">Show everything</a>.
          </p>

          <.group_section :for={group <- Catalogue.groups()} group={group} entries={@entries} />

          <section id="reference" class="scroll-mt-6 space-y-4 border-t border-base-content/10 pt-8">
            <.usage />

            <.gap_panel
              :if={@coverage.needs_example != []}
              id="needs-example"
              entries={@coverage.needs_example}
              tone={:warning}
              title="Cannot render without an example"
              subtitle="A required assign here is a domain-shaped value the design system will not invent."
            />

            <.gap_panel
              :if={@coverage.defaults_only != []}
              id="defaults-only"
              entries={@coverage.defaults_only}
              tone={:neutral}
              title="Renders from its own declared defaults"
              subtitle="Mostly parts composed inside a larger component. An example adds intent, not visibility."
            />
          </section>
        </main>
      </body>
    </html>
    """
  end

  @doc """
  The render page: only the examples the query selected.
  """
  def show(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en" data-theme={@query.theme}>
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>{document_title(@plan)}</title>
        <link rel="stylesheet" href="/assets/css/app.css" />
        <style>
          /* An example holding an overlay is the containing block for it, so the
             overlay needs somewhere to be: a modal fills its container, and a
             container sized to a button clips the dialog inside it. This is the
             viewer's own furniture, which is why it is here and not in the
             product stylesheet. */
          section[data-favn-example]:has(.modal, [role="dialog"]) { min-height: 36rem; }
        </style>
        <script defer src="/design-system/audit.js">
        </script>
      </head>
      <body class="text-base-content">
        <div
          id="favn-design-system"
          class="favn-shell-backdrop min-h-screen"
          data-theme={@query.theme}
          data-scale={@query.scale}
          data-audit-rules={Jason.encode!(@rules)}
          style={zoom_style(@query.scale)}
        >
          <div class="mx-auto space-y-6 p-6" style={width_style(@query.width)}>
            <.notice :for={warning <- warnings(@query, @plan)} tone={:warning}>{warning}</.notice>

            <p :if={@query.chrome? and @plan.items == []} class="text-sm text-base-content/70">
              Nothing selected. Open <code class="font-mono">/design-system</code> for the catalogue.
            </p>

            <section :for={item <- @plan.items} class="space-y-3">
              <div :if={@query.chrome?} class="flex flex-wrap items-baseline gap-2">
                <h2 class="font-mono text-sm text-base-content/80">{item.entry.id}</h2>
                <span class="font-mono text-xs text-base-content/40">{Entry.label(item.entry)}</span>
              </div>

              <p :if={@query.chrome? and item.examples == []} class="text-xs text-base-content/50">
                No examples in this mode.
              </p>

              <.example_frame
                :for={example <- item.examples}
                entry={item.entry}
                example={example}
                chrome?={@query.chrome?}
              />
            </section>
          </div>
        </div>
      </body>
    </html>
    """
  end

  attr :query, :map, required: true
  attr :entries, :list, required: true
  attr :total, :integer, required: true

  defp toolbar(assigns) do
    ~H"""
    <div class="space-y-2">
      <div class="flex flex-wrap items-center gap-2">
        <form method="get" action="/design-system" class="w-full max-w-xs">
          <input :if={@query.group} type="hidden" name="group" value={@query.group} />
          <input
            type="search"
            name="q"
            value={@query.search}
            placeholder="Filter by id, press enter"
            class="input input-sm w-full font-mono"
          />
        </form>

        <nav class="flex flex-wrap items-center gap-1.5">
          <.group_pill label="all" group={nil} query={@query} />
          <.group_pill
            :for={group <- Catalogue.groups()}
            label={group_title(group)}
            group={group}
            query={@query}
          />
        </nav>

        <div class="ml-auto flex flex-wrap items-center gap-1.5 text-xs text-base-content/50">
          <a :for={group <- Catalogue.groups()} class="link-hover link" href={"#group-#{group}"}>
            {group_title(group)}&nbsp;↓
          </a>
        </div>
      </div>

      <p :if={filtered?(@query)} class="text-xs text-base-content/60">
        Showing {length(@entries)} of {@total} components.
        <a class="link" href="/design-system">Clear</a>
      </p>
    </div>
    """
  end

  attr :label, :string, required: true
  attr :group, :atom, required: true
  attr :query, :map, required: true

  defp group_pill(assigns) do
    ~H"""
    <a
      class={[
        "badge badge-sm",
        if(@query.group == @group, do: "badge-primary", else: "badge-outline")
      ]}
      href={index_path(group: @group, q: @query.search)}
    >
      {String.downcase(@label)}
    </a>
    """
  end

  attr :id, :string, required: true
  attr :entries, :list, required: true
  attr :tone, :atom, required: true
  attr :title, :string, required: true
  attr :subtitle, :string, required: true

  defp gap_panel(assigns) do
    ~H"""
    <div id={@id} class="scroll-mt-6">
      <.panel>
        <:header title={@title} subtitle={@subtitle} icon="hero-exclamation-triangle" />
        <details>
          <summary class="cursor-pointer text-xs text-base-content/60">
            {length(@entries)} components
          </summary>
          <ul class="mt-3 flex flex-wrap gap-2">
            <li :for={entry <- @entries}>
              <a
                class={[
                  "font-mono text-xs underline decoration-dotted",
                  Tokens.text_class(@tone)
                ]}
                href={render_path(entry.id, mode: "defaults")}
              >
                {entry.id}
              </a>
            </li>
          </ul>
        </details>
      </.panel>
    </div>
    """
  end

  attr :group, :atom, required: true
  attr :entries, :list, required: true

  defp group_section(assigns) do
    assigns =
      assign(assigns, :grouped, Enum.filter(assigns.entries, &(&1.group == assigns.group)))

    ~H"""
    <section :if={@grouped != []} id={"group-#{@group}"} class="scroll-mt-6 space-y-3">
      <div class="flex flex-wrap items-baseline gap-2">
        <.section_title>{group_title(@group)}</.section_title>
        <span class="text-xs text-base-content/50">{length(@grouped)}</span>
      </div>
      <p class="text-sm text-base-content/60">{group_doc(@group)}</p>
      <div class="grid items-start gap-2.5 md:grid-cols-2">
        <.entry_card :for={entry <- @grouped} entry={entry} />
      </div>
    </section>
    """
  end

  attr :entry, Entry, required: true

  defp entry_card(assigns) do
    examples = DesignSystem.curated(assigns.entry)

    assigns =
      assigns
      |> assign(:examples, examples)
      |> assign(:axes, Entry.matrix_axes(assigns.entry))
      |> assign(:required, Entry.required_attrs(assigns.entry))
      |> assign(:doc, doc_summary(assigns.entry.doc))
      |> assign(
        :href,
        if(examples == [],
          do: render_path(assigns.entry.id, mode: "defaults"),
          else: render_path(assigns.entry.id)
        )
      )

    ~H"""
    <.list_card>
      <div class="space-y-2">
        <div class="min-w-0">
          <a class="font-mono text-sm font-medium text-primary hover:underline" href={@href}>
            {@entry.id}
          </a>
          <p class="truncate font-mono text-[11px] text-base-content/40">{Entry.label(@entry)}</p>
        </div>

        <p :if={@doc} class="text-xs text-base-content/65">{@doc}</p>

        <div class="flex flex-wrap gap-1.5">
          <a
            :for={example <- @examples}
            class="badge badge-sm badge-outline font-mono"
            href={render_path(@entry.id, example: example.id)}
          >
            {example.id}
          </a>
          <a
            class="badge badge-sm badge-outline font-mono opacity-60"
            href={render_path(@entry.id, mode: "defaults")}
          >
            defaults
          </a>
          <a
            :for={axis <- @axes}
            class="badge badge-sm badge-outline font-mono opacity-60"
            href={render_path(@entry.id, mode: "matrix", axis: axis)}
          >
            {axis}=*
          </a>
        </div>

        <dl class="grid grid-cols-[5rem_1fr] gap-x-3 gap-y-1 text-xs">
          <dt class="text-base-content/45">Required</dt>
          <dd class="font-mono text-base-content/75">
            {if @required == [], do: "—", else: Enum.join(@required, ", ")}
          </dd>
          <div :if={@entry.slots != []} class="contents">
            <dt class="text-base-content/45">Slots</dt>
            <dd class="font-mono text-base-content/75">
              {Enum.map_join(@entry.slots, ", ", &slot_label/1)}
            </dd>
          </div>
        </dl>
      </div>
    </.list_card>
    """
  end

  attr :entry, Entry, required: true
  attr :example, :map, required: true
  attr :chrome?, :boolean, default: true

  defp example_frame(assigns) do
    assigns = assign(assigns, :result, Render.to_safe(assigns.entry, assigns.example))

    ~H"""
    <div class="space-y-1.5">
      <div :if={@chrome?} class="flex flex-wrap items-baseline gap-2">
        <span class="font-mono text-xs text-base-content/55">{@example.id}</span>
        <span :if={@example.source != :curated} class="badge badge-xs badge-outline font-mono">
          {@example.source}
        </span>
        <span :if={@example.doc} class="text-xs text-base-content/45">{@example.doc}</span>
      </div>

      <section
        class="relative isolate [contain:layout]"
        data-favn-example={"#{@entry.id}/#{@example.id}"}
        data-favn-component={Entry.label(@entry)}
        data-favn-source={@example.source}
        data-favn-error={if(rendered?(@result), do: "0", else: "1")}
      >
        {if rendered?(@result), do: elem(@result, 1)}
        <.render_error :if={not rendered?(@result)} example={@example} message={elem(@result, 1)} />
      </section>
    </div>
    """
  end

  attr :example, :map, required: true
  attr :message, :string, required: true

  defp render_error(assigns) do
    ~H"""
    <div class="rounded-box border border-error/40 bg-error/10 p-3 text-xs">
      <p class="font-medium text-error">This example did not render.</p>
      <p :if={@example.unavailable != []} class="mt-1 text-base-content/70">
        Required assigns the design system cannot invent: <span class="font-mono">{Enum.join(@example.unavailable, ", ")}</span>. Add a curated example.
      </p>
      <pre class="mt-2 overflow-x-auto whitespace-pre-wrap font-mono text-[11px] text-base-content/70">{@message}</pre>
    </div>
    """
  end

  defp usage(assigns) do
    assigns = assign(assigns, :params, @params)

    ~H"""
    <div id="querying" class="scroll-mt-6">
      <.panel>
        <:header title="Querying" subtitle="The URL is the whole interface" icon="hero-link" />
        <div class="space-y-3">
          <dl class="grid grid-cols-[12rem_1fr] gap-x-4 gap-y-1.5 text-xs">
            <div :for={{param, meaning} <- @params} class="contents">
              <dt class="font-mono text-base-content/55">{param}</dt>
              <dd class="text-base-content/75">{meaning}</dd>
            </div>
          </dl>
          <p class="text-xs text-base-content/60">
            After loading a render URL, call
            <code class="font-mono text-base-content/80">window.favn.audit()</code>
            for measured verdicts and the bounding box of every example, or
            <code class="font-mono text-base-content/80">window.favn.summary()</code>
            for just the failures and the boxes.
          </p>
        </div>
      </.panel>
    </div>
    """
  end

  defp warnings(%{chrome?: false}, _plan), do: []
  defp warnings(_query, %{warnings: warnings}), do: warnings

  defp rendered?({:ok, _safe}), do: true
  defp rendered?({:error, _message}), do: false

  defp render_path(id, params \\ []) do
    query =
      [{"id", id} | Enum.map(params, fn {key, value} -> {to_string(key), to_string(value)} end)]
      |> URI.encode_query()

    "/design-system/render?" <> query
  end

  defp index_path(params) do
    case params
         |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
         |> Enum.map(fn {key, value} -> {to_string(key), to_string(value)} end)
         |> URI.encode_query() do
      "" -> "/design-system"
      query -> "/design-system?" <> query
    end
  end

  defp filtered?(query), do: query.group != nil or query.search not in [nil, ""]

  defp zoom_style(scale) when scale == 1.0, do: nil
  defp zoom_style(scale), do: "zoom: #{scale}"

  defp width_style(nil), do: nil
  defp width_style(width), do: "max-width: #{width}px"

  defp document_title(%{items: [%{entry: entry}]}), do: "#{entry.id} · Favn design system"
  defp document_title(%{items: items}), do: "#{length(items)} components · Favn design system"

  defp group_title(:element), do: "Elements"
  defp group_title(:component), do: "Components"
  defp group_title(:page), do: "Pages"

  defp group_doc(:element) do
    "FavnView.UI: the layer that owns colour, border, radius, and spacing."
  end

  defp group_doc(:component) do
    "FavnView.Components: sections that compose elements into part of a screen."
  end

  defp group_doc(:page) do
    "Whole screens. Each one is a pure function of a single view model."
  end

  defp slot_label(%{name: name, required: true}), do: "#{name}*"
  defp slot_label(%{name: name}), do: to_string(name)

  # A component's doc is markdown, and Phoenix appends generated "## Attributes"
  # and "## Slots" sections to it. The card wants the first sentence of prose,
  # not a heading, so everything from the first heading on is dropped.
  defp doc_summary(nil), do: nil

  defp doc_summary(doc) do
    doc
    |> String.split("\n")
    |> Enum.take_while(&(not String.starts_with?(String.trim_leading(&1), "#")))
    |> Enum.join("\n")
    |> String.split("\n\n", parts: 2)
    |> hd()
    |> String.replace(~r/\s+/, " ")
    |> String.trim()
    |> case do
      "" -> nil
      summary -> summary
    end
  end
end
