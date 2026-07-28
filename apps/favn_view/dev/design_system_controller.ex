defmodule FavnView.Dev.DesignSystemController do
  @moduledoc """
  Serves the design-system browser.

  | Route | Purpose |
  | --- | --- |
  | `GET /design-system` | the index: every component, its contract, and its examples |
  | `GET /design-system/render` | renders the examples a query selects, and nothing else |
  | `GET /design-system/render?format=json` | the same selection as data |
  | `GET /design-system/audit.js` | the browser audit, loaded by the render page |

  A plain controller, not a LiveView. Nothing here needs a socket: components are
  pure functions of assigns, and every choice the viewer offers is in the URL. It
  also means the rendered page contains no LiveView runtime, so a screenshot
  shows the component and a measurement measures the component.
  """

  use FavnView, :controller

  alias FavnView.Dev.DesignSystem
  alias FavnView.Dev.DesignSystem.Audit
  alias FavnView.Dev.DesignSystem.AuditScript
  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Query
  alias FavnView.Dev.DesignSystem.Render

  plug :put_root_layout, false
  plug :put_layout, false

  @doc """
  The human-facing index.
  """
  def index(conn, params) do
    query = Query.parse(params)

    entries =
      DesignSystem.entries()
      |> FavnView.Dev.DesignSystem.Catalogue.filter(group: query.group, query: query.search)

    render(conn, :index,
      query: query,
      entries: entries,
      total: length(DesignSystem.entries()),
      coverage: DesignSystem.coverage(),
      rules: Audit.rules()
    )
  end

  @doc """
  Renders the selected examples.
  """
  def show(conn, params) do
    query = Query.parse(params)
    plan = DesignSystem.plan(query)

    case query.format do
      :json -> json(conn, payload(plan))
      :html -> render(conn, :show, plan: plan, query: query, rules: Audit.rules_payload())
    end
  end

  @doc """
  The audit script.
  """
  def audit_js(conn, _params) do
    conn
    |> put_resp_content_type("text/javascript")
    |> put_resp_header("cache-control", "no-store")
    |> send_resp(200, AuditScript.js())
  end

  defp payload(%{items: items, warnings: warnings}) do
    %{
      warnings: warnings,
      entries:
        Enum.map(items, fn %{entry: entry, examples: examples} ->
          %{
            id: entry.id,
            group: entry.group,
            component: Entry.label(entry),
            doc: entry.doc,
            required_attrs: Entry.required_attrs(entry),
            matrix_axes: Entry.matrix_axes(entry),
            attrs: Enum.map(entry.attrs, &attr_payload/1),
            slots: Enum.map(entry.slots, & &1.name),
            examples:
              Enum.map(examples, fn example ->
                %{
                  id: example.id,
                  doc: example.doc,
                  source: example.source,
                  unavailable: example.unavailable,
                  renders: renders?(entry, example)
                }
              end)
          }
        end)
    }
  end

  # An attr type can be a struct module (`{:struct, Module}`) and a declared value
  # can be any term, so both are rendered with `inspect/1`. JSON has no tuples,
  # and a payload that crashes on one component is useless for every other.
  defp attr_payload(attr) do
    %{
      name: attr.name,
      type: inspect(attr.type),
      required: attr.required,
      doc: attr.doc,
      default: inspect(Keyword.get(attr.opts, :default)),
      values: attr.opts |> Keyword.get(:values) |> encodable_values()
    }
  end

  defp encodable_values(nil), do: nil
  defp encodable_values(values), do: Enum.map(values, &inspect/1)

  # The JSON view reports whether an example actually renders, so a plan can be
  # checked without loading the page. It costs one render per example, which is
  # the same work the HTML view does.
  defp renders?(entry, example) do
    match?({:ok, _safe}, Render.to_safe(entry, example))
  end
end
