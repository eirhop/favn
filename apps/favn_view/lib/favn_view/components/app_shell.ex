defmodule FavnView.Components.AppShell do
  @moduledoc """
  The application frame every operator screen renders inside.

  The shell owns the ambient background, the primary navigation, the one page
  title, the account controls, flash, and the slot where a page mounts its mode
  rail. A page component supplies content and metadata; it never draws chrome of
  its own and never renders a second `page_title/1`.

  ## Layout

      ┌──────────────────────────────────────────────┐
      │ nav rail │ title · status        controls     │  header
      │          ├──────────────────────────────────  │
      │          │ facts                    actions   │  toolbar (optional)
      │          │                                    │
      │          │ inner_block                 mode   │  content
      │          │                             rail   │
      └──────────────────────────────────────────────┘

  ## Examples

      <.app_shell title="Runs" subtitle="Recent executions" nav_items={Navigation.items(:runs)} flash={@flash}>
        <.runs_table runs={@runs} />
        <:mode_rail><.mode_rail modes={@modes} active={@mode} on_select="set_mode" /></:mode_rail>
      </.app_shell>
  """

  use FavnView, :html

  alias FavnView.Components.NavRail

  attr :title, :string, required: true, doc: "names the screen; the only page title"
  attr :subtitle, :string, default: nil
  attr :status, :string, default: nil, doc: "one live status word for the screen"
  attr :status_tone, :atom, default: :success, doc: "see `FavnView.UI.Tokens`"
  attr :nav_items, :list, default: [], doc: "see `FavnView.Components.Navigation.items/1`"
  attr :back_href, :string, default: nil
  attr :back_label, :string, default: nil
  attr :facts, :list, default: [], doc: "toolbar facts; see `FavnView.UI.Data.fact_list/1`"
  attr :flash, :map, default: %{}, doc: "the LiveView flash map"

  attr :content_scroll?, :boolean,
    default: true,
    doc: "set false when the page owns its own scroll region, such as a canvas"

  slot :inner_block, required: true
  slot :mode_rail, doc: "page-local mode controls; see `FavnView.Components.ModeRail`"
  slot :actions, doc: "screen-level actions, rendered in the toolbar"

  def app_shell(assigns) do
    ~H"""
    <div class="favn-shell-bg text-base-content">
      <div class="favn-orbital-grid" aria-hidden="true"></div>
      <NavRail.nav_rail items={@nav_items} />

      <div class="relative z-10 flex h-screen min-h-0 flex-col px-5 py-3 md:py-4 md:pl-32 md:pr-8 lg:pr-32">
        <header class="mx-auto flex w-full max-w-[120rem] shrink-0 items-center justify-between gap-3">
          <div class="flex min-w-0 items-center gap-3">
            <NavRail.nav_menu items={@nav_items} />
            <.link navigate={~p"/"} class="btn btn-ghost gap-2 px-2 md:hidden" aria-label="Favn home">
              <.icon name="hero-sparkles" size={:md} />
              <span class="text-lg font-semibold">Favn</span>
            </.link>

            <.button
              :if={@back_href && @back_label}
              variant={:ghost}
              navigate={@back_href}
              icon="hero-arrow-left"
              class="min-w-0 px-2 favn-text-muted hover:text-primary md:px-3"
            >
              <span class="truncate">{@back_label}</span>
            </.button>

            <div class="min-w-0">
              <div class="flex min-w-0 items-center gap-2">
                <.page_title compact>{@title}</.page_title>
                <.status_badge
                  :if={@status}
                  tone={@status_tone}
                  label={@status}
                  size={:sm}
                  glow
                  class="hidden sm:inline-flex"
                />
              </div>
              <.meta :if={@subtitle} title={@subtitle}>{@subtitle}</.meta>
            </div>
          </div>

          <div class="flex items-center gap-3">
            <FavnView.Components.ThemeToggle.theme_toggle />
            <form action={~p"/logout"} method="post">
              <input type="hidden" name="_method" value="delete" />
              <input type="hidden" name="_csrf_token" value={get_csrf_token()} />
              <.icon_button type="submit" icon="hero-arrow-right-on-rectangle" label="Sign out" />
            </form>
          </div>
        </header>

        <main class={[
          "mx-auto flex min-h-0 w-full max-w-[120rem] flex-1 flex-col justify-start py-2 md:py-3",
          if(@content_scroll?, do: "overflow-y-auto", else: "overflow-hidden")
        ]}>
          <div
            :if={@facts != [] || @actions != []}
            class="mb-3 flex shrink-0 flex-col gap-3 border-b border-base-content/10 pb-3 sm:flex-row sm:items-end sm:justify-between"
          >
            <.fact_list :if={@facts != []} facts={@facts} class="flex-1" />

            <div :if={@actions != []} class="flex shrink-0 justify-start sm:justify-end">
              {render_slot(@actions)}
            </div>
          </div>

          {render_slot(@inner_block)}
        </main>
      </div>

      {render_slot(@mode_rail)}
      <.flash_group flash={@flash} />
    </div>
    """
  end
end
