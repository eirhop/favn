defmodule FavnView.Components.AppShell do
  @moduledoc """
  Reusable ambient HUD shell for the Phoenix LiveView UI foundation.
  """

  use FavnView, :html

  alias FavnView.Components.IconNav
  alias FavnView.Components.ThemeToggle

  attr :title, :string, required: true
  attr :subtitle, :string, default: nil
  attr :status, :string, default: nil
  attr :status_tone, :atom, default: :success
  attr :nav_items, :list, default: []
  attr :back_href, :string, default: nil
  attr :back_label, :string, default: nil
  attr :facts, :list, default: []
  attr :show_header?, :boolean, default: true

  slot :inner_block, required: true
  slot :mode_rail

  def app_shell(assigns) do
    ~H"""
    <div class="favn-shell-bg text-base-content">
      <div class="favn-orbital-grid" aria-hidden="true"></div>
      <IconNav.icon_nav items={@nav_items} />

      <div class="relative z-10 flex h-screen min-h-0 flex-col px-5 py-3 md:py-4 md:pl-32 md:pr-8 lg:pr-32">
        <header class="mx-auto flex w-full max-w-7xl shrink-0 items-center justify-between gap-3">
          <div class="flex min-w-0 items-center gap-2">
            <IconNav.mobile_icon_nav items={@nav_items} />
            <a href={~p"/"} class="btn btn-ghost gap-2 px-2 md:hidden" aria-label="Favn home">
              <.icon name="hero-sparkles" class="size-5" />
              <span class="text-lg font-semibold">Favn</span>
            </a>

            <.link
              :if={@back_href && @back_label}
              navigate={@back_href}
              class="btn btn-ghost btn-sm min-w-0 gap-2 px-2 text-base-content/70 hover:text-primary md:px-3"
            >
              <.icon name="hero-arrow-left" class="size-4 shrink-0" />
              <span class="truncate">{@back_label}</span>
            </.link>
          </div>

          <div class="flex items-center gap-3">
            <button
              type="button"
              class="btn btn-ghost btn-square favn-icon-button favn-icon-rail rounded-box"
              aria-label="Search placeholder"
            >
              <.icon name="hero-magnifying-glass" class="size-5" />
            </button>
            <ThemeToggle.theme_toggle />
          </div>
        </header>

        <main class="mx-auto flex min-h-0 w-full max-w-7xl flex-1 flex-col justify-start overflow-y-auto py-4 md:py-6">
          <section
            :if={@show_header?}
            class="mb-4 flex flex-col gap-4 md:mb-5 lg:flex-row lg:items-end lg:justify-between"
          >
            <div class="min-w-0">
              <div class="flex flex-col gap-2 sm:flex-row sm:items-center md:gap-3">
                <h1 class="truncate text-2xl font-light tracking-tight text-base-content sm:text-3xl lg:text-4xl">
                  {@title}
                </h1>
                <span
                  :if={@status}
                  class={[
                    "badge badge-soft favn-status-glow gap-2 px-3 py-3",
                    status_badge_class(@status_tone)
                  ]}
                >
                  <span class={["status", status_dot_class(@status_tone)]}></span>
                  {@status}
                </span>
              </div>

              <p :if={@subtitle} class="mt-1.5 text-sm text-base-content/60 md:text-base">
                {@subtitle}
              </p>
            </div>

            <dl :if={@facts != []} class="grid gap-4 text-sm sm:grid-cols-3 lg:min-w-[28rem]">
              <div
                :for={fact <- @facts}
                class="border-base-content/20 sm:border-l sm:pl-5 first:border-l-0 first:pl-0"
              >
                <dt class="text-base-content/55">{fact.label}</dt>
                <dd class="mt-1 font-medium text-base-content">{fact.value}</dd>
              </div>
            </dl>
          </section>

          {render_slot(@inner_block)}
        </main>
      </div>

      {render_slot(@mode_rail)}
    </div>
    """
  end

  defp status_badge_class(:info), do: "badge-info"
  defp status_badge_class(:warning), do: "badge-warning"
  defp status_badge_class(:error), do: "badge-error"
  defp status_badge_class(:neutral), do: "badge-neutral"
  defp status_badge_class(_tone), do: "badge-success"

  defp status_dot_class(:info), do: "status-info"
  defp status_dot_class(:warning), do: "status-warning"
  defp status_dot_class(:error), do: "status-error"
  defp status_dot_class(:neutral), do: "status-neutral"
  defp status_dot_class(_tone), do: "status-success"
end
