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
  attr :nav_items, :list, default: []

  slot :inner_block, required: true
  slot :mode_rail

  def app_shell(assigns) do
    ~H"""
    <div class="favn-shell-bg text-base-content">
      <div class="favn-orbital-grid" aria-hidden="true"></div>
      <IconNav.icon_nav items={@nav_items} />

      <div class="relative z-10 min-h-screen px-5 py-4 md:py-5 md:pl-32 md:pr-8 lg:pr-32">
        <header class="mx-auto flex max-w-7xl items-center justify-between gap-3">
          <div class="flex items-center gap-2 md:hidden">
            <IconNav.mobile_icon_nav items={@nav_items} />
            <a href={~p"/"} class="btn btn-ghost gap-2 px-2" aria-label="Favn home">
              <.icon name="hero-sparkles" class="size-5" />
              <span class="text-lg font-semibold">Favn</span>
            </a>
          </div>

          <div class="hidden md:block" aria-hidden="true"></div>

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

        <main class="mx-auto flex min-h-[calc(100vh-5.5rem)] max-w-7xl flex-col justify-start py-8 md:justify-center md:py-12">
          <section class="mb-6 flex flex-col gap-3 sm:flex-row sm:items-start md:mb-8 md:gap-4">
            <div>
              <h1 class="text-3xl font-light tracking-tight text-base-content sm:text-5xl lg:text-6xl">
                {@title}
              </h1>
              <p :if={@subtitle} class="mt-2 text-base text-base-content/60 md:mt-3 md:text-lg">
                {@subtitle}
              </p>
            </div>
            <span
              :if={@status}
              class="badge badge-success badge-soft favn-status-glow gap-2 px-4 py-4"
            >
              <span class="status status-success"></span>
              {@status}
            </span>
          </section>

          {render_slot(@inner_block)}
        </main>
      </div>

      {render_slot(@mode_rail)}
    </div>
    """
  end
end
