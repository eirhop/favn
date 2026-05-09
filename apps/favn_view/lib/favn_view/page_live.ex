defmodule FavnView.PageLive do
  @moduledoc false

  use FavnView, :live_view

  @impl true
  def render(assigns) do
    ~H"""
    <main class="min-h-screen bg-slate-950 px-6 py-16 text-slate-100 sm:px-10">
      <section class="mx-auto flex max-w-3xl flex-col gap-6">
        <p class="text-sm font-semibold uppercase tracking-[0.3em] text-orange-300">
          Favn
        </p>
        <h1 class="text-4xl font-semibold tracking-tight sm:text-6xl">Favn View</h1>
        <p class="max-w-2xl text-lg leading-8 text-slate-300">
          Phoenix and LiveView shell for the next Favn UI. The real operator
          screens will be added behind the public orchestrator facade in a later PR.
        </p>
      </section>
    </main>
    """
  end
end
