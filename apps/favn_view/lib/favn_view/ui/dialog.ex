defmodule FavnView.UI.Dialog do
  @moduledoc """
  A modal dialog, for a decision that should interrupt.

  ## When a dialog is right

  Reach for one when an action needs a few choices and the operator should not
  lose the screen they were reading. Configuration that lives permanently inline
  competes with the page for attention and, worse, invites a reflexive click on
  something consequential. A dialog makes the moment deliberate.

  Do not use one to show information. A dialog that only tells is a panel that
  stole the page.

  ## Behaviour

  Open state is a server assign, not browser state, because the action behind it
  is a server concern: a dialog reopened by a re-render must reflect what the
  server thinks is happening. Escape and a backdrop click both send `on_close`,
  so the dialog is dismissible the three ways operators expect — button,
  keyboard, and outside click.

  ## Examples

      <.dialog id="run-asset" open?={@open?} title="Run mart_daily_sales" on_close="close_run">
        <p>Runs the selected window with its upstream dependencies.</p>
        <:actions>
          <.button variant={:ghost} phx-click="close_run">Cancel</.button>
          <.button type="submit" form="run-form">Run asset</.button>
        </:actions>
      </.dialog>
  """

  use Phoenix.Component

  import FavnView.UI.Button

  attr :id, :string, required: true
  attr :open?, :boolean, default: false
  attr :title, :string, required: true
  attr :subtitle, :string, default: nil

  attr :on_close, :string,
    required: true,
    doc: "event sent by the close button, Escape, and the backdrop"

  attr :size, :atom, default: :md, values: [:md, :lg]
  attr :class, :any, default: nil

  slot :inner_block, required: true
  slot :actions, doc: "footer controls, in reading order: dismiss first, confirm last"

  def dialog(assigns) do
    ~H"""
    <div
      :if={@open?}
      id={@id}
      class={["modal modal-open", @class]}
      role="dialog"
      aria-modal="true"
      aria-labelledby={"#{@id}-title"}
      phx-window-keydown={@on_close}
      phx-key="escape"
      data-testid={@id}
    >
      <div class={["modal-box favn-surface-panel", size_class(@size)]}>
        <header class="mb-4 flex items-start justify-between gap-3">
          <div class="min-w-0">
            <h2 id={"#{@id}-title"} class="text-base font-medium">{@title}</h2>
            <p :if={@subtitle} class="favn-text-muted mt-1 text-xs">{@subtitle}</p>
          </div>
          <.icon_button
            icon="hero-x-mark"
            label="Close"
            size={:xs}
            phx-click={@on_close}
            data-testid={"#{@id}-close"}
          />
        </header>

        {render_slot(@inner_block)}

        <footer :if={@actions != []} class="modal-action">
          {render_slot(@actions)}
        </footer>
      </div>

      <button
        type="button"
        class="modal-backdrop cursor-default"
        phx-click={@on_close}
        aria-label="Close dialog"
        tabindex="-1"
      ></button>
    </div>
    """
  end

  defp size_class(:md), do: "max-w-xl"
  defp size_class(:lg), do: "max-w-3xl"
end
