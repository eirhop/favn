defmodule FavnView.UI.State do
  @moduledoc """
  Loading, empty, and error states.

  Every list, panel, and page has four states, and all four are part of the
  component contract: content, loading, empty, and error. A page that renders
  only the content state is incomplete, and its Storybook story must show the
  other three.

  ## Which state to render

  | State | Meaning | Recovery |
  | --- | --- | --- |
  | `loading_state/1` | the request is in flight | none; never offer an action |
  | `empty_state/1` | the request succeeded and returned nothing | change the filters, or create the thing |
  | `error_state/1` | the request failed | retry, or a link to the failure detail |

  An empty result is not an error. Never render `error_state/1` for zero rows.

  ## Examples

      <.loading_state label="Loading assets" />
      <.empty_state title="No assets found" description="Try changing the search or filters." />
      <.error_state title="Could not load assets">
        <:action><.button phx-click="reload">Retry</.button></:action>
      </.error_state>
  """

  use Phoenix.Component

  import FavnView.UI.Icon
  import FavnView.UI.Surface

  alias FavnView.UI.Tokens

  attr :label, :string, default: "Loading"
  attr :class, :any, default: nil
  attr :rest, :global

  def loading_state(assigns) do
    ~H"""
    <.panel padding={:lg} class={["mx-auto max-w-2xl text-center", @class]} {@rest}>
      <span class="loading loading-ring loading-lg text-primary" aria-hidden="true"></span>
      <p class="mt-4 text-base-content/60" role="status">{@label}</p>
    </.panel>
    """
  end

  attr :title, :string, required: true
  attr :description, :string, default: nil
  attr :icon, :string, default: "hero-inbox"
  attr :class, :any, default: nil
  attr :rest, :global

  slot :action, doc: "at most one primary action that resolves the emptiness"

  def empty_state(assigns) do
    ~H"""
    <.panel padding={:lg} class={["mx-auto max-w-2xl text-center", @class]} {@rest}>
      <span class="inline-flex size-14 items-center justify-center rounded-box bg-base-content/5 text-base-content/45">
        <.icon name={@icon} size={:lg} />
      </span>
      <h2 class="mt-4 text-lg font-medium tracking-tight">{@title}</h2>
      <p :if={@description} class="mt-2 text-sm text-base-content/60">{@description}</p>
      <div :if={@action != []} class="mt-5 flex justify-center">{render_slot(@action)}</div>
    </.panel>
    """
  end

  attr :title, :string, required: true
  attr :description, :string, default: nil
  attr :tone, :atom, default: :error, values: [:error, :warning]
  attr :class, :any, default: nil
  attr :rest, :global

  slot :action, doc: "a retry control or a link to the failure detail"

  def error_state(assigns) do
    ~H"""
    <.panel padding={:lg} class={["mx-auto max-w-2xl text-center", @class]} {@rest}>
      <span class={[
        "inline-flex size-14 items-center justify-center rounded-box",
        Tokens.surface_class(@tone),
        Tokens.text_class(@tone)
      ]}>
        <.icon name="hero-exclamation-triangle" size={:lg} />
      </span>
      <h2 class="mt-4 text-lg font-medium tracking-tight" role="alert">{@title}</h2>
      <p :if={@description} class="mt-2 text-sm text-base-content/60">{@description}</p>
      <div :if={@action != []} class="mt-5 flex justify-center">{render_slot(@action)}</div>
    </.panel>
    """
  end

  @doc """
  Inline spinner for a region that is refreshing while its content stays visible.

  Use it when replacing the content with `loading_state/1` would make the page
  jump. Never use both for the same region.
  """
  attr :label, :string, default: "Loading"
  attr :class, :any, default: nil

  def inline_loading(assigns) do
    ~H"""
    <span
      class={["inline-flex items-center gap-2 text-xs text-base-content/60", @class]}
      role="status"
    >
      <span class="loading loading-spinner loading-xs" aria-hidden="true"></span>
      {@label}
    </span>
    """
  end

  @doc """
  A compact inline notice inside an existing panel.

  For page-level failures use `error_state/1`; for transient feedback use flash.
  """
  attr :tone, :atom, default: :warning
  attr :icon, :string, default: nil
  attr :class, :any, default: nil
  attr :rest, :global

  slot :inner_block, required: true

  def notice(assigns) do
    assigns = assign(assigns, :tone, Tokens.tone(assigns.tone))

    ~H"""
    <div
      class={["alert alert-soft items-start text-sm", Tokens.alert_class(@tone), @class]}
      role="alert"
      {@rest}
    >
      <.icon name={@icon || notice_icon(@tone)} size={:md} class="shrink-0" />
      <div class="min-w-0">{render_slot(@inner_block)}</div>
    </div>
    """
  end

  defp notice_icon(:error), do: "hero-exclamation-circle"
  defp notice_icon(:warning), do: "hero-exclamation-triangle"
  defp notice_icon(:success), do: "hero-check-circle"
  defp notice_icon(_tone), do: "hero-information-circle"
end
