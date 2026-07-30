defmodule FavnView.UI.Icon do
  @moduledoc """
  Heroicon rendering for the Favn UI.

  Icons come in outline (default), solid (`-solid`), and mini (`-mini`) styles
  and are extracted from `deps/heroicons` into `app.css` by the Tailwind plugin
  in `assets/vendor/heroicons.js`.

  Favn is icon-first, so sizes are part of the design system rather than
  free-form utilities:

    * `:xs` — inline with small metadata text
    * `:sm` — default; list rows, badges, buttons
    * `:md` — icon buttons and nav rails
    * `:lg` — empty and error states

  Pass `size` rather than a `size-*` utility in `class`. Passing both leaves the
  winner up to stylesheet order.

  ## Examples

      <.icon name="hero-x-mark" />
      <.icon name="hero-arrow-path" size={:md} class="motion-safe:animate-spin" />
  """

  use Phoenix.Component

  @sizes %{xs: "size-3.5", sm: "size-4", md: "size-5", lg: "size-8"}

  attr :name, :string, required: true, doc: "the heroicon name, for example `hero-check`"
  attr :size, :atom, default: nil, values: [nil, :xs, :sm, :md, :lg]
  attr :class, :any, default: nil, doc: "extra classes; prefer `size` for sizing"
  attr :rest, :global

  def icon(%{name: "hero-" <> _} = assigns) do
    assigns = assign(assigns, :size_class, size_class(assigns))

    ~H"""
    <span class={[@name, @size_class, @class]} aria-hidden="true" {@rest} />
    """
  end

  # An explicit size always wins. Otherwise only fall back to the default size
  # when the caller passed no classes at all, so a legacy `class="size-5"` does
  # not fight a default `size-4`.
  defp size_class(%{size: size}) when not is_nil(size), do: Map.fetch!(@sizes, size)
  defp size_class(%{class: nil}), do: @sizes.sm
  defp size_class(_assigns), do: nil
end
