defmodule FavnView.UI do
  @moduledoc """
  The Favn UI element library.

  These are the lowest layer of the three-layer component system:

      LiveView -> page component -> section components -> UI elements

  Elements know nothing about Favn's domain. They take a tone, a label, and a
  slot, and they own every border, background, radius, and transition in the
  product. Sections and pages compose them and must not re-implement their
  styling with utility classes.

  | Module | Owns |
  | --- | --- |
  | `FavnView.UI.Tokens` | the tone vocabulary and its DaisyUI classes |
  | `FavnView.UI.Typography` | the type scale |
  | `FavnView.UI.Layout` | the spacing scale: stack, inline, columns |
  | `FavnView.UI.Icon` | heroicons and icon sizes |
  | `FavnView.UI.Surface` | the four glass surfaces |
  | `FavnView.UI.Button` | buttons, icon buttons, button groups |
  | `FavnView.UI.Badge` | badges, status badges, status dots |
  | `FavnView.UI.State` | loading, empty, error, notice |
  | `FavnView.UI.Data` | fact lists, tables, metrics, monospaced values |
  | `FavnView.UI.Field` | filters and form inputs |
  | `FavnView.UI.Feedback` | flash toasts and connection notices |

  `use FavnView, :html` already imports all of them, so components can call
  `<.button>`, `<.panel>`, or `<.status_badge>` directly. Import this module
  explicitly only outside the `FavnView` helpers, such as in a Storybook story
  that renders elements without a page component.

  Documentation for the visual rules these components encode lives in
  `docs/design/style-guide.md`; the composition rules live in
  `docs/design/component-patterns.md`.
  """

  @doc """
  Imports every element module.
  """
  defmacro __using__(_opts) do
    quote do
      import FavnView.UI.Badge
      import FavnView.UI.Button
      import FavnView.UI.Data
      import FavnView.UI.Feedback
      import FavnView.UI.Field
      import FavnView.UI.Icon
      import FavnView.UI.Layout, except: [steps: 0]
      import FavnView.UI.State
      import FavnView.UI.Surface
      import FavnView.UI.Typography, except: [class: 1, steps: 0]

      alias FavnView.UI.Tokens
    end
  end
end
