defmodule FavnView.Dev.DesignSystem.Example do
  @moduledoc """
  One rendering of one component.

  An example is either **attrs** or **render**, never both:

    * `:attrs` — a map of assigns applied to the entry's own component
      function. Use this whenever the component takes no slots; it is data, so
      it stays short and cannot drift into a second implementation of the
      component.
    * `:render` — a one-arity function returning a HEEx block. Use this when the
      example needs slots, `:let`, or several components composed together.
      Because it is real HEEx compiled with the module, `attr` defaults and
      slot validation apply exactly as they do in product code.

  `:source` records where the example came from, so a screenshot can be traced
  back to either a curated example or one the design system generated from the
  component's own metadata.
  """

  @type source :: :curated | :defaults | :matrix

  @type t :: %__MODULE__{
          id: String.t(),
          doc: String.t() | nil,
          attrs: map() | nil,
          render: (map() -> Phoenix.LiveView.Rendered.t()) | nil,
          source: source(),
          unavailable: [atom()]
        }

  @enforce_keys [:id]
  defstruct [:id, :doc, :attrs, :render, source: :curated, unavailable: []]

  @doc """
  Builds a curated attrs example.
  """
  @spec attrs(atom() | String.t(), map(), String.t() | nil) :: t()
  def attrs(id, attrs, doc \\ nil) when is_map(attrs) do
    %__MODULE__{id: to_string(id), attrs: attrs, doc: doc}
  end

  @doc """
  Builds a curated HEEx example.
  """
  @spec render(atom() | String.t(), (map() -> Phoenix.LiveView.Rendered.t()), String.t() | nil) ::
          t()
  def render(id, fun, doc \\ nil) when is_function(fun, 1) do
    %__MODULE__{id: to_string(id), render: fun, doc: doc}
  end

  @doc """
  True when the example cannot be rendered because required assigns are missing.
  """
  @spec renderable?(t()) :: boolean()
  def renderable?(%__MODULE__{unavailable: []}), do: true
  def renderable?(%__MODULE__{}), do: false
end
