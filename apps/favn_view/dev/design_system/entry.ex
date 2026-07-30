defmodule FavnView.Dev.DesignSystem.Entry do
  @moduledoc """
  One renderable thing in the design system: a single function component.

  An entry is derived from `Phoenix.Component.__components__/0`, never from a
  hand-written list. Adding a component to `FavnView.UI` or
  `FavnView.Components` therefore adds it to the catalogue, and renaming or
  removing an attr changes the catalogue in the same commit. Nothing here can
  drift out of sync with the code it documents.
  """

  @type group :: :element | :component | :page

  @type t :: %__MODULE__{
          id: String.t(),
          module: module(),
          function: atom(),
          group: group(),
          doc: String.t() | nil,
          attrs: [map()],
          slots: [map()]
        }

  @enforce_keys [:id, :module, :function, :group]
  defstruct [:id, :module, :function, :group, :doc, attrs: [], slots: []]

  @doc """
  The declared attrs that must be supplied for the component to render.

  `:global` attrs are excluded: they are always optional.
  """
  @spec required_attrs(t()) :: [atom()]
  def required_attrs(%__MODULE__{attrs: attrs}) do
    for %{name: name, required: true, type: type} <- attrs, type != :global, do: name
  end

  @doc """
  The declared slots that must be supplied for the component to render.
  """
  @spec required_slots(t()) :: [atom()]
  def required_slots(%__MODULE__{slots: slots}) do
    for %{name: name, required: true} <- slots, do: name
  end

  @doc """
  The assigns the component declares a default for, as a map.

  Phoenix applies `attr` defaults at the call site, so a component invoked
  dynamically never receives them. The design system renders dynamically, so it
  reads the defaults out of the component's own metadata and applies them
  itself. Without this an example would have to restate every optional attr,
  which is exactly the kind of duplication that goes stale.
  """
  @spec declared_defaults(t()) :: map()
  def declared_defaults(%__MODULE__{attrs: attrs}) do
    for %{name: name, opts: opts} <- attrs,
        Keyword.has_key?(opts, :default),
        into: %{},
        do: {name, Keyword.fetch!(opts, :default)}
  end

  @doc """
  The attrs that must be supplied because they have no declared default.

  An attr that is neither `required: true` nor given a `default:` is absent from
  the assigns map entirely, so a component that reads it raises. At a call site
  the compiler catches that; invoked dynamically, nothing does. The design system
  therefore fills these too, which is why it can render a component its author
  never invoked dynamically.
  """
  @spec attrs_without_defaults(t()) :: [atom()]
  def attrs_without_defaults(%__MODULE__{attrs: attrs}) do
    for %{name: name, type: type, opts: opts} <- attrs,
        type != :global,
        not Keyword.has_key?(opts, :default),
        do: name
  end

  @doc """
  True when the component declares an attr by this name.
  """
  @spec attr?(t(), atom()) :: boolean()
  def attr?(%__MODULE__{attrs: attrs}, name), do: Enum.any?(attrs, &(&1.name == name))

  @doc """
  The `values` an attr is allowed to take, or `nil` when it is unconstrained.

  This is what makes a matrix render possible without declaring the axis twice.
  """
  @spec attr_values(t(), atom()) :: [term()] | nil
  def attr_values(%__MODULE__{attrs: attrs}, name) do
    Enum.find_value(attrs, fn
      %{name: ^name, opts: opts} -> Keyword.get(opts, :values)
      _other -> nil
    end)
  end

  @doc """
  The attrs that can be used as a matrix axis: those with declared `values`.
  """
  @spec matrix_axes(t()) :: [atom()]
  def matrix_axes(%__MODULE__{attrs: attrs}) do
    for %{name: name, opts: opts} <- attrs, Keyword.has_key?(opts, :values), do: name
  end

  @doc """
  `Module.function/1`, for display.
  """
  @spec label(t()) :: String.t()
  def label(%__MODULE__{module: module, function: function}) do
    "#{inspect(module)}.#{function}/1"
  end
end
