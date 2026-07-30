defmodule FavnView.Dev.DesignSystem.Generated do
  @moduledoc """
  Examples the design system derives from a component's own metadata.

  Two modes, neither of which is written by hand:

    * `:defaults` — the component with every optional attr at its declared
      default and every required attr filled by
      `FavnView.Dev.DesignSystem.Sample`. This is the baseline: if it looks
      wrong, the component's own defaults are wrong.
    * `:matrix` — one rendering per value of an attr that declares `values:`,
      so the axis comes from the component instead of being restated. A new size
      or variant appears in the matrix on the commit that adds it.

  `:tone` is the one axis that does not come from an attr. Components declare it
  as a plain `:atom` and delegate its meaning to `FavnView.UI.Tokens`, so the
  vocabulary is read from `FavnView.UI.Tokens.tones/0` instead. Adding a tone
  there adds it to every tone matrix. (Declaring `values:` on each component
  would work too, and would then take precedence — but it would restate the
  vocabulary once per component.)

  When an attr cannot be invented the example is still returned, marked
  `unavailable`, listing what is missing. The index shows that as a gap so it
  reads as "needs a curated example" rather than as a component that renders
  nothing.
  """

  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Example
  alias FavnView.Dev.DesignSystem.Render
  alias FavnView.Dev.DesignSystem.Sample
  alias FavnView.UI.Tokens

  @doc """
  The single defaults example for an entry.
  """
  @spec defaults(Entry.t()) :: Example.t()
  def defaults(%Entry{} = entry) do
    {attrs, unavailable} = required_assigns(entry)

    %Example{
      id: "defaults",
      doc: "Declared defaults, with required attrs sampled.",
      attrs: attrs,
      source: :defaults,
      unavailable: unavailable
    }
  end

  @doc """
  One example per declared value of `axis`.

  Returns `[]` when the attr declares no `values`, because there is then no
  axis to walk and inventing one would be guesswork.
  """
  @spec matrix(Entry.t(), atom()) :: [Example.t()]
  def matrix(%Entry{} = entry, axis) do
    case axis_values(entry, axis) do
      nil ->
        []

      values ->
        {attrs, unavailable} = required_assigns(entry)

        for value <- values do
          %Example{
            id: "#{axis}=#{inspect_value(value)}",
            doc: nil,
            attrs: Map.put(attrs, axis, value),
            source: :matrix,
            unavailable: unavailable
          }
        end
    end
  end

  @doc """
  Every axis worth rendering as a matrix for this entry.
  """
  @spec axes(Entry.t()) :: [atom()]
  def axes(%Entry{} = entry) do
    declared = Entry.matrix_axes(entry)

    if Entry.attr?(entry, :tone) and :tone not in declared do
      [:tone | declared]
    else
      declared
    end
  end

  defp axis_values(entry, :tone) do
    Entry.attr_values(entry, :tone) || if(Entry.attr?(entry, :tone), do: Tokens.tones())
  end

  defp axis_values(entry, axis), do: Entry.attr_values(entry, axis)

  defp required_assigns(entry) do
    {attrs, unavailable} = required_attr_assigns(entry)
    {slots, missing_slots} = required_slot_assigns(entry)

    {Map.merge(attrs, slots), unavailable ++ missing_slots}
  end

  defp required_attr_assigns(%Entry{attrs: attrs} = entry) do
    needed = Enum.uniq(Entry.required_attrs(entry) ++ Entry.attrs_without_defaults(entry))

    Enum.reduce(needed, {%{}, []}, fn name, {acc, missing} ->
      attr = Enum.find(attrs, &(&1.name == name))

      case Sample.for_attr(entry, attr) do
        {:ok, value} -> {Map.put(acc, name, value), missing}
        :unavailable -> {acc, [name | missing]}
      end
    end)
  end

  defp required_slot_assigns(%Entry{} = entry) do
    Enum.reduce(Entry.required_slots(entry), {%{}, []}, fn
      :inner_block, {acc, missing} ->
        {Map.put(acc, :inner_block, Render.inner_block(Sample.inner_block())), missing}

      slot, {acc, missing} ->
        {acc, [slot | missing]}
    end)
  end

  defp inspect_value(value) when is_atom(value), do: Atom.to_string(value)
  defp inspect_value(value) when is_binary(value), do: value
  defp inspect_value(value), do: inspect(value)
end
