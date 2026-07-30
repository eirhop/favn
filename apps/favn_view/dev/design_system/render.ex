defmodule FavnView.Dev.DesignSystem.Render do
  @moduledoc """
  Turns an entry plus an example into HTML, without ever raising.

  Two things happen here that a story-based viewer does not do.

  **Declared defaults are applied.** Phoenix resolves `attr ... default:` at the
  call site, so a component invoked dynamically receives none of them. This
  module reads the defaults out of `Phoenix.Component.__components__/0` and
  merges them under the example's own attrs, so an attrs example only states
  what it wants to demonstrate. Declared slots default to `[]` and `:global`
  attrs to `%{}` for the same reason.

  **A failing example is contained.** The render is forced to iodata inside a
  `try`, so a component that raises produces a visible error card in place of
  itself and every other example on the page still renders. A viewer that
  answers a bad example with a 500 hides the other nineteen answers.
  """

  alias FavnView.Dev.DesignSystem.Entry
  alias FavnView.Dev.DesignSystem.Example

  @doc """
  The full assigns map a dynamic invocation needs.
  """
  @spec assigns_for(Entry.t(), map()) :: map()
  def assigns_for(%Entry{} = entry, attrs) when is_map(attrs) do
    entry
    |> Entry.declared_defaults()
    |> Map.merge(global_defaults(entry))
    |> Map.merge(slot_defaults(entry))
    |> Map.merge(attrs)
    |> Map.put(:__changed__, nil)
  end

  @doc """
  Renders an example to safe HTML.

  Returns `{:ok, safe}` or `{:error, message}`. The error message is the
  exception message plus the first few stacktrace entries, which is what
  actually identifies the wrong assign.
  """
  @spec to_safe(Entry.t(), Example.t()) :: {:ok, Phoenix.HTML.safe()} | {:error, String.t()}
  def to_safe(%Entry{} = entry, %Example{} = example) do
    {:ok, {:safe, force(entry, example)}}
  rescue
    error -> {:error, describe(error, __STACKTRACE__)}
  catch
    kind, reason -> {:error, Exception.format(kind, reason, __STACKTRACE__)}
  end

  @doc """
  A slot value that renders the given content, for a required `inner_block`.
  """
  @spec inner_block(iodata()) :: [map()]
  def inner_block(content) do
    [%{__slot__: :inner_block, inner_block: fn _changed, _arg -> content end}]
  end

  defp force(_entry, %Example{render: render}) when is_function(render, 1) do
    render.(%{__changed__: nil}) |> Phoenix.HTML.Safe.to_iodata()
  end

  defp force(%Entry{module: module, function: function} = entry, %Example{attrs: attrs}) do
    module
    |> apply(function, [assigns_for(entry, attrs || %{})])
    |> Phoenix.HTML.Safe.to_iodata()
  end

  defp global_defaults(%Entry{attrs: attrs}) do
    for %{name: name, type: :global} <- attrs, into: %{}, do: {name, %{}}
  end

  defp slot_defaults(%Entry{slots: slots}) do
    for %{name: name} <- slots, into: %{}, do: {name, []}
  end

  defp describe(error, stacktrace) do
    location =
      stacktrace
      |> Enum.take(3)
      |> Enum.map_join("\n", &("  " <> Exception.format_stacktrace_entry(&1)))

    Exception.message(error) <> "\n" <> location
  end
end
