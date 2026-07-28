defmodule FavnView.Dev.DesignSystem.Catalogue do
  @moduledoc """
  Discovers every Favn function component by reflection.

  The catalogue is built from `:application.get_key(:favn_view, :modules)`,
  keeping only modules that export `__components__/0`. That means:

    * a new component appears here the moment it compiles, with no registration
      step and no story file to forget;
    * a component with no example is still listed, and reported as a gap, so
      missing coverage is visible instead of invisible;
    * attr names, types, defaults, `values`, and docs come from the component
      itself, so the catalogue cannot describe a contract the code does not have.

  Groups follow the three-layer component system described in `FavnView.UI`:

  | Group | Modules | Layer |
  | --- | --- | --- |
  | `:element` | `FavnView.UI.*` | owns borders, colour, radius, spacing |
  | `:component` | `FavnView.Components.*` | composes elements into sections |
  | `:page` | `FavnView.Components.*Page*` | one screen, driven by a view model |
  """

  alias FavnView.Dev.DesignSystem.Entry

  @element_prefix "Elixir.FavnView.UI."
  @component_prefix "Elixir.FavnView.Components."

  @doc """
  Every entry, sorted by group and then by id.
  """
  @spec entries() :: [Entry.t()]
  def entries do
    component_modules()
    |> Enum.flat_map(&module_entries/1)
    |> disambiguate()
    |> Enum.sort_by(&{group_order(&1.group), &1.id})
  end

  @doc """
  Looks an entry up by id.
  """
  @spec fetch(String.t()) :: {:ok, Entry.t()} | :error
  def fetch(id) do
    case Enum.find(entries(), &(&1.id == id)) do
      nil -> :error
      entry -> {:ok, entry}
    end
  end

  @doc """
  The groups in display order.
  """
  @spec groups() :: [Entry.group()]
  def groups, do: [:element, :component, :page]

  @doc """
  Filters entries.

  ## Options

    * `:ids` — keep only these entry ids, in the order given
    * `:group` — keep only this group
    * `:query` — keep entries whose id or label contains this string

  Filters combine. An unknown id is dropped here and reported by the caller;
  silently rendering nothing for a typo would look identical to a component
  that renders nothing.
  """
  @spec filter([Entry.t()], keyword()) :: [Entry.t()]
  def filter(entries, opts) do
    entries
    |> filter_group(Keyword.get(opts, :group))
    |> filter_query(Keyword.get(opts, :query))
    |> filter_ids(Keyword.get(opts, :ids))
  end

  defp filter_group(entries, nil), do: entries
  defp filter_group(entries, group), do: Enum.filter(entries, &(&1.group == group))

  defp filter_query(entries, nil), do: entries
  defp filter_query(entries, ""), do: entries

  defp filter_query(entries, query) do
    needle = String.downcase(query)

    Enum.filter(entries, fn entry ->
      String.contains?(String.downcase(entry.id), needle) or
        String.contains?(String.downcase(Entry.label(entry)), needle)
    end)
  end

  defp filter_ids(entries, nil), do: entries
  defp filter_ids(entries, []), do: entries

  defp filter_ids(entries, ids) do
    by_id = Map.new(entries, &{&1.id, &1})
    Enum.flat_map(ids, fn id -> List.wrap(Map.get(by_id, id)) end)
  end

  defp component_modules do
    (loaded_modules() ++ available_modules())
    |> Enum.uniq()
    |> Enum.filter(&component_module?/1)
    |> Enum.sort()
  end

  defp loaded_modules do
    case :application.get_key(:favn_view, :modules) do
      {:ok, modules} -> modules
      :undefined -> []
    end
  end

  # `:application.get_key/2` reads the app spec as it was loaded at boot, so a
  # component written during a development session is invisible until the server
  # restarts — which breaks the promise above that a component appears as soon as
  # it compiles. Scanning the code path finds the beam files that exist *now*.
  # The prefix filter runs on the charlist so this creates no atoms for the
  # hundreds of modules belonging to dependencies.
  defp available_modules do
    :code.all_available()
    |> Enum.filter(fn {name, _path, _loaded} -> favn_component_name?(name) end)
    |> Enum.map(fn {name, _path, _loaded} -> List.to_atom(name) end)
  end

  defp favn_component_name?(name) do
    string = List.to_string(name)

    String.starts_with?(string, @element_prefix) or
      String.starts_with?(string, @component_prefix)
  end

  defp component_module?(module) do
    name = Atom.to_string(module)

    (String.starts_with?(name, @element_prefix) or String.starts_with?(name, @component_prefix)) and
      Code.ensure_loaded?(module) and
      function_exported?(module, :__components__, 0)
  end

  defp module_entries(module) do
    docs = docs(module)
    group = group(module)

    module.__components__()
    # A private function component is declared with `attr` but cannot be called
    # from outside its module, so it is part of that module's implementation
    # rather than part of the design system.
    |> Enum.filter(fn {_function, meta} -> Map.get(meta, :kind) == :def end)
    |> Enum.map(fn {function, meta} ->
      %Entry{
        id: short_id(module, function),
        module: module,
        function: function,
        group: group,
        doc: Map.get(docs, function),
        attrs: Enum.sort_by(Map.get(meta, :attrs, []), & &1.line),
        slots: Enum.sort_by(Map.get(meta, :slots, []), & &1.line)
      }
    end)
  end

  defp group(module) do
    segments = Module.split(module)

    cond do
      String.starts_with?(Atom.to_string(module), @element_prefix) -> :element
      Enum.any?(segments, &page_segment?/1) -> :page
      true -> :component
    end
  end

  defp page_segment?(segment) do
    String.ends_with?(segment, "Page") or String.ends_with?(segment, "Pages")
  end

  defp group_order(:element), do: 0
  defp group_order(:component), do: 1
  defp group_order(:page), do: 2

  defp short_id(module, function) do
    module |> Module.split() |> List.last() |> Macro.underscore() |> Kernel.<>("/#{function}")
  end

  # Two modules can end in the same segment. When that happens the short id is
  # ambiguous, so both entries fall back to including the parent segment. Only
  # the colliding ids change, so stable ids stay short.
  defp disambiguate(entries) do
    counts = Enum.frequencies_by(entries, & &1.id)

    Enum.map(entries, fn entry ->
      if Map.fetch!(counts, entry.id) > 1 do
        %{entry | id: qualified_id(entry)}
      else
        entry
      end
    end)
  end

  defp qualified_id(%Entry{module: module, function: function}) do
    module
    |> Module.split()
    |> Enum.take(-2)
    |> Enum.map_join("/", &Macro.underscore/1)
    |> Kernel.<>("/#{function}")
  end

  defp docs(module) do
    case Code.fetch_docs(module) do
      {:docs_v1, _anno, _lang, _format, _module_doc, _meta, docs} ->
        for {{:function, name, 1}, _anno, _sig, %{"en" => doc}, _meta} <- docs,
            into: %{},
            do: {name, doc}

      _other ->
        %{}
    end
  end
end
