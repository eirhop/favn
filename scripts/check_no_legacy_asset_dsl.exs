defmodule Favn.CheckNoLegacyAssetDSL do
  @moduledoc false

  @source_globs [
    "apps/*/lib/**/*.{ex,exs}",
    "apps/favn_test_support/priv/fixtures/**/*.{ex,exs}",
    "examples/*/lib/**/*.{ex,exs}"
  ]

  @documentation_globs [
    "README.md",
    "apps/*/README.md",
    "apps/favn/guides/**/*.md",
    "docs/**/*.md",
    "examples/*/README.md"
  ]

  @legacy_attribute ~r/^\s*@(asset|config|custom|defaults|depends|description|execution_pool|extra|freshness|materialized|meta|relation|resources|rest|retry|runtime_config|runtime_inputs|settings|title|window)\b/
  @legacy_use ~r/\buse\s+Favn\.Assets\b/
  @legacy_documentation_attribute ~r/`@(asset|config|custom|defaults|depends|description|execution_pool|extra|freshness|materialized|meta|resources|rest|retry|runtime_config|runtime_inputs|settings|title|window)\b/

  def run do
    violations =
      @source_globs
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.flat_map(&violations_for_file/1)
      |> Kernel.++(documentation_violations())
      |> Kernel.++(removed_module_violations())

    case violations do
      [] ->
        IO.puts("legacy asset DSL forms are absent")

      violations ->
        Enum.each(violations, &IO.puts(:stderr, &1))
        System.halt(1)
    end
  end

  defp documentation_violations do
    @documentation_globs
    |> Enum.flat_map(&Path.wildcard/1)
    |> Enum.reject(&historical_document?/1)
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(&documentation_violations_for_file/1)
  end

  defp documentation_violations_for_file(path) do
    path
    |> File.stream!()
    |> Stream.with_index(1)
    |> Enum.flat_map(fn {line, number} ->
      if Regex.match?(@legacy_attribute, line) or
           Regex.match?(@legacy_documentation_attribute, line) or
           Regex.match?(@legacy_use, line) do
        ["#{path}:#{number}: removed asset DSL form: #{String.trim(line)}"]
      else
        []
      end
    end)
  end

  defp historical_document?(path),
    do:
      String.starts_with?(path, "docs/archive/") or
        String.starts_with?(path, "docs/refactor/")

  defp violations_for_file(path) do
    path
    |> File.stream!()
    |> Stream.with_index(1)
    |> Enum.flat_map(fn {line, number} ->
      if Regex.match?(@legacy_attribute, line) or Regex.match?(@legacy_use, line) do
        ["#{path}:#{number}: removed asset DSL form: #{String.trim(line)}"]
      else
        []
      end
    end)
  end

  defp removed_module_violations do
    path = "apps/favn_authoring/lib/favn/assets.ex"
    if File.exists?(path), do: ["#{path}: removed Favn.Assets module still exists"], else: []
  end
end

Favn.CheckNoLegacyAssetDSL.run()
