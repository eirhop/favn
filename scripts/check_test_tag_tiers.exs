defmodule Favn.CheckTestTagTiers do
  @moduledoc false

  @allowed_apps %{
    acceptance: MapSet.new(["favn_local"]),
    slow: MapSet.new(["favn", "favn_local"]),
    browser: MapSet.new([])
  }

  @tag_regex ~r/@(?:module)?tag\s+:?(acceptance|slow|browser)\b|(?:acceptance|slow|browser):\s*true/

  def run do
    violations =
      "apps/*/test/**/*.{exs,ex}"
      |> Path.wildcard()
      |> Enum.flat_map(&violations_for_file/1)

    case violations do
      [] ->
        IO.puts("test tag tiers are covered by CI")

      violations ->
        Enum.each(violations, &IO.puts(:stderr, &1))
        System.halt(1)
    end
  end

  defp violations_for_file(path) do
    tags = tags_in_file(path)
    app = app_name(path)

    tags
    |> Enum.flat_map(fn tag ->
      if MapSet.member?(Map.fetch!(@allowed_apps, tag), app) do
        []
      else
        [
          "#{path}: :#{tag} tests are not covered by the current CI tag slice for app #{inspect(app)}"
        ]
      end
    end)
    |> Kernel.++(disjoint_tag_violations(path, tags))
  end

  defp tags_in_file(path) do
    path
    |> File.read!()
    |> then(&Regex.scan(@tag_regex, &1))
    |> Enum.map(fn
      [_match, tag] when tag != "" -> String.to_existing_atom(tag)
      [match, ""] -> match |> String.split(":", parts: 2) |> hd() |> String.to_existing_atom()
    end)
    |> Enum.uniq()
  end

  defp app_name(path) do
    path
    |> Path.split()
    |> Enum.at(1)
  end

  defp disjoint_tag_violations(path, tags) do
    if :acceptance in tags and :slow in tags do
      ["#{path}: :acceptance and :slow must be disjoint because they run in separate CI jobs"]
    else
      []
    end
  end
end

Favn.CheckTestTagTiers.run()
