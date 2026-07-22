defmodule Favn.CheckTestTagTiers do
  @moduledoc false

  @allowed_apps %{
    acceptance: MapSet.new(["favn_local"]),
    container: MapSet.new(["favn_local"]),
    slow: MapSet.new(["favn", "favn_local", "favn_storage_postgres"]),
    browser: MapSet.new([])
  }

  @external_lifecycle_module_tiers %{
    "apps/favn/test/mix_tasks/env_bootstrap_integration_test.exs" => :slow,
    "apps/favn_local/test/acceptance/local_compose_acceptance_test.exs" => :acceptance,
    "apps/favn_local/test/acceptance/local_compose_execution_acceptance_test.exs" => :acceptance,
    "apps/favn_local/test/acceptance/single_node_production_acceptance_test.exs" => :acceptance,
    "apps/favn_local/test/integration/dev_split_root_regression_test.exs" => :slow,
    "apps/favn_local/test/integration/dev_stack_smoke_test.exs" => :slow
  }

  @tag_regex ~r/@(?:module)?tag\s+:?(acceptance|container|slow|browser)\b|(?:acceptance|container|slow|browser):\s*true/

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
    |> Kernel.++(external_lifecycle_tier_violations(path))
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

  defp external_lifecycle_tier_violations(path) do
    case @external_lifecycle_module_tiers do
      %{^path => tier} ->
        module_tag_regex = Regex.compile!("@moduletag\\s+:#{tier}\\b")

        if Regex.match?(module_tag_regex, File.read!(path)) do
          []
        else
          ["#{path}: external BEAM lifecycle module must have @moduletag :#{tier}"]
        end

      %{} ->
        []
    end
  end
end

Favn.CheckTestTagTiers.run()
