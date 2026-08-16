defmodule FavnOrchestrator.ConfigOwnershipTest do
  use ExUnit.Case, async: true

  @direct_consumer_config_read ~r/Application\.(?:get_env|fetch_env!?|compile_env!?|get_all_env)\(\s*:favn\b/
  @piped_consumer_config_read ~r/:favn\s*\|>\s*Application\.(?:get_env|fetch_env!?|compile_env!?|get_all_env)\(/

  test "recognizes direct and piped consumer configuration reads" do
    assert consumer_config_read?("Application.get_env(:favn, :connections)")
    assert consumer_config_read?("Application.fetch_env!(:favn, :connections)")
    assert consumer_config_read?(":favn |> Application.get_all_env()")

    refute consumer_config_read?("Application.get_env(:favn_orchestrator, :http)")
  end

  test "control-plane and browser code do not reread consumer Favn configuration" do
    repository_root = Path.expand("../../..", __DIR__)

    files =
      [
        Path.join(repository_root, "apps/favn_orchestrator/lib/**/*.ex"),
        Path.join(repository_root, "apps/favn_view/lib/**/*.ex")
      ]
      |> Enum.flat_map(&Path.wildcard/1)

    violations =
      for file <- files,
          source = File.read!(file),
          consumer_config_read?(source) do
        Path.relative_to(file, repository_root)
      end

    assert violations == [], """
    Control-plane and browser code must consume typed manifest, deployment, or
    run configuration instead of rereading the consumer application's :favn
    environment. Violations: #{inspect(violations)}
    """
  end

  defp consumer_config_read?(source) do
    Regex.match?(@direct_consumer_config_read, source) or
      Regex.match?(@piped_consumer_config_read, source)
  end
end
