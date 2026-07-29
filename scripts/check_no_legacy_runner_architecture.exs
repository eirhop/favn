defmodule Favn.CheckNoLegacyRunnerArchitecture do
  @moduledoc false

  @roots [".github", "apps", "config", "deployment", "docs", "examples", "rel", "security"]

  @removed ~r/Favn\.Contracts\.RunnerClient|RunnerClient\.BeamNode|RunnerClientValidator|RunnerDispatch|RunnerHealth|RunnerDiagnostics|RunnerManifestRegistration|ActiveManifestReconciler|RunnerLogBridge|RunnerReplacement|RunExecutionOwnership|RunnerExecutionIdentity|ExecutionOwnershipCodec|RunnerReleaseCompatibility|RuntimeStarter|FavnRunner\.Server|FavnRunner\.ResultRetention|FavnRunner\.ExecutionLifecycle|FavnRunner\.Shutdown|FavnLocal\.(RunnerMain|RunnerChild|Lifecycle)|\brunner_executions\b|\brunner_execution_id\b|\binflight_execution_ids\b|\brunner_ref\b|\brunner_replacement\b|runner-replacement|runner replacement|\bFAVN_RUNNER_NODE\b/

  def run do
    violations =
      @roots
      |> Enum.flat_map(&Path.wildcard(Path.join(&1, "**/*")))
      |> Enum.filter(&File.regular?/1)
      |> Enum.reject(&historical?/1)
      |> Enum.sort()
      |> Enum.flat_map(&violations/1)

    case violations do
      [] ->
        IO.puts("legacy runner architecture is absent")

      violations ->
        Enum.each(violations, &IO.puts(:stderr, &1))
        System.halt(1)
    end
  end

  defp historical?(path) do
    normalized = String.replace(path, "\\", "/")

    String.starts_with?(normalized, "docs/archive/") or
      normalized in [
        "docs/architecture/elastic-runners.md",
        "docs/architecture/elastic-runners-implementation-log.md",
        "docs/architecture/elastic-runners-legacy-inventory.md"
      ]
  end

  defp violations(path) do
    path
    |> File.stream!()
    |> Stream.with_index(1)
    |> Enum.flat_map(fn {line, number} ->
      if Regex.match?(@removed, line) do
        ["#{path}:#{number}: removed runner architecture symbol: #{String.trim(line)}"]
      else
        []
      end
    end)
  end
end

Favn.CheckNoLegacyRunnerArchitecture.run()
