defmodule Mix.Tasks.Favn.Init do
  use Mix.Task

  @shortdoc "Scaffolds a Favn sample or deployment template"

  @moduledoc """
  Scaffolds either a local DuckDB sample or a consumer-owned Compose template.

      mix favn.init --duckdb --sample
      mix favn.init --target compose
      mix favn.init --target compose --profile single-host
      mix favn.init --target compose --output deploy/compose.team.yml
      mix favn.init --target runner
  """

  alias Favn.Dev

  @switches [
    duckdb: :boolean,
    sample: :boolean,
    target: :string,
    profile: :string,
    output: :string
  ]
  @usage """
  usage:
    mix favn.init --duckdb --sample
    mix favn.init --target compose [--profile local|single-host] [--output PATH.yml]
    mix favn.init --target runner [--output DIRECTORY]
  """

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest, valid_mode?(opts)} do
      {[], [], true} -> init(normalize_opts(opts))
      {_invalid, _rest, _valid} -> Mix.raise(String.trim(@usage))
    end
  end

  defp init(opts) do
    case Dev.init(opts) do
      {:ok, result} ->
        print_result(result)

      {:error, {:missing_required_flags, flags}} ->
        Mix.raise("missing required option(s): #{Enum.map_join(flags, ", ", &"--#{&1}")}")

      {:error, {:missing_mix_project, root_dir}} ->
        Mix.raise("init failed: no mix.exs found under #{root_dir}")

      {:error, {:compose_scaffold_modified, path}} ->
        Mix.raise(
          "init failed: #{path} contains consumer changes; choose a different --output path to generate a comparison copy"
        )

      {:error, {:runner_scaffold_modified, path}} ->
        Mix.raise(
          "init failed: #{path} contains consumer changes; choose a different --output directory to generate a comparison copy"
        )

      {:error, {:unsupported_compose_profile, profile}} ->
        Mix.raise(
          "init failed: unsupported Compose profile #{inspect(profile)}\n#{String.trim(@usage)}"
        )

      {:error, {:unsafe_compose_output, path}} ->
        Mix.raise(
          "init failed: Compose output must be a .yml or .yaml file inside the project: #{inspect(path)}"
        )

      {:error, {:unsafe_runner_output, path}} ->
        Mix.raise(
          "init failed: runner output must be a simple directory path inside the project: #{inspect(path)}"
        )

      {:error, reason} ->
        Mix.raise("init failed: #{inspect(reason)}")
    end
  end

  defp print_result(result) do
    cond do
      Map.has_key?(result, :pipeline_module) ->
        IO.puts("Favn local bootstrap complete")
        IO.puts("pipeline: #{result.pipeline_module}")

        print_paths("created", result.created)
        print_paths("already present", result.existing)
        print_paths("updated", result.updated)
        print_paths("left unchanged", result.skipped)

        Enum.each(result.warnings, &IO.puts("warning: #{&1}"))

      result[:target] == :runner ->
        IO.puts("Favn runner template ready")
        print_paths("created", result.created)
        print_paths("already present", result.existing)
        IO.puts("runner template: #{result.output}")

      true ->
        IO.puts("Favn Compose template ready")
        IO.puts("profile: #{profile_name(result.profile)}")
        print_paths("created", result.created)
        print_paths("already present", result.existing)
        IO.puts("deployment: #{result.output}")
        IO.puts("environment reference: #{result.env_example}")
    end
  end

  defp valid_mode?(opts) do
    target = Keyword.get(opts, :target)
    sample? = Keyword.get(opts, :duckdb, false) or Keyword.get(opts, :sample, false)
    deployment_options? = Keyword.has_key?(opts, :profile) or Keyword.has_key?(opts, :output)
    profile? = Keyword.has_key?(opts, :profile)

    cond do
      target == "compose" ->
        not sample?

      target == "runner" ->
        not sample? and not profile?

      target == nil ->
        not deployment_options?

      true ->
        false
    end
  end

  defp normalize_opts(opts) do
    opts
    |> normalize_target()
    |> normalize_profile()
  end

  defp normalize_target(opts) do
    if Keyword.has_key?(opts, :target) do
      Keyword.update!(opts, :target, fn
        "compose" -> :compose
        "runner" -> :runner
        value -> value
      end)
    else
      opts
    end
  end

  defp normalize_profile(opts) do
    if Keyword.has_key?(opts, :profile) do
      Keyword.update!(opts, :profile, fn
        "local" -> :local
        "single-host" -> :single_host
        value -> value
      end)
    else
      opts
    end
  end

  defp profile_name(:single_host), do: "single-host"
  defp profile_name(profile), do: Atom.to_string(profile)

  defp print_paths(_label, []), do: :ok

  defp print_paths(label, paths) do
    IO.puts("#{label}:")
    Enum.each(paths, &IO.puts("  - #{&1}"))
  end
end
