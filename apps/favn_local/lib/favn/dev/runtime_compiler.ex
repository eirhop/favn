defmodule Favn.Dev.RuntimeCompiler do
  @moduledoc false

  alias Favn.Dev.EnvFile

  @type runtime :: %{required(String.t()) => term()}

  @spec compile_runtime(runtime(), keyword()) :: :ok | {:error, term()}
  def compile_runtime(runtime, opts) when is_map(runtime) and is_list(opts) do
    cond do
      Keyword.get(opts, :skip_runtime_compile, false) ->
        :ok

      Keyword.has_key?(opts, :service_specs_override) ->
        :ok

      true ->
        run_runtime_compile(runtime, opts)
    end
  end

  @spec compile_project(keyword()) :: :ok | {:error, term()}
  def compile_project(opts) when is_list(opts) do
    if Keyword.get(opts, :skip_bootstrap, false) do
      :ok
    else
      Mix.Task.reenable("compile")
      Mix.Task.run("compile", [])
      :ok
    end
  rescue
    error -> {:error, {:compile_failed, error}}
  end

  defp run_runtime_compile(runtime, opts) do
    runtime_root = Map.fetch!(runtime, "materialized_root")
    mix = System.find_executable("mix") || "mix"
    runner = Keyword.get(opts, :runtime_command_runner, &System.cmd/3)

    command_opts = [
      cd: runtime_root,
      env: Map.merge(EnvFile.loaded_env(opts), %{"MIX_ENV" => "dev"}),
      stderr_to_stdout: true
    ]

    case runner.(mix, ["compile"], command_opts) do
      {_output, 0} ->
        :ok

      {output, status} ->
        {:error, {:runtime_compile_failed, :runtime_root, status, String.trim(output)}}
    end
  end
end
