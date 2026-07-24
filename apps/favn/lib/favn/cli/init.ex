defmodule Favn.CLI.Init do
  @moduledoc false

  alias Favn.CLI.Init.Sample

  @spec run(keyword()) :: {:ok, map()} | {:error, term()}
  def run(opts) when is_list(opts) do
    case Keyword.get(opts, :target) do
      :deployment -> copy_deployment(opts)
      "deployment" -> copy_deployment(opts)
      nil -> Sample.run(opts)
      target -> {:error, {:unsupported_init_target, target}}
    end
  end

  defp copy_deployment(opts) do
    root_dir = opts |> Keyword.get(:root_dir, File.cwd!()) |> Path.expand()
    source = Application.app_dir(:favn, "priv/templates/deployment")
    destination = Path.join(root_dir, "deploy/favn")

    cond do
      not File.regular?(Path.join(root_dir, "mix.exs")) ->
        {:error, {:missing_mix_project, root_dir}}

      File.exists?(destination) ->
        {:error, {:deployment_target_exists, destination}}

      true ->
        with {:ok, files} <- copy_directory(source, destination) do
          {:ok, %{target: :deployment, output: destination, created: files}}
        end
    end
  end

  defp copy_directory(source, destination) do
    with :ok <- File.mkdir_p(destination) do
      source
      |> Path.join("**/*")
      |> Path.wildcard()
      |> Enum.filter(&File.regular?/1)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, copied} ->
        relative = Path.relative_to(path, source)
        target = Path.join(destination, relative)

        with :ok <- File.mkdir_p(Path.dirname(target)),
             {:ok, _bytes} <- File.copy(path, target) do
          {:cont, {:ok, [target | copied]}}
        else
          {:error, reason} -> {:halt, {:error, {:deployment_copy_failed, target, reason}}}
        end
      end)
      |> case do
        {:ok, copied} -> {:ok, Enum.reverse(copied)}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end
