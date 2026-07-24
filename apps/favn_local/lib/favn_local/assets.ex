defmodule FavnLocal.Assets do
  @moduledoc false

  @tailwind_version "4.3.3"
  @esbuild_version "0.28.1"

  @spec build!(Path.t(), keyword()) :: :ok
  def build!(view_root, opts \\ []) when is_binary(view_root) and is_list(opts) do
    task_runner = Keyword.get(opts, :task_runner, &Mix.Task.run/2)
    build_path = Keyword.get(opts, :build_path, Mix.Project.build_path())
    deps_path = Keyword.get(opts, :deps_path, Mix.Project.deps_path())

    configure(view_root, build_path, deps_path)
    task_runner.("tailwind", ["favn_view"])
    task_runner.("esbuild", ["favn_view"])
    :ok
  end

  @doc false
  @spec configure(Path.t(), Path.t(), Path.t()) :: :ok
  def configure(view_root, build_path, deps_path)
      when is_binary(view_root) and is_binary(build_path) and is_binary(deps_path) do
    Application.put_env(:tailwind, :version, @tailwind_version)

    Application.put_env(:tailwind, :favn_view,
      args: ~w(
        --input=assets/css/app.css
        --output=priv/static/assets/css/app.css
      ),
      cd: view_root
    )

    Application.put_env(:esbuild, :version, @esbuild_version)

    Application.put_env(:esbuild, :favn_view,
      args:
        ~w(js/app.js --bundle --target=es2022 --outdir=../priv/static/assets/js --external:/fonts/* --external:/images/* --alias:@=.),
      cd: Path.join(view_root, "assets"),
      env: %{"NODE_PATH" => [deps_path, build_path]}
    )

    :ok
  end
end
