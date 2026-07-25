defmodule FavnLocal.AssetsTest do
  use ExUnit.Case, async: false

  alias FavnLocal.Assets

  setup do
    previous_tailwind = Application.get_all_env(:tailwind)
    previous_esbuild = Application.get_all_env(:esbuild)

    on_exit(fn ->
      restore_env(:tailwind, previous_tailwind)
      restore_env(:esbuild, previous_esbuild)
    end)
  end

  test "builds dependency-owned View assets with consumer build paths" do
    parent = self()
    runner = fn task, args -> send(parent, {:task, task, args}) end

    assert :ok =
             Assets.build!("/tmp/favn/apps/favn_view",
               build_path: "/tmp/consumer/_build/dev",
               deps_path: "/tmp/consumer/deps",
               task_runner: runner
             )

    assert_received {:task, "tailwind", ["favn_view"]}
    assert_received {:task, "esbuild", ["favn_view"]}

    assert [
             args: [
               "--input=assets/css/app.css",
               "--output=priv/static/assets/css/app.css"
             ],
             cd: "/tmp/favn/apps/favn_view"
           ] = Application.fetch_env!(:tailwind, :favn_view)

    assert [
             args: [
               "js/app.js",
               "--bundle",
               "--target=es2022",
               "--outdir=../priv/static/assets/js",
               "--external:/fonts/*",
               "--external:/images/*",
               "--alias:@=."
             ],
             cd: "/tmp/favn/apps/favn_view/assets",
             env: %{
               "NODE_PATH" => ["/tmp/consumer/deps", "/tmp/consumer/_build/dev"]
             }
           ] = Application.fetch_env!(:esbuild, :favn_view)
  end

  defp restore_env(app, values) do
    current_keys = Application.get_all_env(app) |> Keyword.keys()
    previous_keys = Keyword.keys(values)

    Enum.each(current_keys -- previous_keys, &Application.delete_env(app, &1))
    Enum.each(values, fn {key, value} -> Application.put_env(app, key, value) end)
  end
end
