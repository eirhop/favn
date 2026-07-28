defmodule FavnView.DesignSystemIsolationTest do
  @moduledoc """
  The design-system browser is a development tool. It must never be reachable in
  a shipped release.

  Two independent mechanisms keep it out, and this test asserts both:

    1. `apps/favn_view/dev/` is not in `elixirc_paths` for `:prod`, so those
       modules are not compiled into a release at all. Nothing that does not
       exist can be routed to, whatever the configuration says.
    2. The routes live inside the `Application.compile_env(:favn_view, :dev_routes)`
       block, so they are not compiled into the router unless dev routes are on.

  If this test fails, a development-only surface is about to ship. Do not relax
  the assertions; restore the isolation.
  """

  use ExUnit.Case, async: true

  test "dev routes are disabled in this environment" do
    refute Application.get_env(:favn_view, :dev_routes),
           "this test only proves router isolation while dev routes are off"
  end

  test "dev/ is excluded from the release build" do
    assert FavnView.MixProject.elixirc_paths(:prod) == ["lib"]
  end

  test "dev/ is compiled in dev and test so fixtures can be shared" do
    assert "dev" in FavnView.MixProject.elixirc_paths(:dev)
    assert "dev" in FavnView.MixProject.elixirc_paths(:test)
  end

  test "the router exposes no design-system route" do
    paths = Enum.map(FavnView.Router.__routes__(), & &1.path)

    refute Enum.any?(paths, &String.starts_with?(&1, "/design-system")),
           "a /design-system route is compiled into the router: #{inspect(paths)}"
  end

  test "only the router mentions the dev namespace from lib" do
    offenders =
      Path.join([__DIR__, "..", "..", "lib", "**", "*.ex"])
      |> Path.wildcard()
      |> Enum.filter(&String.contains?(File.read!(&1), "FavnView.Dev"))
      |> Enum.map(&Path.basename/1)

    assert offenders == ["router.ex"],
           "lib/ must reference FavnView.Dev only from the dev-routes block: #{inspect(offenders)}"
  end
end
