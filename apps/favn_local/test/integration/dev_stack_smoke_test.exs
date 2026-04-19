defmodule Favn.DevStackSmokeTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  test "local stack lifecycle smoke path" do
    if System.get_env("FAVN_RUN_DEV_SMOKE") != "1" do
      :ok
    else
      root_dir = File.cwd!()

      assert :ok = Favn.Dev.dev(root_dir: root_dir)
      assert %{stack_status: :running} = Favn.Dev.status(root_dir: root_dir)
      assert :ok = Favn.Dev.reload(root_dir: root_dir)
      assert :ok = Favn.Dev.stop(root_dir: root_dir)
      assert %{stack_status: :stopped} = Favn.Dev.status(root_dir: root_dir)
    end
  end
end
