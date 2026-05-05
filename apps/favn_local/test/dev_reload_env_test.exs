defmodule Favn.Dev.ReloadEnvTest do
  use ExUnit.Case, async: false

  alias Favn.Dev

  setup do
    root_dir =
      Path.join(
        System.tmp_dir!(),
        "favn_dev_reload_env_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root_dir)
    previous_env = System.get_env("FAVN_RELOAD_ENV_TEST")
    System.delete_env("FAVN_RELOAD_ENV_TEST")

    on_exit(fn ->
      File.rm_rf(root_dir)
      restore_env("FAVN_RELOAD_ENV_TEST", previous_env)
    end)

    %{root_dir: root_dir}
  end

  test "reload/1 loads local env file before runtime checks", %{root_dir: root_dir} do
    File.write!(Path.join(root_dir, ".env"), "FAVN_RELOAD_ENV_TEST=loaded\n")

    assert {:error, :stack_not_running} = Dev.reload(root_dir: root_dir)
    assert System.get_env("FAVN_RELOAD_ENV_TEST") == "loaded"
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
