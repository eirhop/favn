defmodule Favn.Dev.LockTest do
  use ExUnit.Case, async: true

  alias Favn.Dev.Lock

  setup do
    root_dir =
      Path.join(System.tmp_dir!(), "favn_dev_lock_test_#{System.unique_integer([:positive])}")

    File.mkdir_p!(root_dir)

    on_exit(fn ->
      File.rm_rf(root_dir)
    end)

    %{root_dir: root_dir}
  end

  test "with_lock/2 runs callback under lock", %{root_dir: root_dir} do
    assert {:ok, :done} =
             Lock.with_lock([root_dir: root_dir], fn ->
               {:ok, :done}
             end)
  end
end
