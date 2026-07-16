ExUnit.start()

Code.require_file("umbrella_test_runner.ex", __DIR__)

defmodule Favn.UmbrellaTestRunnerTest do
  use ExUnit.Case, async: true

  alias Favn.UmbrellaTestRunner

  test "forwards ExUnit arguments to every child app" do
    parent = self()
    args = ["--timeout", "1200000", "--seed", "1234"]

    assert 0 =
             UmbrellaTestRunner.run(
               args,
               fn app, child_args ->
                 send(parent, {:child, app, child_args})
                 0
               end,
               fn _message -> :ok end
             )

    expected_args = [
      "--exclude",
      "acceptance",
      "--exclude",
      "slow",
      "--exclude",
      "browser" | args
    ]

    Enum.each(UmbrellaTestRunner.apps(), fn app ->
      assert_receive {:child, ^app, ^expected_args}
    end)
  end

  test "runs every app and reports aggregate failure" do
    parent = self()

    assert 1 =
             UmbrellaTestRunner.run(
               [],
               fn app, _args ->
                 send(parent, {:ran, app})
                 if app in [:favn, :favn_local], do: 2, else: 0
               end,
               fn message -> send(parent, {:output, message}) end
             )

    Enum.each(UmbrellaTestRunner.apps(), fn app ->
      assert_receive {:ran, ^app}
    end)

    assert_receive {:output, "\nUmbrella fast-test failures: favn=2, favn_local=2"}
  end
end
