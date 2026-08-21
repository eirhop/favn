defmodule FavnLocal.DistributionTest do
  use ExUnit.Case, async: true

  alias FavnLocal.Distribution

  test "uses a fully qualified local host alias" do
    assert Distribution.local_host_alias() == "favn-local.test"
  end

  test "writes the child BEAM resolver before it starts", %{test: test} do
    root_dir =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "#{test}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf(root_dir) end)

    assert {:ok, path} = Distribution.write_runner_resolver(root_dir)
    assert path == Path.join([root_dir, ".favn", "local", "inetrc"])

    assert File.read!(path) == """
           {lookup, [file, native]}.
           {host, {127, 0, 0, 1}, ["favn-local.test"]}.
           """
  end
end
