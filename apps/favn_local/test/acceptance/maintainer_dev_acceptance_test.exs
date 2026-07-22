defmodule Favn.Local.MaintainerDevAcceptanceTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Init.Compose, as: ComposeInit

  @moduletag :integration
  @moduletag :container
  @moduletag timeout: 1_200_000

  test "a real consumer maintainer task reaches runner image creation" do
    checkout = Path.expand("../../../..", __DIR__)
    consumer = Favn.Local.CanonicalSampleProject.create!("favn_maintainer_acceptance")
    marker = Path.join(consumer, "runner-image-build-reached")
    wrapper_dir = Path.join(consumer, "bin")
    docker = System.find_executable("docker") || flunk("docker is required")

    File.mkdir_p!(wrapper_dir)
    wrapper = Path.join(wrapper_dir, "docker")

    File.write!(
      wrapper,
      """
      #!/bin/sh
      if [ "$1" = "build" ]; then
        : > "$FAVN_TEST_RUNNER_BUILD_MARKER"
        exit 86
      fi
      exec #{inspect(docker)} "$@"
      """
    )

    File.chmod!(wrapper, 0o755)
    assert {:ok, _scaffold} = ComposeInit.run(root_dir: consumer)

    environment = [
      {"FAVN_CHECKOUT", checkout},
      {"FAVN_TEST_RUNNER_BUILD_MARKER", marker},
      {"MIX_ENV", "dev"},
      {"PATH", wrapper_dir <> ":" <> System.fetch_env!("PATH")}
    ]

    on_exit(fn -> File.rm_rf(consumer) end)

    assert {deps_output, 0} =
             System.cmd("mix", ["deps.get"],
               cd: consumer,
               env: environment,
               stderr_to_stdout: true
             )

    refute deps_output =~ "Dependencies have diverged"

    {output, status} =
      System.cmd("mix", ["favn.maintainer.dev"],
        cd: consumer,
        env: environment,
        stderr_to_stdout: true
      )

    assert status != 0
    assert File.regular?(marker), String.slice(output, -8_000, 8_000)
    refute output =~ "favn_checkout_not_pinned"
    refute output =~ "absolute_path_literal"

    assert [_runner_context] =
             Path.wildcard(Path.join(consumer, ".favn/dist/runner/rr_*"))
  end
end
