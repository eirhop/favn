defmodule FavnRunner.ReleaseVerifierTest do
  use ExUnit.Case, async: false

  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease
  alias FavnRunner.ReleaseVerifier

  setup do
    on_exit(fn ->
      :ok =
        ReleaseVerifier.verify_test_startup(%{
          "FAVN_RUNNER_RELEASE_ID" => FavnTestSupport.runner_release_id()
        })
    end)

    :ok
  end

  test "installs bounded operational identity from source configuration" do
    release_id = FavnTestSupport.runner_release_id(:alternate)

    assert :ok =
             ReleaseVerifier.verify_test_startup(%{
               "FAVN_RUNNER_RELEASE_ID" => release_id
             })

    assert {:ok, info} = FavnRunner.release_info()

    assert {:ok, source_target} =
             ReleaseVerifier.runtime_target(
               "source",
               :os.type(),
               :erlang.system_info(:system_architecture)
             )

    assert info == %{
             runner_release_id: release_id,
             favn_version: RunnerRelease.current_favn_version(),
             runner_contract_version: Compatibility.current_runner_contract_version(),
             elixir_version: System.version(),
             otp_release: to_string(:erlang.system_info(:otp_release)),
             target: source_target,
             build_profile: "source",
             identity_source: :source
           }
  end

  test "fails closed when the operator omits or malforms the release ID" do
    assert {:error, :runner_release_id_missing} =
             ReleaseVerifier.verify_test_startup(%{})

    assert {:error, {:invalid_runner_release_id, "latest"}} =
             ReleaseVerifier.verify_test_startup(%{
               "FAVN_RUNNER_RELEASE_ID" => "latest"
             })

    assert {:error, {:invalid_runner_build_profile, "preview"}} =
             ReleaseVerifier.verify_test_startup(%{
               "FAVN_RUNNER_RELEASE_ID" => FavnTestSupport.runner_release_id(),
               "FAVN_RUNNER_BUILD_PROFILE" => "preview"
             })
  end

  test "derives the supported target from the actual runtime architecture" do
    assert {:ok, "linux/amd64"} =
             ReleaseVerifier.runtime_target({:unix, :linux}, "x86_64-pc-linux-gnu")

    assert {:error, {:unsupported_runner_target, {:unix, :linux}, "aarch64-linux-gnu"}} =
             ReleaseVerifier.runtime_target({:unix, :linux}, "aarch64-linux-gnu")

    assert {:error, {:unsupported_runner_target, {:win32, :nt}, "x86_64-pc-windows"}} =
             ReleaseVerifier.runtime_target({:win32, :nt}, "x86_64-pc-windows")

    assert {:ok, "windows/amd64"} =
             ReleaseVerifier.runtime_target(
               "source",
               {:win32, :nt},
               "x86_64-pc-windows"
             )

    assert {:ok, "linux/amd64"} =
             ReleaseVerifier.runtime_target(
               "source",
               {:unix, :linux},
               "x86_64-pc-linux-gnu"
             )
  end

  test "checks manifest requirements against the configured release" do
    release_id = FavnTestSupport.runner_release_id()
    alternate = FavnTestSupport.runner_release_id(:alternate)

    assert :ok =
             ReleaseVerifier.verify_test_startup(%{
               "FAVN_RUNNER_RELEASE_ID" => release_id
             })

    assert :ok = ReleaseVerifier.verify_required_release(release_id)

    assert {:error,
            %Favn.Contracts.RunnerError{
              type: :runner_release_mismatch,
              details: %{
                required_runner_release_id: ^alternate,
                runner_release_id: ^release_id
              }
            }} = ReleaseVerifier.verify_required_release(alternate)
  end
end
