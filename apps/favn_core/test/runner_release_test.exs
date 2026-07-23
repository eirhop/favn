defmodule Favn.RunnerReleaseTest do
  use ExUnit.Case, async: true

  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  test "accepts an explicit immutable operator-owned identity" do
    attrs = release_attrs()

    assert {:ok, release} = RunnerRelease.new(attrs)
    assert release.runner_release_id == attrs.runner_release_id
    assert {:ok, ^release} = RunnerRelease.verify(release)
  end

  test "rejects missing and malformed IDs" do
    assert {:error, {:missing_runner_release_field, :runner_release_id}} =
             release_attrs() |> Map.delete(:runner_release_id) |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}} =
             release_attrs() |> Map.put(:runner_release_id, "latest") |> RunnerRelease.new()
  end

  test "rejects incompatible target and runner contract" do
    assert {:error, {:invalid_runner_release_field, :target, :unsupported_value}} =
             release_attrs() |> Map.put(:target, "linux/arm64") |> RunnerRelease.new()

    assert {:error, {:invalid_runner_release_field, :runner_contract_version, :unsupported_value}} =
             release_attrs()
             |> Map.put(
               :runner_contract_version,
               Compatibility.current_runner_contract_version() + 1
             )
             |> RunnerRelease.new()
  end

  defp release_attrs do
    %{
      favn_version: RunnerRelease.current_favn_version(),
      runner_contract_version: Compatibility.current_runner_contract_version(),
      elixir_version: System.version(),
      otp_release: to_string(:erlang.system_info(:otp_release)),
      target: RunnerRelease.current_target(),
      runner_release_id: FavnTestSupport.runner_release_id(),
      build_profile: "prod"
    }
  end
end
