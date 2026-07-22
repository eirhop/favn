defmodule Favn.ControlPlaneBuildTest do
  use ExUnit.Case, async: true

  alias Favn.ControlPlaneBuild

  @digest_a String.duplicate("a", 64)
  @digest_b String.duplicate("b", 64)

  test "identity is independent of input and identity map ordering" do
    inputs = [
      %{path: "config/runtime.exs", sha256: @digest_b, size: 2},
      %{path: "apps/favn_core/lib/a.ex", sha256: @digest_a, size: 1}
    ]

    identity = %{
      "target" => "linux/amd64",
      "applications" => ["favn_view", "favn_core"],
      "otp_release" => "28"
    }

    assert {:ok, first} = ControlPlaneBuild.new(inputs, identity)

    assert {:ok, second} =
             ControlPlaneBuild.new(
               Enum.reverse(inputs),
               Map.new(Enum.reverse(Map.to_list(identity)))
             )

    assert first == second
    assert first.inputs == Enum.sort_by(inputs, & &1.path)
    assert first.identity["applications"] == ["favn_core", "favn_view"]
    assert first.control_plane_build_id =~ ~r/\A[0-9a-f]{64}\z/
  end

  test "every final-byte input and compatibility value changes identity" do
    input = [%{path: "config/runtime.exs", sha256: @digest_a, size: 1}]
    identity = %{"target" => "linux/amd64", "manifest_schema_version" => 10}

    assert {:ok, original} = ControlPlaneBuild.new(input, identity)

    changed_input = [%{path: "config/runtime.exs", sha256: @digest_b, size: 1}]
    changed_identity = Map.put(identity, "manifest_schema_version", 11)

    assert {:ok, input_change} = ControlPlaneBuild.new(changed_input, identity)
    assert {:ok, identity_change} = ControlPlaneBuild.new(input, changed_identity)

    refute original.control_plane_build_id == input_change.control_plane_build_id
    refute original.control_plane_build_id == identity_change.control_plane_build_id
  end

  test "rejects unsafe, malformed, empty, and duplicate records" do
    identity = %{"target" => "linux/amd64"}

    for input <- [
          [],
          [%{path: "../secret", sha256: @digest_a, size: 1}],
          [%{path: "/absolute", sha256: @digest_a, size: 1}],
          [%{path: "a\\b", sha256: @digest_a, size: 1}],
          [%{path: "a", sha256: "invalid", size: 1}],
          [%{path: "a", sha256: @digest_a, size: -1}]
        ] do
      assert {:error, :invalid_control_plane_input} = ControlPlaneBuild.new(input, identity)
    end

    duplicate = [
      %{path: "a", sha256: @digest_a, size: 1},
      %{"path" => "a", "sha256" => @digest_b, "size" => 2}
    ]

    assert {:error, {:duplicate_control_plane_input, "a"}} =
             ControlPlaneBuild.new(duplicate, identity)
  end

  test "rejects empty or non-explicit identity values" do
    input = [%{path: "a", sha256: @digest_a, size: 1}]

    for identity <- [%{}, %{"target" => ""}, %{"target" => nil}, %{target: "linux/amd64"}] do
      assert {:error, :invalid_control_plane_identity} =
               ControlPlaneBuild.new(input, identity)
    end
  end
end
