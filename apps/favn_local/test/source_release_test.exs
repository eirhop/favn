defmodule FavnLocal.SourceReleaseTest do
  use ExUnit.Case, async: true

  alias FavnLocal.SourceRelease

  test "derives a stable release from sorted compiled BEAM contents" do
    build_path = build_path()
    first = beam_path(build_path, "sample_a", "Elixir.SampleA.beam")
    second = beam_path(build_path, "sample_b", "Elixir.SampleB.beam")
    File.mkdir_p!(Path.dirname(first))
    File.mkdir_p!(Path.dirname(second))
    File.write!(first, "first")
    File.write!(second, "second")
    on_exit(fn -> File.rm_rf(build_path) end)

    assert {:ok, release_id} = SourceRelease.current(build_path: build_path)
    assert release_id =~ ~r/^rr_[0-9a-f]{64}$/

    File.touch!(first, 1_000_000)
    assert {:ok, ^release_id} = SourceRelease.current(build_path: build_path)

    File.write!(second, "changed")
    assert {:ok, changed_release_id} = SourceRelease.current(build_path: build_path)
    refute changed_release_id == release_id
  end

  test "fails explicitly when compiled source is unavailable" do
    build_path = build_path()
    on_exit(fn -> File.rm_rf(build_path) end)

    assert {:error, {:compiled_source_unavailable, expanded}} =
             SourceRelease.current(build_path: build_path)

    assert expanded == Path.expand(build_path)
  end

  defp build_path do
    Path.join(
      System.tmp_dir!(),
      "favn_source_release_#{System.unique_integer([:positive, :monotonic])}"
    )
  end

  defp beam_path(build_path, app, filename),
    do: Path.join([build_path, "lib", app, "ebin", filename])
end
