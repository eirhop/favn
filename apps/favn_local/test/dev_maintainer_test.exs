defmodule Favn.Dev.MaintainerTest do
  use ExUnit.Case, async: false

  alias Favn.Dev.Maintainer
  alias Favn.Dev.Maintainer.{Candidate, Source}

  @revision String.duplicate("a", 40)
  @build_id String.duplicate("b", 64)
  @image_id "sha256:" <> String.duplicate("c", 64)

  setup do
    root =
      Path.join(
        Path.expand("../../../_build/test-artifacts", __DIR__),
        "favn_maintainer_test_#{System.unique_integer([:positive])}"
      )

    checkout = Path.join(root, "favn")
    consumer = Path.join(root, "consumer")
    File.mkdir_p!(consumer)

    for relative <- [
          "mix.exs",
          "apps/favn/mix.exs",
          "apps/favn_local/mix.exs",
          "scripts/control_plane_build_id.exs"
        ] do
      path = Path.join(checkout, relative)
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "fixture\n")
    end

    dependency_paths =
      Map.new(
        %{
          favn: "apps/favn",
          favn_authoring: "apps/favn_authoring",
          favn_core: "apps/favn_core",
          favn_local: "apps/favn_local",
          favn_runner: "apps/favn_runner",
          favn_sql_runtime: "apps/favn_sql_runtime"
        },
        fn {app, relative} -> {app, Path.join(checkout, relative)} end
      )

    Enum.each(dependency_paths, fn {_app, directory} ->
      File.mkdir_p!(directory)
      File.write!(Path.join(directory, "mix.exs"), "fixture\n")
    end)

    for app <- ~w(favn_orchestrator favn_storage_postgres favn_view) do
      directory = Path.join(checkout, "apps/#{app}")
      File.mkdir_p!(Path.join(directory, "lib"))
      File.write!(Path.join(directory, "mix.exs"), "fixture\n")
      File.write!(Path.join(directory, "lib/source.ex"), "#{app}\n")
    end

    on_exit(fn -> File.rm_rf(root) end)

    %{checkout: checkout, consumer: consumer, dependency_paths: dependency_paths}
  end

  test "source requires one checkout for every loaded Favn dependency", context do
    assert {:ok, source} = Source.resolve(source_opts(context))
    assert source.checkout == context.checkout
    assert source.revision == @revision
    assert source.dirty == false
    assert source.fingerprint =~ ~r/\A[0-9a-f]{64}\z/

    mismatched = Map.put(context.dependency_paths, :favn_runner, "/other/favn_runner")

    assert {:error, {:maintainer_dependency_mismatch, :favn_runner, expected, actual}} =
             Source.resolve(source_opts(%{context | dependency_paths: mismatched}))

    assert expected == Path.join(context.checkout, "apps/favn_runner")
    assert actual == "/other/favn_runner"
  end

  test "source reports dirty checkout identity", context do
    command_runner = git_runner(" M apps/favn_view/lib/page.ex\n")

    assert {:ok, %{dirty: true}} =
             Source.resolve(
               source_opts(context)
               |> Keyword.put(:maintainer_command_runner, command_runner)
             )
  end

  test "source rejects an optional Favn dependency from another checkout", context do
    dependency_paths =
      Map.put(context.dependency_paths, :favn_duckdb, "/other/favn/apps/favn_duckdb")

    assert {:error, {:maintainer_dependency_mismatch, :favn_duckdb, expected, actual}} =
             Source.resolve(source_opts(%{context | dependency_paths: dependency_paths}))

    assert expected == Path.join(context.checkout, "apps/favn_duckdb")
    assert actual == "/other/favn/apps/favn_duckdb"
  end

  test "candidate pins the loaded image ID and both source identities", context do
    candidate_path = Path.join(context.checkout, "candidate.json")

    File.write!(
      candidate_path,
      JSON.encode!(%{
        "schema_version" => 1,
        "control_plane_build_id" => @build_id,
        "candidate_tag" => "favn-control-plane-candidate:#{@build_id}",
        "image_id" => @image_id,
        "source_revision" => @revision,
        "source_dirty" => false,
        "target" => "linux/amd64"
      })
    )

    source = %Source{
      checkout: context.checkout,
      revision: @revision,
      dirty: true,
      fingerprint: String.duplicate("e", 64)
    }

    assert {:ok, candidate} =
             Candidate.from_build(
               %{
                 control_plane_build_id: @build_id,
                 image_status: :reused,
                 image_tag: "favn-control-plane-candidate:#{@build_id}",
                 image_id: @image_id,
                 candidate_path: candidate_path
               },
               source
             )

    assert candidate.image_id == @image_id
    assert candidate.image_source_dirty == false
    assert candidate.checkout_dirty == true
  end

  test "maintainer run wires checkout, candidate build, and lifecycle", context do
    candidate_path = Path.join(context.checkout, "candidate.json")

    File.write!(
      candidate_path,
      JSON.encode!(%{
        "schema_version" => 1,
        "control_plane_build_id" => @build_id,
        "candidate_tag" => "favn-control-plane-candidate:#{@build_id}",
        "image_id" => @image_id,
        "source_revision" => @revision,
        "source_dirty" => false,
        "target" => "linux/amd64"
      })
    )

    parent = self()

    build_fun = fn source, _opts ->
      send(parent, {:built_from, source.checkout})

      {:ok,
       %{
         control_plane_build_id: @build_id,
         image_status: :reused,
         image_tag: "favn-control-plane-candidate:#{@build_id}",
         image_id: @image_id,
         candidate_path: candidate_path
       }}
    end

    lifecycle_fun = fn candidate, _opts ->
      send(parent, {:selected, candidate})
      :ok
    end

    opts =
      source_opts(context) ++
        [
          maintainer_build_fun: build_fun,
          maintainer_lifecycle_fun: lifecycle_fun,
          progress_fun: fn _message -> :ok end
        ]

    assert :ok = Maintainer.run(opts)
    assert_receive {:built_from, checkout}
    assert checkout == context.checkout
    assert_receive {:selected, %Candidate{image_id: @image_id, checkout: ^checkout}}
  end

  defp source_opts(context) do
    [
      root_dir: context.consumer,
      maintainer_checkout: context.checkout,
      maintainer_dependency_paths: context.dependency_paths,
      maintainer_command_runner: git_runner("")
    ]
  end

  defp git_runner(status) do
    fn "git", args, _opts ->
      case Enum.drop(args, 2) do
        ["rev-parse", "--verify", "HEAD^{commit}"] -> {@revision <> "\n", 0}
        ["status", "--porcelain", "--untracked-files=all", "--" | _paths] -> {status, 0}
      end
    end
  end

end
