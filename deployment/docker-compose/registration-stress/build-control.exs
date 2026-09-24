Code.require_file("support.exs", __DIR__)
alias RegistrationStress.Support, as: S
{opts, []} = S.options(System.argv(), [revision: :string], revision: "HEAD")

if opts[:help] do
  IO.puts("elixir build-control.exs [--revision HEAD]")
else
  revision = S.revision(opts.revision)

  context =
    Path.join(S.root(), ".favn/registration-stress/control-#{String.slice(revision, 0, 12)}")

  S.archive(revision, context)
  dockerfile = Path.join(context, "rel/control_plane/Dockerfile")

  File.write!(
    dockerfile,
    String.replace(
      File.read!(dockerfile),
      "ENV MIX_ENV=prod",
      ~s(ENV MIX_ENV=prod ERL_FLAGS="+JMsingle true"), global: false)
  )

  recipe_hash = S.hash(File.read!(dockerfile))
  image = "favn-763-control:#{String.slice(revision, 0, 12)}-#{String.slice(recipe_hash, 0, 8)}"
  {metadata, 0} = System.cmd("bash", ["scripts/release_metadata.sh"], cd: context)

  args =
    metadata
    |> String.split("\n", trim: true)
    |> Enum.map(&String.split(&1, "=", parts: 2))
    |> Map.new(fn [k, v] -> {k, v} end)

  args =
    Map.merge(args, %{
      "FAVN_SOURCE_REVISION" => revision,
      "FAVN_BUILD_TIMESTAMP" =>
        S.command(["git", "show", "-s", "--format=%cI", revision]) |> String.trim()
    })

  command =
    [
      "docker",
      "--context",
      "orbstack",
      "buildx",
      "build",
      "--builder",
      "favn-qualification-v1",
      "--platform",
      "linux/amd64",
      "--provenance=false",
      "--load",
      "--tag",
      image,
      "--file",
      dockerfile
    ] ++ Enum.flat_map(args, fn {k, v} -> ["--build-arg", k <> "=" <> v] end) ++ [context]

  record = %{
    source_revision: revision,
    dockerfile_sha256: recipe_hash,
    local_build_adjustment: "ERL_FLAGS=+JMsingle true",
    image: image,
    context: context,
    command: command
  }

  evidence = Path.join(context, "local-build.json")
  S.json_file(evidence, record)
  S.emit(record)
  IO.write(S.command(command))

  record =
    Map.put(
      record,
      :image_id,
      S.docker(["image", "inspect", "--format", "{{.Id}}", image]) |> String.trim()
    )

  S.json_file(evidence, record)
  S.emit(record)
end
