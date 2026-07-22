{root_dir, mode} =
  case System.argv() do
    [] ->
      {Path.expand("..", __DIR__), :id}

    ["--metadata"] ->
      {Path.expand("..", __DIR__), :metadata}

    ["--root", root_dir] ->
      {Path.expand(root_dir), :id}

    ["--root", root_dir, "--metadata"] ->
      {Path.expand(root_dir), :metadata}

    _invalid ->
      raise "usage: elixir scripts/control_plane_build_id.exs [--root PATH] [--metadata]"
  end

Code.require_file(Path.join(root_dir, "apps/favn_core/lib/favn/manifest/contract_versions.ex"))

Code.require_file(Path.join(root_dir, "apps/favn_core/lib/favn/control_plane_build.ex"))
Code.require_file(Path.join(root_dir, "rel/control_plane/release.exs"))

Code.require_file(
  Path.join(root_dir, "apps/favn_local/lib/favn/dev/build/control_plane_inputs.ex")
)

case Favn.Dev.Build.ControlPlaneInputs.collect(root_dir) do
  {:ok, %{descriptor: descriptor, release_version: release_version}} ->
    if mode == :metadata do
      IO.puts("control_plane_build_id=#{descriptor.control_plane_build_id}")
      IO.puts("control_plane_version=#{descriptor.identity["control_plane_version"]}")
      IO.puts("release_version=#{release_version}")
      IO.puts("elixir_version=#{descriptor.identity["elixir_version"]}")
      IO.puts("otp_version=#{descriptor.identity["otp_version"]}")
      IO.puts("manifest_schema_version=#{descriptor.identity["manifest_schema_version"]}")
      IO.puts("runner_contract_version=#{descriptor.identity["runner_contract_version"]}")
      IO.puts("target=#{descriptor.identity["target"]}")
    else
      IO.puts(descriptor.control_plane_build_id)
    end

  {:error, reason} ->
    raise "control-plane input discovery failed: #{inspect(reason)}"
end
