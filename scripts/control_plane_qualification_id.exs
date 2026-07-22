Code.require_file("control_plane_qualification.ex", __DIR__)

{opts, arguments, invalid} =
  OptionParser.parse(System.argv(),
    strict: [
      root: :string,
      control_plane_build_id: :string,
      metadata: :boolean,
      classify_paths: :boolean
    ]
  )

cond do
  invalid != [] or arguments != [] ->
    raise "usage: elixir scripts/control_plane_qualification_id.exs [--root PATH --control-plane-build-id ID --metadata | --classify-paths]"

  opts[:classify_paths] ->
    paths = IO.read(:stdio, :eof) |> String.split("\n", trim: true)
    %{unknown_runtime_paths: unknown} = Favn.ControlPlaneQualification.classify_paths(paths)
    IO.puts("unknown_runtime_changed=#{unknown != []}")
    IO.puts("unknown_runtime_paths=#{Enum.join(unknown, ",")}")

  opts[:metadata] and is_binary(opts[:control_plane_build_id]) ->
    root_dir = opts[:root] || Path.expand("..", __DIR__)

    case Favn.ControlPlaneQualification.identities(root_dir, opts[:control_plane_build_id]) do
      {:ok, identities} ->
        IO.puts("runtime_qualification_id=#{identities.runtime_qualification_id}")
        IO.puts("security_scan_id=#{identities.security_scan_id}")

      {:error, reason} ->
        raise "control-plane qualification input discovery failed: #{inspect(reason)}"
    end

  true ->
    raise "usage: elixir scripts/control_plane_qualification_id.exs [--root PATH --control-plane-build-id ID --metadata | --classify-paths]"
end
