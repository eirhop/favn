[release_root] = System.argv()

paths = Path.wildcard(Path.join(release_root, "releases/*/sys.config"))

path =
  case paths do
    [path] -> path
    _missing_or_ambiguous -> raise "expected exactly one control-plane sys.config"
  end

config =
  case :file.consult(String.to_charlist(path)) do
    {:ok, [config]} when is_list(config) -> config
    _invalid -> raise "invalid control-plane sys.config"
  end

sanitized = Keyword.drop(config, [:esbuild, :tailwind])
encoded = IO.iodata_to_binary(:io_lib.format("~p.~n", [sanitized]))

if String.contains?(encoded, "/build/") do
  raise "control-plane sys.config contains a builder path"
end

File.write!(path, encoded)
