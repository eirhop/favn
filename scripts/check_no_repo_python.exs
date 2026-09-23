{paths, 0} = System.cmd("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"])

paths = String.split(paths, "\0", trim: true)

sources =
  paths
  |> Enum.filter(&String.ends_with?(&1, [".py", ".pyi", ".pyw"]))
  |> Enum.filter(&File.regular?/1)

if sources != [] do
  IO.puts(:stderr, "Repository-owned Python source is not allowed: #{Enum.join(sources, ", ")}")
  System.halt(1)
end

pattern =
  "(^|[[:space:]\"'/])(py" <>
    "thon([0-9]+(\\.[0-9]+)?)?|pypy([0-9]+)?)([[:space:]\"'/]|$)|worker" <> "\\.py"

pattern = Regex.compile!(pattern)

invocations =
  for path <- paths,
      String.starts_with?(path, ["apps/", "scripts/", ".github/"]),
      Path.basename(path) != "check_no_repo_python.exs",
      Path.extname(path) in [".ex", ".exs", ".sh", ".yml", ".yaml"] or
        Path.basename(path) == "Makefile",
      File.regular?(path),
      {line, number} <- File.stream!(path) |> Stream.with_index(1),
      Regex.match?(pattern, line) do
    "#{path}:#{number}:#{line}"
  end

case invocations do
  [] ->
    IO.puts("No repository Python sources or active invocations")

  matches ->
    IO.puts(:stderr, matches)
    System.halt(1)
end
