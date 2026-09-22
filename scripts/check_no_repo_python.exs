{paths, 0} = System.cmd("git", ["ls-files", "--cached", "--others", "--exclude-standard"])

sources =
  paths
  |> String.split("\n", trim: true)
  |> Enum.filter(&String.ends_with?(&1, [".py", ".pyi", ".pyw"]))
  |> Enum.filter(&File.regular?/1)

if sources != [] do
  IO.puts(:stderr, "Repository-owned Python source is not allowed: #{Enum.join(sources, ", ")}")
  System.halt(1)
end

pattern =
  "(^|[[:space:]\"'/])(py" <>
    "thon([0-9]+(\\.[0-9]+)?)?|pypy([0-9]+)?)([[:space:]\"'/]|$)|worker" <> "\\.py"

case System.cmd("rg", [
       "-n",
       pattern,
       "apps",
       "scripts",
       ".github",
       "-g",
       "*.ex",
       "-g",
       "*.exs",
       "-g",
       "*.sh",
       "-g",
       "*.yml",
       "-g",
       "*.yaml",
       "-g",
       "Makefile",
       "-g",
       "!check_no_repo_python.exs"
     ]) do
  {"", 1} -> IO.puts("No repository Python sources or active invocations")
  {output, _} -> IO.puts(:stderr, output); System.halt(1)
end
