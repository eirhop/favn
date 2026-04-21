defmodule Mix.Tasks.Favn.ReadDoc do
  use Mix.Task

  @shortdoc "Reads module/function docs from local compiled code"

  @moduledoc """
  Reads documentation from BEAM docs chunks using local compiled code only.

      mix favn.read_doc ModuleName
      mix favn.read_doc ModuleName function_name
  """

  alias FavnAuthoring.DocReader

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("loadpaths")

    case args do
      [module_name] ->
        module_name
        |> parse_module_name()
        |> DocReader.read_module()
        |> print_module_result(module_name)

      [module_name, function_name] ->
        module_name
        |> parse_module_name()
        |> DocReader.read_function(function_name)
        |> print_function_result(module_name, function_name)

      _other ->
        Mix.raise("usage: mix favn.read_doc ModuleName [function_name]")
    end
  end

  defp parse_module_name(module_name) when is_binary(module_name) do
    segments =
      module_name
      |> String.trim()
      |> String.split(".", trim: true)

    if segments == [] do
      Mix.raise("invalid module name: #{inspect(module_name)}")
    end

    if Enum.all?(segments, &Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*$/, &1)) do
      Module.concat(segments)
    else
      Mix.raise("invalid module name: #{inspect(module_name)}")
    end
  end

  defp print_module_result({:ok, result}, _module_name) do
    IO.puts("Module: #{inspect(result.module)}")
    IO.puts("Format: #{result.format}")
    IO.puts("")
    IO.puts("Moduledoc:")
    IO.puts(render_doc(result.moduledoc))
  end

  defp print_module_result({:error, reason}, module_name) do
    Mix.raise(error_message(reason, module_name))
  end

  defp print_function_result({:ok, result}, _module_name, function_name) do
    IO.puts("Module: #{inspect(result.module)}")
    IO.puts("Function: #{function_name}")
    IO.puts("Format: #{result.format}")

    Enum.each(result.entries, fn entry ->
      IO.puts("")
      IO.puts("#{entry.name}/#{entry.arity}")

      if entry.signatures != [] do
        IO.puts("Signatures:")
        Enum.each(entry.signatures, &IO.puts("  #{&1}"))
      end

      IO.puts(render_doc(entry.doc))
    end)
  end

  defp print_function_result({:error, :function_not_found}, module_name, function_name) do
    Mix.raise("no public documented function named #{function_name} found in #{module_name}")
  end

  defp print_function_result({:error, reason}, module_name, _function_name) do
    Mix.raise(error_message(reason, module_name))
  end

  defp error_message({:fetch_failed, :module_not_found}, module_name) do
    "module #{module_name} is not available on the current code path; run mix compile if this is a project module"
  end

  defp error_message({:fetch_failed, :chunk_not_found}, module_name) do
    "docs chunk not found for #{module_name}; module was compiled without docs"
  end

  defp error_message({:fetch_failed, reason}, module_name) do
    "failed to read docs for #{module_name}: #{inspect(reason)}"
  end

  defp error_message(:invalid_docs_chunk, module_name),
    do: "invalid or unsupported docs chunk for #{module_name}"

  defp render_doc(:hidden), do: "(hidden)"
  defp render_doc(:none), do: "(no docs available)"

  defp render_doc(%{} = doc_map) do
    Map.get(doc_map, "en") ||
      doc_map
      |> Map.values()
      |> List.first() ||
      "(no docs available)"
  end
end
