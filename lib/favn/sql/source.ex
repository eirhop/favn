defmodule Favn.SQL.Source do
  @moduledoc false

  @spec load_file!(Macro.Env.t(), String.t(), keyword()) :: %{
          sql: String.t(),
          sql_file: String.t(),
          sql_line: pos_integer()
        }
  def load_file!(%Macro.Env{} = env, authored_path, opts \\ []) when is_binary(authored_path) do
    owner = Keyword.get(opts, :owner, "SQL")
    owner_file = env.file |> to_string() |> Path.expand()
    project_root = File.cwd!() |> Path.expand()

    if Path.type(authored_path) == :absolute do
      compile_error!(
        env.file,
        env.line,
        "#{owner} file path must be relative, got: #{inspect(authored_path)}"
      )
    end

    resolved_path = authored_path |> Path.expand(Path.dirname(owner_file)) |> Path.expand()

    if not within_project_root?(resolved_path, project_root) do
      compile_error!(
        env.file,
        env.line,
        "#{owner} file path must resolve inside the project root, got: #{inspect(authored_path)}"
      )
    end

    if Path.extname(resolved_path) != ".sql" do
      compile_error!(
        env.file,
        env.line,
        "#{owner} file path must end with .sql, got: #{inspect(authored_path)}"
      )
    end

    sql =
      case File.read(resolved_path) do
        {:ok, content} ->
          content

        {:error, reason} ->
          compile_error!(
            env.file,
            env.line,
            "failed to read #{owner} file #{inspect(authored_path)} (resolved to #{inspect(resolved_path)}): #{:file.format_error(reason)}"
          )
      end

    Module.put_attribute(env.module, :external_resource, resolved_path)

    %{
      sql: sql,
      sql_file: normalize_file(resolved_path),
      sql_line: 1
    }
  end

  defp within_project_root?(path, root) do
    canonical_path = Path.expand(path)
    canonical_root = Path.expand(root)
    root_prefix = canonical_root <> "/"

    canonical_path == canonical_root or String.starts_with?(canonical_path, root_prefix)
  end

  defp normalize_file(file) do
    file
    |> to_string()
    |> Path.relative_to_cwd()
  end

  defp compile_error!(file, line, description) do
    raise CompileError, file: file, line: line, description: description
  end
end
