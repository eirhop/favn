defmodule Favn.ControlPlaneQualification do
  @moduledoc false

  @identity_schema 1
  @runtime_specs [
    {:file, ".github/workflows/control-plane-image.yml"},
    {:file, "mix.exs"},
    {:file, "mix.lock"},
    {:file, "config/config.exs"},
    {:file, "config/test.exs"},
    {:file, "apps/favn/mix.exs"},
    {:tree, "apps/favn/lib"},
    {:tree, "apps/favn/priv"},
    {:file, "apps/favn_authoring/mix.exs"},
    {:tree, "apps/favn_authoring/lib"},
    {:tree, "apps/favn_authoring/priv"},
    {:file, "apps/favn_runner/mix.exs"},
    {:tree, "apps/favn_runner/lib"},
    {:tree, "apps/favn_runner/priv"},
    {:file, "apps/favn_sql_runtime/mix.exs"},
    {:tree, "apps/favn_sql_runtime/lib"},
    {:tree, "apps/favn_sql_runtime/priv"},
    {:file, "apps/favn_duckdb/mix.exs"},
    {:tree, "apps/favn_duckdb/lib"},
    {:tree, "apps/favn_duckdb/priv"},
    {:file, "apps/favn_local/mix.exs"},
    {:tree, "apps/favn_local/lib"},
    {:tree, "apps/favn_local/test/acceptance"},
    {:tree, "apps/favn_local/test_support"},
    {:file, "scripts/control_plane_image_contract.sh"},
    {:file, "scripts/control_plane_registry.sh"},
    {:file, "scripts/control_plane_qualification.ex"},
    {:file, "scripts/control_plane_qualification_id.exs"}
  ]

  @scan_specs [
    {:file, ".github/workflows/control-plane-image.yml"},
    {:file, ".github/workflows/control-plane-security-scan.yml"},
    {:file, "security/control-plane-grype.yaml"},
    {:file, "scripts/control_plane_image_contract.sh"},
    {:file, "scripts/control_plane_qualification.ex"},
    {:file, "scripts/control_plane_qualification_id.exs"}
  ]

  @runtime_exclusions MapSet.new([
                        "apps/favn_local/lib/favn/dev/backfill.ex",
                        "apps/favn_local/lib/favn/dev/data_inspection.ex",
                        "apps/favn_local/lib/favn/dev/init.ex",
                        "apps/favn_local/lib/favn/dev/run.ex",
                        "apps/favn_local/lib/favn/dev/runs.ex"
                      ])

  @image_trees [
    "apps/favn_core/lib",
    "apps/favn_core/priv",
    "apps/favn_storage_postgres/lib",
    "apps/favn_storage_postgres/priv",
    "apps/favn_orchestrator/lib",
    "apps/favn_orchestrator/priv",
    "apps/favn_view/lib",
    "apps/favn_view/priv",
    "apps/favn_view/assets",
    "rel/control_plane"
  ]

  @image_files MapSet.new([
                 "apps/favn_core/mix.exs",
                 "apps/favn_storage_postgres/mix.exs",
                 "apps/favn_orchestrator/mix.exs",
                 "apps/favn_view/mix.exs",
                 "config/prod.exs",
                 "config/runtime.exs"
               ])

  @known_unaffected_trees [
    "apps/favn_azure",
    "apps/favn_duckdb_adbc",
    "apps/favn_test_support",
    "apps/favn_local/test",
    "docs"
  ]

  @known_unaffected_files MapSet.new([
                            ".formatter.exs",
                            ".gitignore",
                            ".github/workflows/ci.yml",
                            ".github/workflows/control-plane-release.yml",
                            "CHANGELOG.md",
                            "README.md",
                            "config/dev.exs",
                            "scripts/check_elixir_static_security.sh",
                            "scripts/check_no_legacy_asset_dsl.exs",
                            "scripts/check_test_tag_tiers.exs",
                            "scripts/control_plane_qualification_test.exs",
                            "scripts/test_umbrella.exs",
                            "scripts/umbrella_test_runner.ex",
                            "scripts/umbrella_test_runner_test.exs"
                          ])

  @type identity_kind :: :runtime | :scan
  @type identities :: %{
          required(:runtime_qualification_id) => String.t(),
          required(:security_scan_id) => String.t()
        }

  @spec identities(Path.t(), String.t()) :: {:ok, identities()} | {:error, term()}
  def identities(root_dir, control_plane_build_id)
      when is_binary(root_dir) and is_binary(control_plane_build_id) do
    root_dir = Path.expand(root_dir)

    with :ok <- validate_build_id(control_plane_build_id),
         {:ok, runtime_records} <- records(root_dir, :runtime),
         {:ok, scan_records} <- records(root_dir, :scan) do
      {:ok,
       %{
         runtime_qualification_id: identity(:runtime, control_plane_build_id, runtime_records),
         security_scan_id: identity(:scan, control_plane_build_id, scan_records)
       }}
    end
  end

  @doc false
  @spec identity(identity_kind(), String.t(), [{String.t(), String.t(), non_neg_integer()}]) ::
          String.t()
  def identity(kind, control_plane_build_id, records)
      when kind in [:runtime, :scan] and is_binary(control_plane_build_id) and is_list(records) do
    payload =
      [
        "schema=#{@identity_schema}",
        "kind=#{kind}",
        "control_plane_build_id=#{control_plane_build_id}"
        | records
          |> Enum.sort_by(&elem(&1, 0))
          |> Enum.map(fn {path, digest, size} -> "#{path}\0#{size}\0#{digest}" end)
      ]
      |> Enum.join("\n")

    prefix = if kind == :runtime, do: "cpr_", else: "cps_"
    prefix <> sha256(payload)
  end

  @doc false
  @spec input_paths(Path.t(), identity_kind()) :: {:ok, [String.t()]} | {:error, term()}
  def input_paths(root_dir, kind) when is_binary(root_dir) and kind in [:runtime, :scan] do
    root_dir = Path.expand(root_dir)
    exclusions = if kind == :runtime, do: @runtime_exclusions, else: MapSet.new()

    kind
    |> specs()
    |> Enum.reduce_while({:ok, []}, fn spec, {:ok, paths} ->
      case collect_spec(root_dir, spec) do
        {:ok, discovered} -> {:cont, {:ok, discovered ++ paths}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, paths} ->
        {:ok,
         paths
         |> Enum.reject(&MapSet.member?(exclusions, &1))
         |> Enum.uniq()
         |> Enum.sort()}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec classify_paths([String.t()]) :: %{unknown_runtime_paths: [String.t()]}
  def classify_paths(paths) when is_list(paths) do
    unknown =
      paths
      |> Enum.map(&normalize_relative/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.filter(&(path_categories(&1) == [:unknown]))
      |> Enum.uniq()
      |> Enum.sort()

    %{unknown_runtime_paths: unknown}
  end

  @doc false
  @spec path_categories(String.t()) :: [identity_kind() | :image | :unaffected | :unknown]
  def path_categories(path) when is_binary(path) do
    case normalize_relative(path) do
      nil ->
        [:unknown]

      normalized ->
        cond do
          path_in_specs?(normalized, @scan_specs) and path_in_runtime?(normalized) ->
            [:runtime, :scan]

          path_in_specs?(normalized, @scan_specs) ->
            [:scan]

          path_in_runtime?(normalized) ->
            [:runtime]

          image_input_path?(normalized) ->
            [:image]

          known_unaffected_path?(normalized) ->
            [:unaffected]

          true ->
            [:unknown]
        end
    end
  end

  defp records(root_dir, kind) do
    with {:ok, paths} <- input_paths(root_dir, kind) do
      Enum.reduce_while(paths, {:ok, []}, fn relative, {:ok, records} ->
        path = Path.join(root_dir, relative)

        case File.read(path) do
          {:ok, bytes} ->
            {:cont, {:ok, [{relative, sha256(bytes), byte_size(bytes)} | records]}}

          {:error, reason} ->
            {:halt, {:error, {:qualification_input_read_failed, relative, reason}}}
        end
      end)
    end
  end

  defp collect_spec(root_dir, {:file, relative}) do
    case File.lstat(Path.join(root_dir, relative)) do
      {:ok, %{type: :regular}} -> {:ok, [relative]}
      {:ok, %{type: :symlink}} -> {:error, {:qualification_input_symlink, relative}}
      {:ok, _other} -> {:error, {:qualification_input_not_regular, relative}}
      {:error, reason} -> {:error, {:qualification_input_read_failed, relative, reason}}
    end
  end

  defp collect_spec(root_dir, {:tree, relative}) do
    case File.lstat(Path.join(root_dir, relative)) do
      {:ok, %{type: :directory}} -> walk_tree(root_dir, relative)
      {:ok, %{type: :symlink}} -> {:error, {:qualification_input_symlink, relative}}
      {:ok, _other} -> {:error, {:qualification_input_not_directory, relative}}
      {:error, :enoent} -> {:ok, []}
      {:error, reason} -> {:error, {:qualification_input_read_failed, relative, reason}}
    end
  end

  defp walk_tree(root_dir, relative) do
    path = Path.join(root_dir, relative)

    case File.lstat(path) do
      {:ok, %{type: :regular}} ->
        {:ok, [relative]}

      {:ok, %{type: :directory}} ->
        with {:ok, entries} <- File.ls(path) do
          entries
          |> Enum.sort()
          |> Enum.reduce_while({:ok, []}, fn entry, {:ok, paths} ->
            case walk_tree(root_dir, Path.join(relative, entry)) do
              {:ok, discovered} -> {:cont, {:ok, discovered ++ paths}}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
        else
          {:error, reason} -> {:error, {:qualification_input_read_failed, relative, reason}}
        end

      {:ok, %{type: :symlink}} ->
        {:error, {:qualification_input_symlink, relative}}

      {:ok, _other} ->
        {:error, {:qualification_input_not_regular, relative}}

      {:error, reason} ->
        {:error, {:qualification_input_read_failed, relative, reason}}
    end
  end

  defp path_in_runtime?(path) do
    path_in_specs?(path, @runtime_specs) and not MapSet.member?(@runtime_exclusions, path)
  end

  defp path_in_specs?(path, specs) do
    Enum.any?(specs, fn
      {:file, ^path} -> true
      {:file, _other} -> false
      {:tree, tree} -> path == tree or String.starts_with?(path, tree <> "/")
    end)
  end

  defp image_input_path?(path) do
    MapSet.member?(@image_files, path) or
      Enum.any?(@image_trees, &(path == &1 or String.starts_with?(path, &1 <> "/")))
  end

  defp known_unaffected_path?(path) do
    MapSet.member?(@runtime_exclusions, path) or
      MapSet.member?(@known_unaffected_files, path) or
      String.ends_with?(path, ".md") or
      String.contains?(path, "/test/") or
      Enum.any?(
        @known_unaffected_trees,
        &(path == &1 or String.starts_with?(path, &1 <> "/"))
      )
  end

  defp validate_build_id(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: :ok,
      else: {:error, {:invalid_control_plane_build_id, value}}
  end

  defp normalize_relative(path) do
    normalized = path |> String.replace("\\", "/") |> String.trim_leading("./")

    if normalized == "" or Path.type(normalized) == :absolute or
         Enum.any?(Path.split(normalized), &(&1 in ["", ".", ".."])),
       do: nil,
       else: normalized
  end

  defp specs(:runtime), do: @runtime_specs
  defp specs(:scan), do: @scan_specs

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
