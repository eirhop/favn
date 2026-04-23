defmodule Favn.Dev.RuntimeSource do
  @moduledoc false

  alias Favn.Dev.Paths

  @type source_kind :: :root_override | :cwd | :dependency_checkout

  @type t :: %{
          kind: source_kind(),
          root: Path.t()
        }

  @spec resolve(keyword()) :: {:ok, t()} | {:error, term()}
  def resolve(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :root_dir) do
      root = Paths.root_dir(opts) |> Path.expand()

      if valid_runtime_root?(root) do
        {:ok, %{kind: :root_override, root: root}}
      else
        {:error, {:invalid_runtime_source_root, root}}
      end
    else
      with {:error, :not_found} <- source_tree_root(),
           {:error, :not_found} <- dependency_root(),
           {:error, :not_found} <- cwd_root() do
        {:error, :runtime_source_not_found}
      else
        {:ok, _source} = ok -> ok
      end
    end
  end

  @spec fingerprint(t()) :: {:ok, map()} | {:error, term()}
  def fingerprint(%{root: root}) when is_binary(root) do
    case valid_runtime_root?(root) do
      true ->
        {:ok,
         %{
           "runtime_mix_lock_sha256" => file_sha256(Path.join(root, "mix.lock")),
           "runtime_mix_exs_sha256" => file_sha256(Path.join(root, "mix.exs")),
           "runner_mix_exs_sha256" =>
             file_sha256(Path.join(root, "apps/favn_runner/mix.exs")),
           "orchestrator_mix_exs_sha256" =>
             file_sha256(Path.join(root, "apps/favn_orchestrator/mix.exs")),
           "web_package_json_sha256" =>
             file_sha256(Path.join(root, "web/favn_web/package.json")),
           "web_package_lock_sha256" =>
             file_sha256(Path.join(root, "web/favn_web/package-lock.json"))
         }}

      false ->
        {:error, {:invalid_runtime_source_root, root}}
    end
  end

  @spec valid_runtime_root?(Path.t()) :: boolean()
  def valid_runtime_root?(root) when is_binary(root) do
    required = [
      "apps/favn_runner/mix.exs",
      "apps/favn_orchestrator/mix.exs",
      "web/favn_web/package.json"
    ]

    Enum.all?(required, &File.exists?(Path.join(root, &1)))
  end

  defp dependency_root do
    case Mix.Project.deps_paths()[:favn] do
      nil -> {:error, :not_found}
      root ->
        expanded = Path.expand(root)

        if valid_runtime_root?(expanded) do
          {:ok, %{kind: :dependency_checkout, root: expanded}}
        else
          {:error, :not_found}
        end
    end
  rescue
    _ -> {:error, :not_found}
  end

  defp source_tree_root do
    root = Path.expand("../../../../../", __DIR__)

    if valid_runtime_root?(root) do
      {:ok, %{kind: :dependency_checkout, root: root}}
    else
      {:error, :not_found}
    end
  end

  defp cwd_root do
    root = Path.expand(File.cwd!())

    if valid_runtime_root?(root) do
      {:ok, %{kind: :cwd, root: root}}
    else
      {:error, :not_found}
    end
  end

  defp file_sha256(path) do
    case File.read(path) do
      {:ok, bytes} -> :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
      {:error, :enoent} -> nil
      {:error, _reason} -> nil
    end
  end
end
