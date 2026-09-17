defmodule FavnAuthoring.Semantic.BuildDirectory do
  @moduledoc """
  Checks the dedicated semantic build path selected before starting Mix.

  Mix may compile dependencies while locating a task, before `run/1` is called.
  Setting `MIX_BUILD_PATH` at process startup is therefore a caller prerequisite,
  not something a task can repair afterward. Never launch a runner from this
  dedicated directory. Normal development compilation/reload is a separate flow.
  """

  @doc "Rejects absent, default, symlinked, or overlapping build paths."
  @spec validate(String.t() | nil, String.t(), [String.t()]) :: :ok | {:error, atom()}
  def validate(path, root, active_paths \\ [])
  def validate(nil, _root, _active), do: {:error, :semantic_build_path_required}
  def validate("", _root, _active), do: {:error, :semantic_build_path_required}

  def validate(path, root, active) when is_binary(path) do
    path = Path.expand(path, root)
    root = Path.expand(root)
    shared = [Path.join(root, "_build") | Enum.map(active, &Path.expand(&1, root))]

    cond do
      path == root or Enum.any?(shared, &(within?(path, &1) or within?(&1, path))) ->
        {:error, :semantic_build_path_shared}

      symlink_component?(path) ->
        {:error, :semantic_build_path_symlink}

      true ->
        :ok
    end
  end

  defp within?(path, parent),
    do: Enum.take(Path.split(path), length(Path.split(parent))) == Path.split(parent)

  defp symlink_component?(path) do
    case File.lstat(path) do
      {:ok, %{type: :symlink}} -> true
      _ -> path != Path.dirname(path) and symlink_component?(Path.dirname(path))
    end
  end
end
