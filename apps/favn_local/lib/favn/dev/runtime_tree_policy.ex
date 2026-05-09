defmodule Favn.Dev.RuntimeTreePolicy do
  @moduledoc false

  @entries ["mix.exs", "mix.lock", "config", "apps"]
  @optional_entries ["mix.exs", "mix.lock", "config"]
  @ignored_entries [
    ".elixir_ls",
    ".favn",
    ".git",
    "_build",
    "cover",
    "deps"
  ]
  @ignored_relative_entries ["apps/favn_view/priv/static/assets"]

  @spec entries() :: [Path.t()]
  def entries, do: @entries

  @spec optional_entries() :: [Path.t()]
  def optional_entries, do: @optional_entries

  @spec ignored_entries() :: [String.t()]
  def ignored_entries, do: @ignored_entries

  @spec reduce_required_entries(Path.t(), term(), (Path.t(), Path.t(), term() ->
                                                     {:ok, term()} | {:error, term()})) ::
          {:ok, term()} | {:error, term()}
  def reduce_required_entries(root, acc, fun) when is_binary(root) and is_function(fun, 3) do
    Enum.reduce_while(entries(), {:ok, acc}, fn relative, {:ok, current_acc} ->
      path = Path.join(root, relative)

      cond do
        not File.exists?(path) and relative in optional_entries() ->
          {:cont, {:ok, current_acc}}

        not File.exists?(path) ->
          {:halt, {:error, {:missing_runtime_entry, relative, path}}}

        true ->
          case fun.(path, relative, current_acc) do
            {:ok, next_acc} -> {:cont, {:ok, next_acc}}
            {:error, _reason} = error -> {:halt, error}
          end
      end
    end)
  end

  @spec reduce_child_entries(Path.t(), term(), (File.posix() -> term()), (
                                Path.t(), String.t(), term() -> {:ok, term()} | {:error, term()}
                              )) :: {:ok, term()} | {:error, term()}
  def reduce_child_entries(path, acc, list_error, fun)
      when is_binary(path) and is_function(list_error, 1) and is_function(fun, 3) do
    case File.ls(path) do
      {:ok, entries} ->
        Enum.reduce_while(entries, {:ok, acc}, fn entry, {:ok, current_acc} ->
          child_path = Path.join(path, entry)

          if ignored_entry?(child_path, entry) do
            {:cont, {:ok, current_acc}}
          else
            case fun.(child_path, entry, current_acc) do
              {:ok, next_acc} -> {:cont, {:ok, next_acc}}
              {:error, _reason} = error -> {:halt, error}
            end
          end
        end)

      {:error, reason} ->
        {:error, list_error.(reason)}
    end
  end

  defp ignored_entry?(path, entry) do
    entry in ignored_entries() or path_has_ignored_suffix?(path)
  end

  defp path_has_ignored_suffix?(path) do
    normalized = path |> Path.expand() |> Path.split() |> Path.join()

    Enum.any?(@ignored_relative_entries, fn relative ->
      String.ends_with?(normalized, relative)
    end)
  end
end
