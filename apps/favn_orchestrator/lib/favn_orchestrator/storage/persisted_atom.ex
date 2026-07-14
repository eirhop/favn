defmodule FavnOrchestrator.Storage.PersistedAtom do
  @moduledoc false

  @max_module_length 255
  @module_pattern ~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/

  @spec existing(term()) :: {:ok, atom()} | {:error, term()}
  def existing(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  def existing(value), do: {:error, {:invalid_atom, value}}

  @spec module(term()) :: {:ok, module()} | {:error, term()}
  def module(value) when is_binary(value) do
    with :ok <- validate_module(value) do
      case existing(value) do
        {:ok, atom} -> {:ok, atom}
        {:error, {:unknown_atom, ^value}} -> {:error, {:unknown_module, value}}
      end
    end
  end

  def module(value), do: {:error, {:invalid_module, value}}

  defp validate_module(value) do
    if byte_size(value) <= @max_module_length and Regex.match?(@module_pattern, value),
      do: :ok,
      else: {:error, {:invalid_module, value}}
  end
end
