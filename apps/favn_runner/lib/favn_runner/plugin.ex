defmodule FavnRunner.Plugin do
  @moduledoc """
  Minimal runner plugin boundary for execution runtime extensions.
  """

  @type plugin_entry :: module() | {module(), keyword()}

  @callback child_specs(keyword()) :: [Supervisor.child_spec()]

  @spec normalize_config([plugin_entry()]) :: {:ok, [{module(), keyword()}]} | {:error, term()}
  def normalize_config(entries) when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn entry, {:ok, acc} ->
      case normalize_entry(entry) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, reason} -> {:error, reason}
    end
  end

  def normalize_config(_invalid), do: {:error, :invalid_runner_plugins}

  defp normalize_entry(module) when is_atom(module), do: validate_plugin(module, [])

  defp normalize_entry({module, opts}) when is_atom(module) and is_list(opts),
    do: validate_plugin(module, opts)

  defp normalize_entry(other), do: {:error, {:invalid_runner_plugin_entry, other}}

  defp validate_plugin(module, opts) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :child_specs, 1) do
      {:ok, {module, opts}}
    else
      _ -> {:error, {:invalid_runner_plugin, module}}
    end
  end
end
