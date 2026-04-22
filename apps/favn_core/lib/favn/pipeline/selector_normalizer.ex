defmodule Favn.Pipeline.SelectorNormalizer do
  @moduledoc false

  alias Favn.Assets.Compiler

  @type selector ::
          {:asset, module() | Favn.Ref.t()}
          | {:module, module()}
          | {:tag, term()}
          | {:category, term()}

  @type resolve_asset_module :: (module() -> {:ok, Favn.Ref.t()} | {:error, term()})

  @spec normalize([selector()], keyword()) :: {:ok, [selector()]} | {:error, term()}
  def normalize(selectors, opts \\ [])

  def normalize(selectors, opts) when is_list(selectors) and is_list(opts) do
    resolve_asset_module = Keyword.get(opts, :resolve_asset_module, &resolve_asset_module/1)

    selectors
    |> Enum.reduce_while({:ok, []}, fn selector, {:ok, acc} ->
      case normalize_selector(selector, resolve_asset_module) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  def normalize(_invalid, _opts), do: {:error, :invalid_selector}

  defp normalize_selector({:asset, {module, name} = ref}, _resolve_asset_module)
       when is_atom(module) and is_atom(name) do
    {:ok, {:asset, ref}}
  end

  defp normalize_selector({:asset, module}, resolve_asset_module) when is_atom(module) do
    with {:ok, {asset_module, name}} <- resolve_asset_module.(module),
         true <- is_atom(asset_module) and is_atom(name) do
      {:ok, {:asset, {asset_module, name}}}
    else
      _ -> {:error, :not_asset_module}
    end
  end

  defp normalize_selector({:module, module}, _resolve_asset_module) when is_atom(module),
    do: {:ok, {:module, module}}

  defp normalize_selector({:tag, value}, _resolve_asset_module), do: {:ok, {:tag, value}}

  defp normalize_selector({:category, value}, _resolve_asset_module),
    do: {:ok, {:category, value}}

  defp normalize_selector(_other, _resolve_asset_module), do: {:error, :invalid_selector}

  defp resolve_asset_module(module) when is_atom(module) do
    with {:ok, assets} <- Compiler.compile_module_assets(module),
         [%{ref: {^module, :asset}}] <- assets do
      {:ok, {module, :asset}}
    else
      _ -> {:error, :not_asset_module}
    end
  end
end
