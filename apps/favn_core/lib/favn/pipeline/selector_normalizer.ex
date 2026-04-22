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
    case resolve_asset_module.(module) do
      {:ok, {asset_module, name}} when is_atom(asset_module) and is_atom(name) ->
        {:ok, {:asset, {asset_module, name}}}

      {:error, :not_asset_module} ->
        {:error, :not_asset_module}

      {:error, reason} ->
        {:error, reason}

      _other ->
        {:error, :not_asset_module}
    end
  end

  defp normalize_selector({:module, module}, _resolve_asset_module) when is_atom(module),
    do: {:ok, {:module, module}}

  defp normalize_selector({:tag, value}, _resolve_asset_module)
       when is_atom(value) or is_binary(value),
       do: {:ok, {:tag, value}}

  defp normalize_selector({:category, value}, _resolve_asset_module)
       when is_atom(value) or is_binary(value),
       do: {:ok, {:category, value}}

  defp normalize_selector(_other, _resolve_asset_module), do: {:error, :invalid_selector}

  defp resolve_asset_module(module) when is_atom(module) do
    case Compiler.compile_module_assets(module) do
      {:ok, [%{ref: {^module, :asset}}]} ->
        {:ok, {module, :asset}}

      {:ok, _assets} ->
        {:error, :not_asset_module}

      {:error, :not_asset_module} ->
        {:error, :not_asset_module}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
