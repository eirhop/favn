defmodule FavnRunner.RuntimeConfigDiagnostic do
  @moduledoc false

  alias Favn.Manifest.Asset
  alias Favn.RuntimeConfig.Error, as: RuntimeConfigError

  @spec asset_resolution_failed(RuntimeConfigError.t() | term(), Asset.t()) :: map()
  def asset_resolution_failed(error, %Asset{} = asset) do
    %{
      type: :missing_runtime_config,
      phase: :asset_runtime_config,
      message: "missing required asset runtime config",
      details: %{
        asset_ref: asset.ref,
        asset_type: asset.type,
        errors: [safe_error(error)]
      }
    }
  end

  defp safe_error(%RuntimeConfigError{} = error) do
    %{
      type: error.type,
      provider: error.provider,
      key: error.key,
      env: env(error),
      scope: error.scope,
      field: error.field,
      secret?: error.secret? || false,
      message: safe_message(error)
    }
  end

  defp safe_error(_error),
    do: %{type: :unknown, message: "asset runtime config resolution failed"}

  defp env(%RuntimeConfigError{provider: :env, key: key}) when is_binary(key), do: key
  defp env(%RuntimeConfigError{}), do: nil

  defp safe_message(%RuntimeConfigError{type: :missing_env, key: key}) when is_binary(key),
    do: "missing_env #{key}"

  defp safe_message(%RuntimeConfigError{type: :invalid_ref, provider: provider}),
    do: "invalid runtime config provider #{inspect(provider)}"

  defp safe_message(%RuntimeConfigError{type: type}),
    do: "asset runtime config resolution failed with #{type}"
end
