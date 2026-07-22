defmodule Favn.Dev.Config do
  @moduledoc """
  Resolves minimal local developer tooling configuration.
  """

  @enforce_keys [
    :workspace_id,
    :orchestrator_port,
    :web_port,
    :scheduler_enabled
  ]
  defstruct [
    :workspace_id,
    :orchestrator_port,
    :web_port,
    :scheduler_enabled
  ]

  @type t :: %__MODULE__{
          workspace_id: String.t(),
          orchestrator_port: pos_integer(),
          web_port: pos_integer(),
          scheduler_enabled: boolean()
        }

  @typedoc "Keyword overrides used by local tooling tasks."
  @type opts :: keyword()

  @default_orchestrator_port 4101
  @default_web_port 4173

  @doc """
  Resolves local tooling configuration from app config plus runtime overrides.
  """
  @spec resolve(opts()) :: t()
  def resolve(opts \\ []) when is_list(opts) do
    dev_config = Application.get_env(:favn, :dev, [])
    local_config = Application.get_env(:favn, :local, [])
    merged = dev_config |> Keyword.merge(local_config) |> Keyword.merge(opts)

    orchestrator_port =
      merged
      |> Keyword.get(:orchestrator_port, @default_orchestrator_port)
      |> normalize_int(@default_orchestrator_port)

    web_port =
      merged
      |> Keyword.get(:web_port, @default_web_port)
      |> normalize_int(@default_web_port)

    %__MODULE__{
      workspace_id: normalize_workspace_id(Keyword.get(merged, :workspace_id, "local-dev")),
      orchestrator_port: orchestrator_port,
      web_port: web_port,
      scheduler_enabled: normalize_bool(Keyword.get(merged, :scheduler, false), false)
    }
  end

  defp normalize_int(value, _default) when is_integer(value) and value > 0, do: value

  defp normalize_int(value, default) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
      {int, ""} when int > 0 -> int
      _ -> default
    end
  end

  defp normalize_int(_value, default), do: default

  defp normalize_bool(value, _default) when is_boolean(value), do: value
  defp normalize_bool("true", _default), do: true
  defp normalize_bool("false", _default), do: false
  defp normalize_bool(_value, default), do: default

  defp normalize_workspace_id(value) when is_binary(value) do
    case String.trim(value) do
      id when id != "" and byte_size(id) <= 255 -> id
      _invalid -> raise ArgumentError, "local workspace_id must contain 1..255 bytes"
    end
  end

  defp normalize_workspace_id(_value),
    do: raise(ArgumentError, "local workspace_id must be a string")

end
