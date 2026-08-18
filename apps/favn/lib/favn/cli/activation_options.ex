defmodule Favn.CLI.ActivationOptions do
  @moduledoc false

  @default_timeout_ms 180_000
  @maximum_timeout_ms 900_000
  @default_reconcile_timeout_ms 10_000
  @maximum_reconcile_timeout_ms 60_000

  @enforce_keys [:timeout_ms, :reconcile_timeout_ms, :operation_id]
  defstruct [:timeout_ms, :reconcile_timeout_ms, :operation_id]

  @type t :: %__MODULE__{
          timeout_ms: pos_integer(),
          reconcile_timeout_ms: pos_integer(),
          operation_id: String.t()
        }

  @spec new(keyword(), String.t(), String.t()) :: {:ok, t()} | {:error, term()}
  def new(opts, manifest_version_id, workspace_id)
      when is_list(opts) and is_binary(manifest_version_id) and is_binary(workspace_id) do
    with :ok <- identifier(manifest_version_id, :manifest_version_id),
         :ok <- identifier(workspace_id, :workspace_id),
         {:ok, timeout_ms} <-
           timeout(opts, :timeout_ms, @default_timeout_ms, @maximum_timeout_ms),
         {:ok, reconcile_timeout_ms} <-
           timeout(
             opts,
             :reconcile_timeout_ms,
             @default_reconcile_timeout_ms,
             @maximum_reconcile_timeout_ms
           ),
         {:ok, operation_id} <- operation_id(opts, manifest_version_id, workspace_id) do
      {:ok,
       %__MODULE__{
         timeout_ms: timeout_ms,
         reconcile_timeout_ms: reconcile_timeout_ms,
         operation_id: operation_id
       }}
    end
  end

  @spec to_keyword(t()) :: keyword()
  def to_keyword(%__MODULE__{} = options) do
    [
      timeout_ms: options.timeout_ms,
      reconcile_timeout_ms: options.reconcile_timeout_ms,
      operation_id: options.operation_id
    ]
  end

  defp timeout(opts, key, default, maximum) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 and value <= maximum -> {:ok, value}
      _invalid -> {:error, {:invalid_option, key}}
    end
  end

  defp identifier(value, _key) when byte_size(value) in 1..255, do: :ok
  defp identifier(_value, key), do: {:error, {:invalid_option, key}}

  defp operation_id(opts, manifest_version_id, workspace_id) do
    case Keyword.get(opts, :operation_id) do
      nil -> {:ok, default_operation_id(opts, manifest_version_id, workspace_id)}
      value when is_binary(value) and byte_size(value) in 1..512 -> {:ok, value}
      _invalid -> {:error, {:invalid_option, :operation_id}}
    end
  end

  defp default_operation_id(opts, manifest_version_id, workspace_id) do
    case Keyword.get(opts, :maintenance_token) do
      token when is_binary(token) and token != "" ->
        input = %{
          manifest_version_id: manifest_version_id,
          workspace_id: workspace_id,
          command_token: token
        }

        fingerprint =
          %{operation: :activate_manifest, session: %{}, input: input}
          |> canonicalize()
          |> JSON.encode!()

        "favn-local-" <>
          (:crypto.hash(:sha256, fingerprint) |> Base.url_encode64(padding: false))

      _missing ->
        "favn-activation-" <>
          (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))
    end
  end

  defp canonicalize(value) when is_binary(value),
    do: %{"__type__" => "string", "value" => value}

  defp canonicalize(value) when is_atom(value),
    do: %{"__type__" => "atom", "value" => Atom.to_string(value)}

  defp canonicalize(value) when is_map(value) do
    entries =
      value
      |> Enum.map(fn {key, val} -> [to_string(key), canonicalize(val)] end)
      |> Enum.sort_by(fn [key, _val] -> key end)

    %{"__type__" => "map", "entries" => entries}
  end
end
