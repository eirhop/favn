defmodule Favn.Contracts.ResourceOutcome do
  @moduledoc """
  Explicit redaction-safe health outcome for one shared runtime resource.

  Outcomes are consumed only after a node reaches a terminal attempt result.
  Omission means that the node result does not affect resource health.
  """

  alias Favn.Resource.Ref

  @statuses [:success, :failure]

  @enforce_keys [:resource, :status]
  defstruct [:resource, :status, :category, safe_to_repeat?: false]

  @type status :: :success | :failure
  @type t :: %__MODULE__{
          resource: Ref.t(),
          status: status(),
          category: atom() | String.t() | nil,
          safe_to_repeat?: boolean()
        }

  @doc "Builds and validates a resource outcome."
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(fields) when is_map(fields) or is_list(fields) do
    fields = Map.new(fields)

    with {:ok, resource} <- Ref.from_value(field(fields, :resource)),
         {:ok, status} <- normalize_status(field(fields, :status)),
         {:ok, safe_to_repeat?} <- normalize_boolean(field(fields, :safe_to_repeat?, false)) do
      {:ok,
       %__MODULE__{
         resource: resource,
         status: status,
         category: normalize_category(field(fields, :category)),
         safe_to_repeat?: safe_to_repeat?
       }}
    end
  end

  @doc "Builds a resource outcome or raises `ArgumentError`."
  @spec new!(map() | keyword()) :: t()
  def new!(fields) do
    case new(fields) do
      {:ok, outcome} -> outcome
      {:error, reason} -> raise ArgumentError, "invalid resource outcome: #{inspect(reason)}"
    end
  end

  @doc "Normalizes a list while rejecting invalid entries."
  @spec normalize_many(term()) :: {:ok, [t()]} | {:error, term()}
  def normalize_many(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case normalize(value) do
        {:ok, outcome} -> {:cont, {:ok, [outcome | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> then(fn
      {:ok, outcomes} -> {:ok, Enum.reverse(outcomes)}
      error -> error
    end)
  end

  def normalize_many(value), do: {:error, {:invalid_resource_outcomes, value}}

  defp normalize(%__MODULE__{} = outcome), do: new(Map.from_struct(outcome))
  defp normalize(value) when is_map(value) or is_list(value), do: new(value)
  defp normalize(value), do: {:error, {:invalid_resource_outcome, value}}

  defp normalize_status(status) when status in @statuses, do: {:ok, status}
  defp normalize_status("success"), do: {:ok, :success}
  defp normalize_status("failure"), do: {:ok, :failure}
  defp normalize_status(status), do: {:error, {:invalid_resource_outcome_status, status}}

  defp normalize_boolean(value) when is_boolean(value), do: {:ok, value}
  defp normalize_boolean(value), do: {:error, {:invalid_resource_outcome_safe_to_repeat, value}}

  defp normalize_category(nil), do: nil
  defp normalize_category(value) when is_atom(value) or is_binary(value), do: value
  defp normalize_category(_value), do: :other

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
