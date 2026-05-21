defmodule Favn.Contracts.RunnerCancellation do
  @moduledoc """
  Runner cancellation request and outcome contracts.
  """

  @type status :: :requested | :acknowledged | :already_completed | :not_found | :unsupported

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          reason: term(),
          requested_at: DateTime.t() | nil
        }

  @type outcome :: %{
          required(:status) => status(),
          optional(:execution_id) => String.t(),
          optional(:reason) => term()
        }

  defstruct run_id: nil, reason: nil, requested_at: nil

  @doc """
  Builds a cancellation request.
  """
  @spec request(String.t() | nil, term()) :: t()
  def request(run_id, reason),
    do: %__MODULE__{run_id: run_id, reason: reason, requested_at: DateTime.utc_now()}

  @doc """
  Normalizes maps into a cancellation request.
  """
  @spec from_map(map()) :: t()
  def from_map(%__MODULE__{} = request), do: request

  def from_map(%{} = map),
    do: %__MODULE__{
      run_id: Map.get(map, :run_id, Map.get(map, "run_id")),
      reason: Map.get(map, :reason, Map.get(map, "reason")),
      requested_at: Map.get(map, :requested_at, Map.get(map, "requested_at"))
    }

  @doc """
  Builds a cancellation outcome map.
  """
  @spec outcome(status(), keyword()) :: outcome()
  def outcome(status, fields \\ [])
      when status in [:requested, :acknowledged, :already_completed, :not_found, :unsupported] do
    fields |> Map.new() |> Map.put(:status, status)
  end
end
