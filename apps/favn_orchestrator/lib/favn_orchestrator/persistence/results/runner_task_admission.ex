defmodule FavnOrchestrator.Persistence.Results.RunnerTaskAdmission do
  @moduledoc "The durable decision from one atomic runner-task admission."
  defstruct [:status, :task, :transition, :capacity, :context, :reason, replayed?: false]

  @type t :: %__MODULE__{
          status: :admitted | :waiting | :blocked | :already_claimed | :already_succeeded,
          task: FavnOrchestrator.Persistence.Results.RunnerTask.t() | nil,
          transition: term(),
          capacity: FavnOrchestrator.Persistence.Results.Admission.t() | nil,
          context: map() | nil,
          reason: term(),
          replayed?: boolean()
        }
end
