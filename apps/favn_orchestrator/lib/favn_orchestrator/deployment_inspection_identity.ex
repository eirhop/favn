defmodule FavnOrchestrator.DeploymentInspectionIdentity do
  @moduledoc false
  @enforce_keys [:operation_id]
  defstruct [:operation_id, :owner_id]
  @type t :: %__MODULE__{operation_id: String.t(), owner_id: String.t() | nil}
end
