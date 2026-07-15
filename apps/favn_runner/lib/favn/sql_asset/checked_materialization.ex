defmodule Favn.SQLAsset.CheckedMaterialization do
  @moduledoc false

  alias Favn.SQL.{CheckResult, Result, WritePlan}

  @enforce_keys [:result, :check_results, :write_outcome]
  defstruct [
    :write_plan,
    :result,
    :reason,
    :contract_validation,
    check_results: [],
    write_outcome: :written
  ]

  @type t :: %__MODULE__{
          write_plan: WritePlan.t() | nil,
          result: Result.t(),
          check_results: [CheckResult.t()],
          write_outcome: :written | :no_op,
          reason: atom() | nil,
          contract_validation: Favn.SQL.ContractValidation.t() | nil
        }
end
