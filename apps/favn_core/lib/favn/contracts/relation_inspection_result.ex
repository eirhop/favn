defmodule Favn.Contracts.RelationInspectionResult do
  @moduledoc """
  JSON-friendly result for safe read-only relation inspection.
  """

  alias Favn.RelationRef
  alias Favn.Contracts.RunnerReleaseBinding

  @type warning :: %{required(:code) => atom(), optional(:message) => String.t()}

  @type t :: %__MODULE__{
          asset_ref: Favn.Ref.t() | nil,
          required_runner_release_id: String.t(),
          relation_ref: RelationRef.t() | nil,
          relation: term() | nil,
          columns: [term()],
          row_count: non_neg_integer() | nil,
          sample: map() | nil,
          table_metadata: map(),
          adapter: atom() | nil,
          inspected_at: DateTime.t(),
          warnings: [warning()],
          error: map() | nil
        }

  defstruct asset_ref: nil,
            required_runner_release_id: nil,
            relation_ref: nil,
            relation: nil,
            columns: [],
            row_count: nil,
            sample: nil,
            table_metadata: %{},
            adapter: nil,
            inspected_at: nil,
            warnings: [],
            error: nil

  @doc "Validates the exact runner release identity echoed by this inspection result."
  @spec validate_release_binding(t()) :: :ok | {:error, RunnerReleaseBinding.error()}
  def validate_release_binding(%__MODULE__{required_runner_release_id: release_id}),
    do: RunnerReleaseBinding.validate(release_id)
end
