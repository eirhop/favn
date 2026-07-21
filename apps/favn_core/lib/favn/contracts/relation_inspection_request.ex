defmodule Favn.Contracts.RelationInspectionRequest do
  @moduledoc """
  Runner-owned read-only relation inspection request.

  `sample_limit` is a non-negative runner contract value. Browser-facing layers
  currently clamp requested samples to `1..20`; a runner-level value of `0` is
  reserved for internal callers that want schema and metadata without sample
  rows.
  """

  alias Favn.RelationRef
  alias Favn.Contracts.RunnerReleaseBinding

  @type include_item :: :relation | :columns | :row_count | :sample | :table_metadata

  @type t :: %__MODULE__{
          manifest_version_id: String.t(),
          manifest_content_hash: String.t() | nil,
          required_runner_release_id: String.t(),
          asset_ref: Favn.Ref.t() | nil,
          relation: RelationRef.t() | nil,
          include: [include_item()],
          sample_limit: non_neg_integer()
        }

  defstruct manifest_version_id: nil,
            manifest_content_hash: nil,
            required_runner_release_id: nil,
            asset_ref: nil,
            relation: nil,
            include: [:relation, :columns, :row_count, :sample, :table_metadata],
            sample_limit: 20

  @doc "Validates the exact runner release identity required by this inspection."
  @spec validate_release_binding(t()) :: :ok | {:error, RunnerReleaseBinding.error()}
  def validate_release_binding(%__MODULE__{required_runner_release_id: release_id}),
    do: RunnerReleaseBinding.validate(release_id)
end
