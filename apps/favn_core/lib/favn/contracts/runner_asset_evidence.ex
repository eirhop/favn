defmodule Favn.Contracts.RunnerAssetEvidence do
  @moduledoc """
  Framework-owned SQL or Source evidence, separate from application metadata.

  The runner selects `kind` from the pinned asset definition, never from keys
  returned by an Elixir callback. Generation identity and execution safety remain
  governed by `RunnerAssetResult`'s top-level fields.
  """

  @type t :: %__MODULE__{
          kind: :sql | :source,
          observed: boolean() | nil,
          relation: Favn.RelationRef.t() | nil,
          materialized: Favn.RelationRef.t() | nil,
          connection: atom() | nil,
          command: String.t() | nil,
          rows_affected: non_neg_integer() | nil,
          check_results: [Favn.SQL.CheckResult.t()],
          quality_status: :passed | :warning | :failed | nil,
          write_outcome: :written | :no_op | :rolled_back | :not_started | :unknown | nil,
          transaction_outcome: :committed | :rolled_back | :not_started | :unknown | nil,
          contract_validation: Favn.SQL.ContractValidation.t() | nil,
          group_replacement: Favn.SQL.GroupReplacementResult.t() | nil,
          runtime_inputs: map() | nil,
          runtime_publication: map() | nil,
          generation_commit: Favn.Contracts.GenerationCommit.t() | nil,
          manifest_version_id: String.t() | nil,
          manifest_content_hash: String.t() | nil,
          message: String.t() | nil,
          reason: atom() | nil,
          metrics: map()
        }

  @enforce_keys [:kind]
  defstruct [
    :kind,
    :observed,
    :relation,
    :materialized,
    :connection,
    :command,
    :rows_affected,
    :quality_status,
    :write_outcome,
    :transaction_outcome,
    :contract_validation,
    :group_replacement,
    :runtime_inputs,
    :runtime_publication,
    :generation_commit,
    :manifest_version_id,
    :manifest_content_hash,
    :message,
    :reason,
    check_results: [],
    metrics: %{}
  ]

  @doc "Builds evidence from a framework execution path; rejects unknown fields."
  @spec new!(:sql | :source, map()) :: t()
  def new!(kind, fields) when kind in [:sql, :source] and is_map(fields),
    do: struct!(__MODULE__, Map.put(fields, :kind, kind))

  @doc "Checks evidence reconstructed at the persistence boundary."
  @spec valid?(term()) :: boolean()
  def valid?(nil), do: true

  def valid?(%__MODULE__{} = value) do
    value.kind in [:sql, :source] and value.observed in [nil, true, false] and
      optional_struct?(value.relation, Favn.RelationRef) and
      optional_struct?(value.materialized, Favn.RelationRef) and
      is_atom(value.connection) and is_atom(value.reason) and
      Enum.all?(
        [value.command, value.manifest_version_id, value.manifest_content_hash, value.message],
        &(is_nil(&1) or is_binary(&1))
      ) and
      (is_nil(value.rows_affected) or
         (is_integer(value.rows_affected) and value.rows_affected >= 0)) and
      is_list(value.check_results) and
      Enum.all?(value.check_results, &is_struct(&1, Favn.SQL.CheckResult)) and
      value.quality_status in [nil, :passed, :warning, :failed] and
      value.write_outcome in [nil, :written, :no_op, :rolled_back, :not_started, :unknown] and
      value.transaction_outcome in [nil, :committed, :rolled_back, :not_started, :unknown] and
      optional_struct?(value.contract_validation, Favn.SQL.ContractValidation) and
      optional_struct?(value.group_replacement, Favn.SQL.GroupReplacementResult) and
      (is_nil(value.runtime_inputs) or is_map(value.runtime_inputs)) and
      valid_publication?(value.runtime_publication) and is_map(value.metrics) and
      (is_nil(value.generation_commit) or
         Favn.Contracts.GenerationCommit.validate(value.generation_commit) == :ok)
  end

  def valid?(_value), do: false

  defp valid_publication?(nil), do: true

  defp valid_publication?(
         %{
           "publication_id" => id,
           "published_at" => published_at,
           "fresh_until" => fresh_until
         } = receipt
       ) do
    map_size(receipt) == 3 and is_binary(id) and is_binary(published_at) and
      (is_nil(fresh_until) or is_binary(fresh_until))
  end

  defp valid_publication?(_receipt), do: false

  defp optional_struct?(nil, _module), do: true
  defp optional_struct?(value, module), do: is_struct(value, module)
end
