defmodule FavnRunner.TestGenerationPublication do
  @moduledoc false

  alias Favn.Contracts.{GenerationCommit, GenerationMarker, GenerationPrecondition, RunnerWork}
  @fingerprint String.duplicate("a", 64)

  # SQL unit fixtures begin with a managed target. Native adapter tests verify
  # physical identity, first creation, publication, and rollback on real tables.
  def precondition(%RunnerWork{target_operation: :normal_materialization} = work) do
    %GenerationPrecondition{
      mode: :existing,
      physical_fingerprint: @fingerprint,
      marker: %GenerationMarker{
        target_id: work.logical_target_id,
        active_relation: work.write_relation,
        active_generation_id: work.target_generation_id,
        activation_operation_id: "fixture-initial",
        activation_token: "fixture-token",
        activated_at: ~U[2026-09-24 12:00:00Z]
      }
    }
  end

  def precondition(_work), do: nil

  def generation_capabilities(_, _),
    do: {:ok, %Favn.SQL.GenerationCapabilities{atomic_publication: :supported}}

  def prepare_generation_write(_, expected, _), do: {:ok, expected}

  def publish_generation_write(_, expected, _),
    do: {:ok, %GenerationCommit{marker: expected.marker, physical_fingerprint: @fingerprint}}
end
