defmodule Favn.SQL.GenerationAdapter do
  @moduledoc """
  Optional SQL adapter behavior for physical target generations.

  Implementing `Favn.SQL.Adapter` does not imply this behavior. Rebuild planning
  must inspect `generation_capabilities/2` and require each safety capability
  explicitly. Generation callbacks operate on the same owner-exclusive adapter
  connection used by ordinary SQL execution.
  """

  alias Favn.Connection.Resolved
  alias Favn.RelationRef

  alias Favn.SQL.{
    Error,
    GenerationActivation,
    GenerationActivationResult,
    GenerationCapabilities,
    GenerationDiscard,
    GenerationInspection,
    GenerationMarker,
    GenerationReconciliation
  }

  alias Favn.Contracts.{GenerationCommit, GenerationPrecondition}

  @type conn :: term()
  @type opts :: keyword()

  @callback generation_capabilities(Resolved.t(), opts()) ::
              {:ok, GenerationCapabilities.t()} | {:error, Error.t()}

  @callback prepare_generation_write(conn(), GenerationPrecondition.t(), opts()) ::
              {:ok, GenerationPrecondition.t()} | {:error, Error.t()}

  @callback publish_generation_write(conn(), GenerationPrecondition.t(), opts()) ::
              {:ok, GenerationCommit.t()} | {:error, Error.t()}

  @callback inspect_generation(conn(), RelationRef.t(), opts()) ::
              {:ok, GenerationInspection.t() | :not_found} | {:error, Error.t()}

  @callback bind_relation_instance(conn(), RelationRef.t(), String.t(), opts()) ::
              :ok | {:error, Error.t()}

  @callback activate_generation(conn(), GenerationActivation.t(), opts()) ::
              {:ok, GenerationActivationResult.t()} | {:error, Error.t()}

  @callback reconcile_generation(conn(), GenerationReconciliation.t(), opts()) ::
              {:ok, GenerationMarker.t() | nil} | {:error, Error.t()}

  @callback discard_generation(conn(), GenerationDiscard.t(), opts()) ::
              :ok | {:error, Error.t()}
end
