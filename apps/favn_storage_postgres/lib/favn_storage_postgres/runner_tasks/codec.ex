defmodule FavnStoragePostgres.RunnerTasks.Codec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask.PersistenceCodec
  alias FavnOrchestrator.RunnerTaskContext

  defdelegate encode_payload(task_kind, payload), to: PersistenceCodec
  defdelegate decode_payload(task_kind, envelope, version, packages), to: PersistenceCodec
  defdelegate encode_result(task_kind, outcome, result), to: PersistenceCodec
  defdelegate decode_result(task_kind, outcome, result, version, packages), to: PersistenceCodec
  defdelegate payload_hash(envelope), to: PersistenceCodec

  defdelegate decode_orchestration_context(envelope, version, packages),
    to: RunnerTaskContext,
    as: :decode

  defdelegate encode_orchestration_context(context), to: RunnerTaskContext, as: :encode
end
