defmodule FavnStoragePostgres.RunnerTasks.Codec do
  @moduledoc false

  defdelegate encode_payload(task_kind, payload),
    to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate decode_payload(task_kind, envelope),
    to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate encode_result(task_kind, outcome, result),
    to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate decode_result(task_kind, outcome, result),
    to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate payload_hash(envelope), to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate decode_orchestration_context(envelope),
    to: Favn.Contracts.RunnerTask.PersistenceCodec

  defdelegate encode_orchestration_context(context),
    to: Favn.Contracts.RunnerTask.PersistenceCodec
end
