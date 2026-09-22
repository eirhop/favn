defmodule FavnSQLRuntime.RuntimeCatalogRetryTest do
  use ExUnit.Case, async: true
  alias Favn.RuntimeCatalog.Publication
  alias Favn.SQL.{Error, RuntimeCatalog, Session}

  defmodule LegacyBackend do
    def runtime_catalog_backend, do: __MODULE__
  end

  defmodule Backend do
    def runtime_catalog_backend, do: __MODULE__
    def qualify_materialization_retry(session, _, _, _), do: session.conn
  end

  test "missing callback and generation candidates default to unsupported" do
    session =
      struct!(Session, adapter: LegacyBackend, conn: nil, resolved: nil, capabilities: nil)

    publication = struct(Publication, candidate: false)

    assert {:ok, :unsupported} =
             RuntimeCatalog.qualify_materialization_retry(session, publication, nil, [])

    assert {:ok, :unsupported} =
             RuntimeCatalog.qualify_materialization_retry(
               %{session | adapter: Backend},
               %{publication | candidate: true},
               nil,
               []
             )

    assert {:ok, :unsupported} =
             RuntimeCatalog.qualify_materialization_retry(session, nil, nil, [])
  end

  test "qualification is closed and preserves typed failures" do
    publication = struct(Publication, candidate: false)

    for result <- [
          {:ok, :supported},
          {:ok, :unsupported},
          {:error, %Error{type: :connection_error, message: "unavailable"}}
        ] do
      session = struct!(Session, adapter: Backend, conn: result, resolved: nil, capabilities: nil)
      assert ^result = RuntimeCatalog.qualify_materialization_retry(session, publication, nil, [])
    end

    session =
      struct!(Session, adapter: Backend, conn: {:ok, :maybe}, resolved: nil, capabilities: nil)

    assert {:error, %Error{retryable?: false}} =
             RuntimeCatalog.qualify_materialization_retry(session, publication, nil, [])
  end

  test "nested uncertainty takes precedence and arbitrary maps cannot prove rejection" do
    error = %Error{
      type: :transaction_conflict,
      operation: :transaction,
      message: "rejected",
      details: %{transaction_stage: :commit, transaction_outcome: :rolled_back}
    }

    assert Error.rejected_transaction?(error)
    refute Error.rejected_transaction?(Map.from_struct(error))

    for cause <- [
          %{unknown_outcome?: true},
          %{"transaction_outcome" => "unknown"},
          %Error{type: :operation_timeout, message: "uncertain"},
          %{classification: :unknown_commit_state},
          %{type: :operation_timeout},
          %{"type" => "operation_timeout"}
        ] do
      refute Error.rejected_transaction?(%{error | cause: cause})
    end
  end
end
