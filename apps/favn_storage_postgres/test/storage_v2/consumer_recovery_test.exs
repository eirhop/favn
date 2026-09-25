defmodule FavnStoragePostgres.StorageV2.ConsumerRecoveryTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias FavnOrchestrator.Events
  alias FavnStoragePostgres.Config
  alias FavnStoragePostgres.ConsumerSupervisor
  alias FavnStoragePostgres.NotificationListener
  alias FavnStoragePostgres.Outbox.Sequencer
  alias FavnStoragePostgres.Projections.Worker
  alias FavnStoragePostgres.Repo
  alias FavnStoragePostgres.StorageV2.Migrations

  @moduletag capture_log: true

  setup_all do
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    url = System.fetch_env!("FAVN_DATABASE_URL")
    assert String.starts_with?(URI.parse(url).path, "/favn_test")
    {:ok, config} = Config.connection_config(url: url, ssl_mode: :disable, pool_size: 2)
    start_supervised!({Repo, config.repo_options})
    :ok = Migrations.migrate!(Repo)
    stop_supervised!(Repo)
    :ok
  end

  setup do
    url = System.fetch_env!("FAVN_DATABASE_URL")
    assert String.starts_with?(URI.parse(url).path, "/favn_test")
    {:ok, config} = Config.connection_config(url: url, ssl_mode: :disable, pool_size: 1)

    start_supervised!(
      {Repo,
       Keyword.merge(config.repo_options, timeout: 200, queue_target: 1, queue_interval: 10)}
    )

    SQL.query!(Repo, "SELECT 1", [])
    %{config: config}
  end

  test "checkout failure backs off without killing the sequencer and wakes cannot bypass backoff" do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.checkout(
          fn ->
            send(parent, :checked_out)

            receive do
              :release -> :ok
            after
              5_000 -> :ok
            end
          end,
          timeout: 10_000
        )
      end)

    assert_receive :checked_out

    id = "sequencer-recovery-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach_many(
        id,
        [[:favn, :storage, :outbox, :sequence], [:favn, :storage, :outbox, :sequence, :error]],
        fn event, measurements, metadata, owner ->
          send(owner, {event, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(id) end)

    sequencer = start_supervised!({Sequencer, []})

    assert_receive {[:favn, :storage, :outbox, :sequence, :error], _, %{reason: :unavailable}},
                   2_000

    assert Process.alive?(sequencer)
    assert %{failure_count: 1, retry_after: retry_after} = :sys.get_state(sequencer)
    assert retry_after > System.monotonic_time(:millisecond)

    send(holder.pid, :release)
    assert :ok = Task.await(holder)
    for _ <- 1..20, do: GenServer.cast(sequencer, :wake)
    assert :sys.get_state(sequencer).failure_count == 1
    refute_receive {[:favn, :storage, :outbox, :sequence], _, _}, 50
    assert_receive {[:favn, :storage, :outbox, :sequence], _, _}, 2_000
    assert %{failure_count: 0, retry_after: nil} = :sys.get_state(sequencer)
    assert Process.whereis(Sequencer) == sequencer
  end

  test "statement timeout returns an explicit retryable failure", %{config: config} do
    blocker = start_supervised!({Postgrex, config.notification_options})
    Postgrex.query!(blocker, "BEGIN", [])
    Postgrex.query!(blocker, "SELECT 1 FROM favn_control.outbox_publication_state FOR UPDATE", [])
    SQL.query!(Repo, "SET statement_timeout = '50ms'", [])

    assert {:error, %{kind: :timeout, retryable?: true}} = Sequencer.sequence_batch(10)
    Postgrex.query!(blocker, "ROLLBACK", [])
    assert {:ok, _} = Sequencer.sequence_batch(10)
  end

  test "unexpected invariant defects still raise and roll back" do
    assert_raise MatchError, fn ->
      Repo.transaction(fn ->
        SQL.query!(Repo, "DELETE FROM favn_control.outbox_publication_state", [])
        Sequencer.sequence_batch(10)
      end)
    end

    assert %{rows: [[1]]} =
             SQL.query!(Repo, "SELECT count(*) FROM favn_control.outbox_publication_state", [])
  end

  test "a consumer restart preserves sibling processes", %{config: config} do
    start_supervised!({ConsumerSupervisor, config.notification_options})

    siblings =
      for module <- [Worker, NotificationListener, FavnStoragePostgres.Maintenance.Worker],
          into: %{},
          do: {module, Process.whereis(module)}

    original = Process.whereis(Sequencer)
    monitor = Process.monitor(original)
    Process.exit(original, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^original, :killed}

    eventually(fn ->
      is_pid(Process.whereis(Sequencer)) and Process.whereis(Sequencer) != original
    end)

    for {module, pid} <- siblings do
      assert is_pid(pid)
      assert Process.whereis(module) == pid
    end
  end

  test "deferred notification subscriptions survive unavailable startup and deliver after reconnect",
       %{config: config} do
    start_supervised!({Phoenix.PubSub, name: Events.pubsub_name()})
    :ok = Events.subscribe_persistence_publications()
    {:ok, socket} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false])
    {:ok, unavailable_port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    gate = start_supervised!({Agent, fn -> false end})

    options =
      Keyword.merge(config.notification_options,
        reconnect_backoff: 50,
        configure: {__MODULE__, :connection_options, [gate, unavailable_port]}
      )

    listener = start_supervised!({NotificationListener, options})
    state = :sys.get_state(listener)
    assert is_reference(state.committed_ref)
    assert is_reference(state.published_ref)
    assert is_reference(state.admission_ref)
    Agent.update(gate, fn _ -> true end)

    eventually(fn ->
      SQL.query!(Repo, "SELECT pg_notify('favn_outbox_published', '')", [])

      receive do
        :favn_persistence_published -> true
      after
        20 -> false
      end
    end)

    assert Process.whereis(NotificationListener) == listener
    assert :sys.get_state(listener).connection == state.connection
  end

  def connection_options(options, gate, unavailable_port) do
    if Agent.get(gate, & &1), do: options, else: Keyword.put(options, :port, unavailable_port)
  end

  defp eventually(fun, attempts \\ 100)
  defp eventually(fun, 0), do: assert(fun.())

  defp eventually(fun, attempts) do
    unless fun.() do
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
