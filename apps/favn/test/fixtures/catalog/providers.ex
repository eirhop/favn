defmodule CatalogTest.Selected do
  @behaviour Favn.Connection
  def definition do
    %Favn.Connection.Definition{
      name: :warehouse,
      adapter: CatalogTest.Adapter,
      config_schema: [%{key: :value, type: :string, required: true}]
    }
  end
end

defmodule CatalogTest.Unrelated do
  @behaviour Favn.Connection
  def definition, do: raise("unrelated connection loaded")
end

defmodule CatalogTest.Adapter do
  def catalog_publication_backend, do: CatalogTest.Backend
  def capabilities(_, _), do: {:ok, %Favn.SQL.Capabilities{}}
  def connect(resolved, _), do: {:ok, resolved.config.value}
  def disconnect(_, _), do: :ok
end

defmodule CatalogTest.Backend do
  def applications, do: []

  def qualify(session, _, _) do
    started = Enum.map(Application.started_applications(), &elem(&1, 0))

    for forbidden <- [:favn_runner, :favn_orchestrator, :favn_storage_postgres, :catalog_test] do
      if forbidden in started, do: raise("runtime booted")
    end

    if session.conn not in ["only-selected", "slow"], do: raise("wrong configuration")
    :ok
  end

  def publish(%{conn: "slow"}, _request, _) do
    Process.sleep(5_000)
    raise "timed out work resumed"
  end

  def publish(_, request, _),
    do:
      {:ok,
       %{
         "outcome" => "committed",
         "operation_id" => request.operation_id,
         "compatibility" => "unknown"
       }}

  def reconcile(_, request, _),
    do:
      {:ok,
       %{
         "outcome" => "replayed",
         "operation_id" => request.operation_id,
         "compatibility" => "unknown"
       }}
end
