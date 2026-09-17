defmodule FavnDuckdbADBC.SemanticCompiler do
  @moduledoc """
  Validates aggregate semantic SQL in an isolated, bounded DuckDB process.

  Requires Linux, Python 3, and an explicitly configured local DuckDB 1.5.2 or 1.5.5
  shared library (`:favn, :duckdb_adbc, driver: path`, or
  `DUCKDB_ADBC_DRIVER`). It never downloads a driver or opens user databases.
  Only the closed semantic expression grammar is accepted. Native result types
  describe the synthetic validation profile, not arbitrary consumer columns.
  """

  alias FavnDuckdbADBC.Runtime

  @behaviour Favn.Semantic.Validator

  @external_resource Path.join(__DIR__, "semantic_compiler/worker.py")
  @worker File.read!(@external_resource)
  @max_output 16_384
  @logical_types [
    :integer,
    :float,
    :decimal,
    :string,
    :boolean,
    :date,
    :datetime,
    :binary,
    :time,
    :json,
    :uuid
  ]

  @type input :: %{name: String.t(), type: atom(), nullable: boolean()}
  @type result :: %{
          native_type: String.t(),
          nullable: :unknown,
          runtime_version: String.t(),
          compiler_version: String.t(),
          validation_profile: map()
        }

  @doc "Validates one expanded expression and waits for confirmed worker exit."
  @spec validate(String.t(), [input()]) :: {:ok, result()} | {:error, atom()}
  @impl true
  def validate(sql, inputs) when is_binary(sql) and is_list(inputs) do
    driver = Keyword.get(Runtime.driver_opts(), :driver) || System.get_env("DUCKDB_ADBC_DRIVER")

    with :ok <- prerequisites(driver),
         {:ok, request} <- request(sql, inputs, driver) do
      port =
        Port.open({:spawn_executable, System.find_executable("python3")}, [
          :binary,
          :exit_status,
          :use_stdio,
          :hide,
          args: ["-I", "-c", @worker]
        ])

      try do
        true = Port.command(port, request <> "\n")
        receive_result(port, "", System.monotonic_time(:millisecond) + 24_000)
      after
        if Port.info(port), do: Port.close(port)
      end
    end
  rescue
    _ -> {:error, :semantic_worker_start_failed}
  end

  def validate(_, _), do: {:error, :invalid_semantic_input}

  defp prerequisites(driver) do
    cond do
      :os.type() != {:unix, :linux} -> {:error, :semantic_worker_platform_unsupported}
      is_nil(System.find_executable("python3")) -> {:error, :semantic_python_unavailable}
      not is_binary(driver) or not File.regular?(driver) -> {:error, :semantic_driver_unavailable}
      true -> :ok
    end
  end

  defp request(sql, inputs, driver) do
    if byte_size(sql) <= 65_536 and String.valid?(sql) and length(inputs) in 1..64 and
         Enum.all?(inputs, fn
           %{name: name, type: type, nullable: nullable}
           when is_binary(name) and is_atom(type) and is_boolean(nullable) ->
             byte_size(name) in 1..128 and String.valid?(name) and type in @logical_types and
               not String.contains?(name, <<0>>)

           _ ->
             false
         end) do
      Jason.encode(%{
        sql: sql,
        inputs: Enum.map(inputs, &Map.take(&1, [:name, :type, :nullable])),
        driver: Path.expand(driver)
      })
    else
      {:error, :invalid_semantic_input}
    end
  end

  defp receive_result(port, output, deadline) do
    receive do
      {^port, {:data, data}} when byte_size(output) + byte_size(data) <= @max_output ->
        receive_result(port, output <> data, deadline)

      {^port, {:data, _}} ->
        {:error, :semantic_worker_output_limit}

      {^port, {:exit_status, 0}} ->
        decode(output)

      {^port, {:exit_status, _}} ->
        {:error, :semantic_worker_failed}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        {:error, :semantic_worker_cleanup_unconfirmed}
    end
  end

  defp decode(output) do
    case Jason.decode(output) do
      {:ok,
       %{
         "ok" => true,
         "native_type" => type,
         "runtime_version" => version,
         "validation_profile" => profile
       }} ->
        {:ok,
         %{
           native_type: type,
           nullable: :unknown,
           runtime_version: version,
           compiler_version: "duckdb-semantic-v1",
           validation_profile: profile
         }}

      {:ok, %{"error" => reason}} ->
        {:error, error(reason)}

      _ ->
        {:error, :semantic_worker_protocol_error}
    end
  end

  defp error("unsupported_runtime"), do: :semantic_runtime_unsupported
  defp error("invalid_expression"), do: :invalid_semantic_expression
  defp error("bind_failed"), do: :semantic_bind_failed
  defp error("timeout"), do: :semantic_validation_timeout
  defp error("cleanup_unconfirmed"), do: :semantic_worker_cleanup_unconfirmed
  defp error(_), do: :semantic_worker_failed
end
