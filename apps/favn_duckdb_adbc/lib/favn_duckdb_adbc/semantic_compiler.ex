defmodule FavnDuckdbADBC.SemanticCompiler do
  @moduledoc """
  Validates aggregate semantic SQL in an isolated, bounded DuckDB process.

  Requires Linux, Python 3, and an explicitly configured local DuckDB 1.5.2 or 1.5.5
  shared library (`:favn, :duckdb_adbc, driver: path`, or
  `DUCKDB_ADBC_DRIVER`). It never downloads a driver or opens user databases.
  Only the closed semantic expression grammar is accepted. Native result types
  describe the synthetic validation profile, not arbitrary consumer columns.
  At most 1,024 aggregate function locations are accepted. Composition supplies
  exact native-validated child aggregate offsets; these are temporary compiler
  evidence, not published metric metadata.
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

  @type input :: %{
          required(:name) => String.t(),
          required(:type) => atom(),
          required(:nullable) => boolean(),
          optional(:locations) => [non_neg_integer()]
        }
  @type result :: %{
          native_type: String.t(),
          nullable: :unknown,
          runtime_version: String.t(),
          compiler_version: String.t(),
          validation_profile: map(),
          aggregate_locations: [non_neg_integer()]
        }

  @doc "Validates one expanded expression and waits for confirmed worker exit."
  @type process_identity :: %{
          supervisor_pid: pos_integer() | nil,
          worker_pid: pos_integer() | nil,
          process_group: pos_integer() | nil
        }
  @type failure :: atom() | {:semantic_worker_cleanup_unconfirmed, process_identity()}
  @spec validate(String.t(), [input()], keyword()) :: {:ok, result()} | {:error, failure()}
  @impl true
  def validate(sql, inputs, opts \\ [])

  def validate(sql, inputs, opts) when is_binary(sql) and is_list(inputs) and is_list(opts) do
    driver = Keyword.get(Runtime.driver_opts(), :driver) || System.get_env("DUCKDB_ADBC_DRIVER")

    with :ok <- prerequisites(driver),
         :ok <- aggregate_budget(opts),
         {:ok, request} <- request(sql, inputs, driver, opts) do
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
        await_worker(port, 24_000)
      after
        if Port.info(port), do: Port.close(port)
      end
    end
  rescue
    _ -> {:error, :semantic_worker_start_failed}
  end

  def validate(_, _, _), do: {:error, :invalid_semantic_input}

  defp aggregate_budget(opts) do
    case Keyword.keyword?(opts) && Keyword.get(opts, :allowed_aggregate_locations) do
      locations when is_list(locations) and length(locations) > 1024 ->
        {:error, :aggregate_limit}

      _ ->
        :ok
    end
  end

  defp prerequisites(driver) do
    cond do
      :os.type() != {:unix, :linux} -> {:error, :semantic_worker_platform_unsupported}
      is_nil(System.find_executable("python3")) -> {:error, :semantic_python_unavailable}
      not is_binary(driver) or not File.regular?(driver) -> {:error, :semantic_driver_unavailable}
      true -> :ok
    end
  end

  defp request(sql, inputs, driver, opts) do
    if valid_options?(opts) and byte_size(sql) <= 65_536 and String.valid?(sql) and
         length(inputs) in 1..64 and
         Enum.all?(inputs, fn
           %{name: name, type: type, nullable: nullable} = input
           when is_binary(name) and is_atom(type) and is_boolean(nullable) ->
             byte_size(name) in 1..128 and String.valid?(name) and type in @logical_types and
               not String.contains?(name, <<0>>) and valid_locations?(input)

           _ ->
             false
         end) and Enum.sum(Enum.map(inputs, &length(Map.get(&1, :locations, [])))) <= 16_384 do
      Jason.encode(%{
        sql: sql,
        inputs: Enum.map(inputs, &Map.take(&1, [:name, :type, :nullable, :locations])),
        driver: Path.expand(driver),
        allowed_aggregate_locations: Keyword.get(opts, :allowed_aggregate_locations)
      })
    else
      {:error, :invalid_semantic_input}
    end
  end

  defp valid_options?(opts) do
    Keyword.keyword?(opts) and Keyword.keys(opts) in [[], [:allowed_aggregate_locations]] and
      case Keyword.get(opts, :allowed_aggregate_locations) do
        nil ->
          true

        locations when is_list(locations) ->
          length(locations) <= 1024 and
            Enum.all?(locations, &(is_integer(&1) and &1 in 0..65_535))

        _ ->
          false
      end
  end

  defp valid_locations?(%{locations: locations}) when is_list(locations),
    do:
      length(locations) <= 16_384 and Enum.all?(locations, &(is_integer(&1) and &1 in 0..65_535))

  defp valid_locations?(%{locations: _}), do: false
  defp valid_locations?(_), do: true

  @doc false
  @spec await_worker(port(), non_neg_integer()) :: {:ok, result()} | {:error, failure()}
  def await_worker(port, timeout) do
    identity = %{
      supervisor_pid: port |> Port.info(:os_pid) |> elem(1),
      worker_pid: nil,
      process_group: nil
    }

    receive_result(port, "", System.monotonic_time(:millisecond) + timeout, identity)
  end

  defp receive_result(port, output, deadline, identity) do
    receive do
      {^port, {:data, data}} when byte_size(output) + byte_size(data) <= @max_output ->
        output = output <> data
        receive_result(port, output, deadline, process_identity(output, identity))

      {^port, {:data, _}} ->
        {:error, {:semantic_worker_cleanup_unconfirmed, identity}}

      {^port, {:exit_status, 0}} ->
        decode(output, identity)

      {^port, {:exit_status, _}} ->
        {:error, {:semantic_worker_cleanup_unconfirmed, identity}}
    after
      max(deadline - System.monotonic_time(:millisecond), 0) ->
        {:error, {:semantic_worker_cleanup_unconfirmed, identity}}
    end
  end

  defp decode(output, identity) do
    line = output |> String.split("\n", trim: true) |> List.last()

    case Jason.decode(line || "") do
      {:ok,
       %{
         "ok" => true,
         "native_type" => type,
         "runtime_version" => version,
         "validation_profile" => profile,
         "aggregate_locations" => aggregates
       }} ->
        {:ok,
         %{
           native_type: type,
           nullable: :unknown,
           runtime_version: version,
           compiler_version: "duckdb-semantic-v1",
           validation_profile: profile,
           aggregate_locations: aggregates
         }}

      {:ok, %{"error" => "cleanup_unconfirmed"}} ->
        {:error, {:semantic_worker_cleanup_unconfirmed, identity}}

      {:ok, %{"error" => reason}} ->
        {:error, error(reason)}

      _ ->
        {:error, :semantic_worker_protocol_error}
    end
  end

  defp process_identity(output, identity) do
    output
    |> String.split("\n")
    |> Enum.drop(-1)
    |> Enum.reduce(identity, fn line, current ->
      case Jason.decode(line) do
        {:ok,
         %{
           "process" => %{
             "supervisor_pid" => supervisor,
             "worker_pid" => worker,
             "process_group" => group
           }
         }}
        when supervisor == current.supervisor_pid and is_integer(worker) and worker > 0 and
               worker <= 2_147_483_647 and (is_nil(group) or group == worker) ->
          %{current | worker_pid: worker, process_group: group}

        _ ->
          current
      end
    end)
  end

  defp error("unsupported_runtime"), do: :semantic_runtime_unsupported
  defp error("invalid_expression"), do: :invalid_semantic_expression
  defp error("bind_failed"), do: :semantic_bind_failed
  defp error("timeout"), do: :semantic_validation_timeout
  defp error("aggregate_limit"), do: :aggregate_limit
  defp error(_), do: :semantic_worker_failed
end
