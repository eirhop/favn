defmodule FavnDuckdbADBC.SemanticCompiler do
  @moduledoc """
  Validates aggregate semantic SQL with DuckDB in a bounded isolated process.

  Requires a local DuckDB shared library through `:favn, :duckdb_adbc` or
  `DUCKDB_ADBC_DRIVER`. Linux supports DuckDB 1.5.2 and 1.5.5; native macOS
  26 arm64 supports 1.5.5. The packaged worker is built with the plugin. No Python
  interpreter, runtime download, or customer database is involved.

  Validation accepts only the closed expression grammar. Results describe a
  synthetic input profile and carry exact native aggregate offsets. Success is
  returned only after the native child is reaped and its supervisor exits normally.
  """

  alias FavnDuckdbADBC.Runtime
  alias FavnDuckdbADBC.SemanticCompiler.Grammar

  @behaviour Favn.Semantic.Validator

  @request_limit 262_144
  @ast_limit 2_000_000
  @final_limit 65_536
  @total_output 2_066_560
  @expression_timeout 5_000
  @logical_types ~w(integer float decimal string boolean date datetime binary time json uuid)a

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
  @type process_identity :: %{
          supervisor_pid: pos_integer() | nil,
          worker_pid: pos_integer() | nil,
          process_group: pos_integer() | nil
        }
  @type failure :: atom() | {:semantic_worker_cleanup_unconfirmed, process_identity()}

  @doc "Validates one expression and waits for confirmed native worker exit."
  @spec validate(String.t(), [input()], keyword()) :: {:ok, result()} | {:error, failure()}
  @impl true
  def validate(sql, inputs, opts \\ [])

  def validate(sql, inputs, opts) when is_binary(sql) and is_list(inputs) and is_list(opts) do
    driver = Keyword.get(Runtime.driver_opts(), :driver) || System.get_env("DUCKDB_ADBC_DRIVER")

    with :ok <- prerequisites(driver),
         :ok <- validate_input(sql, inputs, opts),
         {:ok, request} <- request(sql, inputs, driver),
         {:ok, port} <- open_worker() do
      identity = %{supervisor_pid: port_pid(port), worker_pid: nil, process_group: nil}

      try do
        if Port.command(port, request) do
          await_worker(
            port,
            inputs,
            Keyword.get(opts, :allowed_aggregate_locations),
            24_000,
            identity.supervisor_pid
          )
        else
          {:error, {:semantic_worker_cleanup_unconfirmed, identity}}
        end
      rescue
        _ -> {:error, {:semantic_worker_cleanup_unconfirmed, identity}}
      after
        if Port.info(port), do: Port.close(port)
      end
    end
  end

  def validate(_, _, _), do: {:error, :invalid_semantic_input}

  defp worker_path, do: Application.app_dir(:favn_duckdb_adbc, "priv/semantic_worker")

  defp open_worker do
    {:ok,
     Port.open({:spawn_executable, worker_path()}, [:binary, :exit_status, :use_stdio, :hide])}
  rescue
    _ -> {:error, :semantic_worker_start_failed}
  end

  defp port_pid(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} -> pid
      _ -> nil
    end
  end

  defp prerequisites(driver) do
    cond do
      :os.type() != {:unix, :linux} and
          (:os.type() != {:unix, :darwin} or
             not String.starts_with?(
               List.to_string(:erlang.system_info(:system_architecture)),
               "aarch64"
             )) ->
        {:error, :semantic_worker_platform_unsupported}

      not is_binary(driver) or not File.regular?(driver) ->
        {:error, :semantic_driver_unavailable}

      not File.regular?(worker_path()) ->
        {:error, :semantic_worker_start_failed}

      true ->
        :ok
    end
  end

  defp validate_input(sql, inputs, opts) do
    if Keyword.keyword?(opts),
      do: validate_keyword_input(sql, inputs, opts),
      else: {:error, :invalid_semantic_input}
  end

  defp validate_keyword_input(sql, inputs, opts) do
    allowed = Keyword.get(opts, :allowed_aggregate_locations)

    if Keyword.keys(opts) in [[], [:allowed_aggregate_locations]] and
         (is_nil(allowed) or
            (is_list(allowed) and length(allowed) <= 1024 and
               Enum.all?(allowed, &(is_integer(&1) and &1 in 0..65_535)))) and
         byte_size(sql) <= 65_536 and String.valid?(sql) and
         length(inputs) in 1..64 and
         Enum.all?(inputs, &valid_input?/1) and
         Enum.sum(Enum.map(inputs, &length(Map.get(&1, :locations, [])))) <= 16_384 do
      if is_list(allowed) and length(allowed) > 1024, do: {:error, :aggregate_limit}, else: :ok
    else
      if is_list(allowed) and length(allowed) > 1024,
        do: {:error, :aggregate_limit},
        else: {:error, :invalid_semantic_input}
    end
  end

  defp valid_input?(%{name: name, type: type, nullable: nullable} = input)
       when is_binary(name) and is_atom(type) and is_boolean(nullable) do
    byte_size(name) in 1..128 and String.valid?(name) and type in @logical_types and
      not String.contains?(name, <<0>>) and
      case Map.fetch(input, :locations) do
        :error ->
          true

        {:ok, locations} when is_list(locations) ->
          length(locations) <= 16_384 and
            Enum.all?(locations, &(is_integer(&1) and &1 in 0..65_535))

        _ ->
          false
      end
  end

  defp valid_input?(_), do: false

  defp request(sql, inputs, driver) do
    escaped = String.replace(sql, "'", "''")
    parse = "SELECT json_serialize_sql('SELECT " <> escaped <> "')"
    profile = Grammar.profile(inputs)

    columns =
      Enum.map_join(inputs, ", ", fn %{name: name} ->
        "CAST(NULL AS " <>
          profile[name] <>
          ~s[) AS "] <>
          String.replace(name, ~s["], ~s[""]) <> ~s["]
      end)

    bind =
      "DESCRIBE SELECT " <>
        sql <>
        " AS result FROM (SELECT " <>
        columns <> " WHERE FALSE) AS inputs"

    driver = Path.expand(driver)
    fields = [driver, parse, bind]

    if Enum.all?(fields, &(not String.contains?(&1, <<0>>))) do
      encoded = [<<1>> | Enum.map(fields, &<<byte_size(&1)::32-big, &1::binary>>)]

      if IO.iodata_length(encoded) <= @request_limit,
        do: {:ok, encoded},
        else: {:error, :invalid_semantic_input}
    else
      {:error, :invalid_semantic_input}
    end
  end

  @doc false
  @spec await_worker(port(), [input()], [non_neg_integer()] | nil, non_neg_integer()) ::
          {:ok, result()} | {:error, failure()}
  def await_worker(port, inputs, allowed, timeout) do
    await_worker(port, inputs, allowed, timeout, port_pid(port))
  end

  @doc false
  @spec await_worker(
          port(),
          [input()],
          [non_neg_integer()] | nil,
          non_neg_integer(),
          pos_integer() | nil
        ) :: {:ok, result()} | {:error, failure()}
  def await_worker(port, inputs, allowed, timeout, supervisor) do
    state = %{
      identity: %{supervisor_pid: supervisor, worker_pid: nil, process_group: nil},
      buffer: <<>>,
      bytes: 0,
      stage: :identity,
      accepted: nil,
      final: nil,
      inputs: inputs,
      allowed: allowed,
      deadline: System.monotonic_time(:millisecond) + timeout,
      expression_deadline: nil
    }

    if is_integer(supervisor),
      do: receive_result(port, state),
      else: uncertain(state)
  end

  defp receive_result(port, state) do
    receive do
      {^port, {:data, chunk}} ->
        if state.bytes + byte_size(chunk) > @total_output do
          uncertain(state)
        else
          case decode_frames(port, %{
                 state
                 | buffer: state.buffer <> chunk,
                   bytes: state.bytes + byte_size(chunk)
               }) do
            {:ok, next} -> receive_result(port, next)
            {:error, _} -> uncertain(state)
          end
        end

      {^port, {:exit_status, 0}} ->
        if state.stage == :finished and state.buffer == <<>> and not is_nil(state.final),
          do: state.final,
          else: uncertain(state)

      {^port, {:exit_status, _}} ->
        uncertain(state)
    after
      max(state.deadline - System.monotonic_time(:millisecond), 0) -> uncertain(state)
    end
  end

  defp decode_frames(port, %{buffer: <<size::32-big, rest::binary>>} = state) do
    cond do
      size < 1 or size > @ast_limit + 1 ->
        {:error, :frame_size}

      byte_size(rest) < size ->
        {:ok, state}

      true ->
        payload_size = size - 1
        <<tag, payload::binary-size(^payload_size), tail::binary>> = rest

        with {:ok, next} <- frame(port, state, tag, payload) do
          decode_frames(port, %{next | buffer: tail})
        end
    end
  end

  defp decode_frames(_port, state), do: {:ok, state}

  defp frame(_port, %{stage: :identity} = state, ?I, <<supervisor::32-big, worker::32-big>>)
       when supervisor == state.identity.supervisor_pid and worker > 0 do
    {:ok, %{state | identity: %{state.identity | worker_pid: worker}, stage: :ready}}
  end

  defp frame(_port, %{stage: :ready} = state, ?R, <<>>) do
    {:ok,
     %{
       state
       | identity: %{state.identity | process_group: state.identity.worker_pid},
         stage: :ast,
         expression_deadline: System.monotonic_time(:millisecond) + @expression_timeout
     }}
  end

  defp frame(port, %{stage: :ast} = state, ?A, payload) when byte_size(payload) <= @ast_limit do
    deadline = min(state.deadline, state.expression_deadline)

    result =
      with :ok <- json_preflight(payload, deadline),
           {:ok, parsed} <- Jason.decode(payload) do
        Grammar.validate(parsed, state.inputs, state.allowed)
      else
        _ -> {:error, :invalid_semantic_expression}
      end

    decision = if match?({:ok, _}, result), do: "Y", else: "N"

    if System.monotonic_time(:millisecond) < deadline and Port.command(port, decision) do
      {:ok, %{state | stage: :final, accepted: result}}
    else
      {:error, :deadline}
    end
  end

  defp frame(_port, %{stage: stage} = state, ?E, <<reason>>)
       when stage in [:ready, :ast, :final] do
    failure =
      case {state.accepted, reason} do
        {{:error, local}, ?i} -> {:error, local}
        {_, ?t} -> {:error, :semantic_validation_timeout}
        {_, ?u} -> {:error, :semantic_runtime_unsupported}
        {_, ?o} -> {:error, :semantic_worker_ownership_unavailable}
        {_, ?b} -> {:error, :semantic_bind_failed}
        {_, ?f} -> {:error, :semantic_worker_failed}
        _ -> {:error, :semantic_worker_protocol_error}
      end

    {:ok, %{state | final: failure, stage: :finished}}
  end

  defp frame(_port, %{stage: :final, accepted: {:ok, offsets}} = state, ?S, payload)
       when byte_size(payload) <= @final_limit do
    case :binary.split(payload, <<0>>) do
      [version, type] when byte_size(type) in 1..1024 ->
        result = %{
          native_type: type,
          nullable: :unknown,
          runtime_version: version,
          compiler_version: "duckdb-semantic-v1",
          validation_profile: Grammar.profile(state.inputs),
          aggregate_locations: offsets
        }

        {:ok, %{state | final: {:ok, result}, stage: :finished}}

      _ ->
        {:error, :invalid_result}
    end
  end

  defp frame(_port, state, ?U, <<>>),
    do: {:ok, %{state | final: uncertain(state), stage: :finished}}

  defp frame(_, _, _, _), do: {:error, :invalid_frame}

  defp uncertain(state),
    do: {:error, {:semantic_worker_cleanup_unconfirmed, state.identity}}

  defp json_preflight(payload, deadline) do
    case scan_json(payload, 0, 0, false, false, 0, deadline) do
      {0, count, false, false} when count <= 262_144 -> :ok
      _ -> {:error, :invalid_semantic_expression}
    end
  end

  defp scan_json(<<>>, depth, count, quoted, escape, _check, _deadline),
    do: {depth, count, quoted, escape}

  defp scan_json(<<char, rest::binary>>, depth, count, quoted, escape, check, deadline) do
    if check == 0 and System.monotonic_time(:millisecond) >= deadline do
      {:deadline, count, false, false}
    else
      next_check = if check == 0, do: 1023, else: check - 1

      cond do
        quoted and escape ->
          scan_json(rest, depth, count, true, false, next_check, deadline)

        quoted and char == ?\\ ->
          scan_json(rest, depth, count, true, true, next_check, deadline)

        char == ?" ->
          scan_json(rest, depth, count, not quoted, false, next_check, deadline)

        quoted ->
          scan_json(rest, depth, count, true, false, next_check, deadline)

        char in [?{, ?[] and depth < 512 and count < 262_144 ->
          scan_json(rest, depth + 1, count + 1, false, false, next_check, deadline)

        char in [?}, ?]] and depth > 0 ->
          scan_json(rest, depth - 1, count, false, false, next_check, deadline)

        char in [?{, ?[, ?}, ?]] ->
          {:invalid, count, false, false}

        true ->
          scan_json(rest, depth, count, false, false, next_check, deadline)
      end
    end
  end
end
