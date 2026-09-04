defmodule Favn.Contracts.RunnerTask do
  @moduledoc """
  Versioned contracts for durable pull-based runner tasks.

  Runtime identities stay bounded strings. Pool names are never converted from
  wire input into new atoms.
  """

  @version 13
  @task_kinds [
    :asset_attempt,
    :relation_inspection,
    :generation_capabilities,
    :generation_marker_read,
    :generation_marker_initialize,
    :generation_activate,
    :generation_reconcile,
    :generation_discard
  ]
  @retry_classes [:safe_to_retry, :reconcile_before_retry, :unknown_do_not_retry, :terminal]
  @terminal_outcomes [:succeeded, :failed, :cancelled, :unknown]
  @reconcilable_kinds [
    :generation_marker_initialize,
    :generation_activate,
    :generation_reconcile,
    :generation_discard
  ]

  @spec version() :: 13
  def version, do: @version

  @spec task_kinds() :: [atom()]
  def task_kinds, do: @task_kinds

  @spec retry_classes() :: [atom()]
  def retry_classes, do: @retry_classes

  @spec terminal_outcomes() :: [atom()]
  def terminal_outcomes, do: @terminal_outcomes

  @doc "Returns the conservative default recovery class for a task kind."
  @spec default_retry_class(atom()) :: atom()
  def default_retry_class(kind)
      when kind in [:relation_inspection, :generation_capabilities, :generation_marker_read],
      do: :safe_to_retry

  def default_retry_class(kind)
      when kind in [
             :generation_marker_initialize,
             :generation_activate,
             :generation_reconcile,
             :generation_discard
           ],
      do: :reconcile_before_retry

  def default_retry_class(:asset_attempt), do: :unknown_do_not_retry

  @doc false
  @spec valid_initial_retry_class?(atom(), atom()) :: boolean()
  def valid_initial_retry_class?(kind, retry_class),
    do: retry_class == default_retry_class(kind)

  @doc false
  @spec validate_terminal_retry(atom(), atom(), atom(), term()) :: :ok | {:error, term()}
  def validate_terminal_retry(_kind, :succeeded, :terminal, nil), do: :ok

  def validate_terminal_retry(_kind, :cancelled, :terminal, nil), do: :ok

  def validate_terminal_retry(
        _kind,
        :cancelled,
        :terminal,
        %Favn.Contracts.RunnerError{outcome: :cancelled} = error
      ),
      do: Favn.Contracts.RunnerError.validate(error)

  def validate_terminal_retry(
        _kind,
        :failed,
        :safe_to_retry,
        %Favn.Contracts.RunnerError{outcome: :safe_failure, retryable?: true} = error
      ),
      do: Favn.Contracts.RunnerError.validate(error)

  def validate_terminal_retry(
        kind,
        outcome,
        :reconcile_before_retry,
        %Favn.Contracts.RunnerError{outcome: :unknown} = error
      )
      when outcome in [:failed, :unknown] and kind in @reconcilable_kinds,
      do: Favn.Contracts.RunnerError.validate(error)

  def validate_terminal_retry(
        _kind,
        outcome,
        :unknown_do_not_retry,
        %Favn.Contracts.RunnerError{outcome: :unknown} = error
      )
      when outcome in [:failed, :unknown],
      do: Favn.Contracts.RunnerError.validate(error)

  def validate_terminal_retry(
        _kind,
        :failed,
        :terminal,
        %Favn.Contracts.RunnerError{} = error
      ),
      do: Favn.Contracts.RunnerError.validate(error)

  def validate_terminal_retry(kind, outcome, retry_class, error),
    do: {:error, {:invalid_runner_task_retry_classification, kind, outcome, retry_class, error}}

  @doc """
  Maps a normalized runner error to the one `{outcome, retry_class}` pair that
  `validate_terminal_retry/4` accepts for it.

  Total over every `Favn.Contracts.RunnerError` outcome, so a runner that
  classifies failure results through this function always produces a result
  the control plane can accept. The pair follows the error envelope:

    * a `:cancelled` error reports a cancelled task
    * a retryable `:safe_failure` may be retried safely
    * a non-retryable `:safe_failure` failed deterministically and is terminal
    * an `:unknown` outcome must not be blindly retried; reconcilable
      generation task kinds keep their reconcile-then-retry class
  """
  @spec classify_failure(atom(), Favn.Contracts.RunnerError.t()) ::
          {:cancelled | :failed | :unknown,
           :terminal | :safe_to_retry | :reconcile_before_retry | :unknown_do_not_retry}
  def classify_failure(kind, %Favn.Contracts.RunnerError{} = error) when kind in @task_kinds do
    case error do
      %{outcome: :cancelled} -> {:cancelled, :terminal}
      %{outcome: :safe_failure, retryable?: true} -> {:failed, :safe_to_retry}
      %{outcome: :safe_failure} -> {:failed, :terminal}
      _unknown when kind in @reconcilable_kinds -> {:unknown, :reconcile_before_retry}
      _unknown -> {:unknown, :unknown_do_not_retry}
    end
  end

  @doc false
  @spec validate_payload(atom(), term()) :: :ok | {:error, term()}
  def validate_payload(:asset_attempt, %Favn.Contracts.RunnerWork{}), do: :ok

  def validate_payload(:relation_inspection, %Favn.Contracts.RelationInspectionRequest{}),
    do: :ok

  def validate_payload(
        :generation_capabilities,
        %Favn.Contracts.GenerationCapabilitiesRequest{} = request
      ),
      do: Favn.Contracts.GenerationCapabilitiesRequest.validate(request)

  def validate_payload(
        :generation_marker_read,
        %Favn.Contracts.GenerationMarkerReadRequest{} = request
      ),
      do: Favn.Contracts.GenerationMarkerReadRequest.validate(request)

  def validate_payload(
        :generation_marker_initialize,
        %Favn.Contracts.GenerationMarkerInitializationRequest{}
      ),
      do: :ok

  def validate_payload(:generation_activate, %Favn.Contracts.GenerationActivationRequest{}),
    do: :ok

  def validate_payload(:generation_reconcile, %Favn.Contracts.GenerationReconciliationRequest{}),
    do: :ok

  def validate_payload(:generation_discard, %Favn.Contracts.GenerationDiscardRequest{}), do: :ok

  def validate_payload(kind, payload),
    do: {:error, {:invalid_runner_task_payload, kind, payload}}

  @doc false
  @spec validate_result(atom(), atom(), term()) :: :ok | {:error, term()}
  def validate_result(_kind, outcome, nil) when outcome in [:failed, :cancelled, :unknown],
    do: :ok

  def validate_result(:asset_attempt, outcome, %Favn.Contracts.RunnerResult{})
      when outcome in @terminal_outcomes,
      do: :ok

  def validate_result(
        :relation_inspection,
        :succeeded,
        %Favn.Contracts.RelationInspectionResult{}
      ),
      do: :ok

  def validate_result(
        :generation_capabilities,
        :succeeded,
        %Favn.Contracts.GenerationCapabilitiesResult{} = result
      ),
      do: Favn.Contracts.GenerationCapabilitiesResult.validate(result)

  def validate_result(
        :generation_marker_read,
        :succeeded,
        %Favn.Contracts.GenerationMarkerReadResult{} = result
      ),
      do: Favn.Contracts.GenerationMarkerReadResult.validate(result)

  def validate_result(
        :generation_marker_initialize,
        :succeeded,
        %Favn.Contracts.GenerationMarkerInitializationResult{}
      ),
      do: :ok

  def validate_result(
        :generation_activate,
        :succeeded,
        %Favn.Contracts.GenerationActivationResult{}
      ),
      do: :ok

  def validate_result(
        :generation_reconcile,
        :succeeded,
        %Favn.Contracts.GenerationReconciliationResult{}
      ),
      do: :ok

  def validate_result(
        :generation_discard,
        :succeeded,
        %Favn.Contracts.GenerationDiscardResult{}
      ),
      do: :ok

  def validate_result(kind, outcome, result),
    do: {:error, {:invalid_runner_task_result, kind, outcome, result}}
end

defmodule Favn.Contracts.RunnerTask.Contract do
  @moduledoc false

  alias Favn.Contracts.RunnerReleaseBinding

  @identity_fields [
    :workspace_id,
    :task_id,
    :runner_instance_id,
    :boot_id,
    :assignment_id,
    :batch_id,
    :command_id,
    :resolution_id
  ]
  @max_identity_bytes 255

  def validate(struct, required, enums, limit, session_fenced?) do
    fields = Map.from_struct(struct)

    with :ok <- exact_version(fields),
         :ok <- required_fields(fields, required),
         :ok <- identities(fields),
         :ok <- runner_pool(fields),
         :ok <- release(fields),
         :ok <- generations(fields, session_fenced?),
         :ok <- bounded_scalars(fields),
         :ok <- datetimes(fields),
         :ok <- enums(fields, enums),
         :ok <- payload_size(struct, limit) do
      :ok
    end
  end

  def redact(struct) do
    struct
    |> Map.from_struct()
    |> redact_value()
  end

  defp exact_version(%{version: 13}), do: :ok

  defp exact_version(fields),
    do: {:error, {:unsupported_runner_task_version, Map.get(fields, :version)}}

  defp required_fields(fields, required) do
    case Enum.find(required, &(not present?(Map.get(fields, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_runner_task_field, field}}
    end
  end

  defp identities(fields) do
    Enum.reduce_while(@identity_fields, :ok, fn field, :ok ->
      case Map.fetch(fields, field) do
        :error ->
          {:cont, :ok}

        {:ok, nil} ->
          {:cont, :ok}

        {:ok, value} when is_binary(value) and byte_size(value) in 1..@max_identity_bytes ->
          {:cont, :ok}

        {:ok, value} ->
          {:halt, {:error, {:invalid_runner_task_identity, field, value}}}
      end
    end)
  end

  defp runner_pool(%{runner_pool: value}) do
    case Favn.RunnerPool.validate_runtime(value) do
      :ok -> :ok
      {:error, _reason} -> {:error, {:invalid_runner_task_pool, value}}
    end
  end

  defp runner_pool(_fields), do: :ok

  defp release(%{required_runner_release_id: value}), do: RunnerReleaseBinding.validate(value)
  defp release(_fields), do: :ok

  defp generations(fields, session_fenced?) do
    with :ok <-
           [:runner_session_generation, :assignment_generation, :sequence, :result_version]
           |> Enum.reduce_while(:ok, fn field, :ok ->
             case Map.fetch(fields, field) do
               :error ->
                 {:cont, :ok}

               {:ok, nil} ->
                 {:cont, :ok}

               {:ok, value} when is_integer(value) and value >= 0 ->
                 {:cont, :ok}

               {:ok, value} ->
                 {:halt, {:error, {:invalid_runner_task_generation, field, value}}}
             end
           end),
         :ok <- positive_session_fence(fields, session_fenced?),
         :ok <- positive_assignment_fence(fields) do
      :ok
    end
  end

  defp positive_session_fence(
         %{runner_instance_id: runner_instance_id, runner_session_generation: generation},
         true
       )
       when is_binary(runner_instance_id) and is_integer(generation) and generation > 0,
       do: :ok

  defp positive_session_fence(
         %{runner_instance_id: runner_instance_id, runner_session_generation: generation},
         true
       )
       when is_binary(runner_instance_id),
       do: {:error, {:invalid_runner_task_session_fence, generation}}

  defp positive_session_fence(_fields, _session_fenced?), do: :ok

  defp positive_assignment_fence(%{
         task_id: task_id,
         runner_instance_id: runner_instance_id,
         runner_session_generation: session_generation,
         assignment_generation: assignment_generation
       })
       when is_binary(task_id) and is_binary(runner_instance_id) and session_generation > 0 and
              assignment_generation > 0,
       do: :ok

  defp positive_assignment_fence(%{
         task_id: task_id,
         runner_instance_id: runner_instance_id,
         runner_session_generation: session_generation,
         assignment_generation: assignment_generation
       })
       when is_binary(task_id) and is_binary(runner_instance_id),
       do:
         {:error,
          {:invalid_runner_task_assignment_fence, session_generation, assignment_generation}}

  defp positive_assignment_fence(_fields), do: :ok

  defp bounded_scalars(fields) do
    with :ok <- exact_one_slot(Map.get(fields, :slots)),
         :ok <- bounded_wait(Map.get(fields, :wait_ms)),
         :ok <- bounded_node(Map.get(fields, :beam_node)),
         :ok <- capability_list(Map.get(fields, :capabilities)),
         :ok <- task_kind_list(Map.get(fields, :supported_task_kinds)),
         :ok <- digest_field(:payload_fingerprint, Map.get(fields, :payload_fingerprint)),
         :ok <- list_field(:entries, Map.get(fields, :entries)) do
      :ok
    end
  end

  defp exact_one_slot(nil), do: :ok
  defp exact_one_slot(1), do: :ok
  defp exact_one_slot(value), do: {:error, {:invalid_runner_task_slots, value}}

  defp bounded_wait(nil), do: :ok
  defp bounded_wait(value) when is_integer(value) and value in 0..3_600_000, do: :ok
  defp bounded_wait(value), do: {:error, {:invalid_runner_task_wait_ms, value}}

  defp bounded_node(nil), do: :ok
  defp bounded_node(value) when is_binary(value) and byte_size(value) in 1..255, do: :ok
  defp bounded_node(value), do: {:error, {:invalid_runner_task_beam_node, value}}

  defp capability_list(nil), do: :ok

  defp capability_list(values) when is_list(values) and length(values) <= 64 do
    if Enum.all?(
         values,
         &(is_binary(&1) and Regex.match?(~r/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/, &1))
       ) and length(values) == length(Enum.uniq(values)) do
      :ok
    else
      {:error, {:invalid_runner_task_capabilities, values}}
    end
  end

  defp capability_list(value), do: {:error, {:invalid_runner_task_capabilities, value}}

  defp task_kind_list(nil), do: :ok

  defp task_kind_list(values) when is_list(values) and values != [] do
    if Enum.all?(values, &(&1 in Favn.Contracts.RunnerTask.task_kinds())) and
         length(values) == length(Enum.uniq(values)) do
      :ok
    else
      {:error, {:invalid_runner_task_supported_task_kinds, values}}
    end
  end

  defp task_kind_list(value),
    do: {:error, {:invalid_runner_task_supported_task_kinds, value}}

  defp digest_field(_field, nil), do: :ok
  defp digest_field(_field, value) when is_binary(value) and byte_size(value) == 32, do: :ok
  defp digest_field(field, value), do: {:error, {:invalid_runner_task_digest, field, value}}

  defp list_field(_field, nil), do: :ok
  defp list_field(_field, value) when is_list(value), do: :ok
  defp list_field(field, value), do: {:error, {:invalid_runner_task_list, field, value}}

  defp datetimes(fields) do
    [
      :issued_at,
      :assigned_at,
      :lease_expires_at,
      :occurred_at,
      :finished_at,
      :requested_at,
      :acknowledged_at
    ]
    |> Enum.reduce_while(:ok, fn field, :ok ->
      case Map.fetch(fields, field) do
        :error -> {:cont, :ok}
        {:ok, nil} -> {:cont, :ok}
        {:ok, %DateTime{}} -> {:cont, :ok}
        {:ok, value} -> {:halt, {:error, {:invalid_runner_task_datetime, field, value}}}
      end
    end)
  end

  defp enums(fields, enums) do
    Enum.reduce_while(enums, :ok, fn {field, accepted}, :ok ->
      value = Map.get(fields, field)

      if value in accepted,
        do: {:cont, :ok},
        else: {:halt, {:error, {:invalid_runner_task_enum, field, value}}}
    end)
  end

  defp payload_size(struct, limit) do
    size = struct |> :erlang.term_to_binary([:deterministic]) |> byte_size()
    if size <= limit, do: :ok, else: {:error, {:runner_task_payload_too_large, size, limit}}
  end

  defp present?(value), do: not is_nil(value) and value != ""

  defp redact_value(%_{} = struct), do: struct

  defp redact_value(map) when is_map(map) do
    Map.new(map, fn {key, value} ->
      if key in [:payload, :result, :runtime_inputs, :entries, :error],
        do: {key, "[REDACTED]"},
        else: {key, redact_value(value)}
    end)
  end

  defp redact_value(list) when is_list(list), do: Enum.map(list, &redact_value/1)
  defp redact_value(value), do: value
end

defmodule Favn.Contracts.RunnerTask.Codec do
  @moduledoc false

  alias Favn.Contracts.RunnerTask.Limits

  def encode(struct, tag, validate) do
    with :ok <- validate.(struct) do
      payload =
        struct
        |> Map.from_struct()
        |> :erlang.term_to_binary([:deterministic])
        |> Base.encode64()

      limit = Limits.wire_bytes(struct.__struct__)

      if byte_size(payload) <= limit,
        do: {:ok, %{"type" => tag, "version" => 13, "payload" => payload}},
        else: {:error, {:runner_task_encoded_payload_too_large, byte_size(payload), limit}}
    end
  end

  def decode_for(module, tag, %{"type" => tag, "version" => 13, "payload" => payload})
      when is_binary(payload) do
    with true <- byte_size(payload) <= Limits.wire_bytes(module),
         {:ok, binary} <- Base.decode64(payload),
         true <- byte_size(binary) <= module.payload_size_limit(),
         {:ok, fields} <- decode_uncompressed_term(binary),
         true <- is_map(fields),
         struct <- struct(module, fields),
         :ok <- module.validate(struct) do
      {:ok, struct}
    else
      _other -> {:error, :invalid_runner_task_payload}
    end
  rescue
    _error -> {:error, :invalid_runner_task_payload}
  end

  def decode_for(_module, _tag, _value), do: {:error, :invalid_runner_task_envelope}

  defp decode_uncompressed_term(<<131, 80, _rest::binary>>),
    do: {:error, :compressed_runner_task_payload_not_allowed}

  defp decode_uncompressed_term(<<131, _rest::binary>> = binary),
    do: {:ok, :erlang.binary_to_term(binary, [:safe])}

  defp decode_uncompressed_term(_binary), do: {:error, :invalid_runner_task_payload}
end

defmodule Favn.Contracts.RunnerTask.Message do
  @moduledoc false

  defmacro __using__(opts) do
    fields = Keyword.fetch!(opts, :fields)
    required = Keyword.get(opts, :required, [])
    enums = Keyword.get(opts, :enums, [])
    limit = Keyword.get(opts, :limit, 1_048_576)
    session_fenced? = Keyword.get(opts, :session_fenced, false)
    tag = Keyword.fetch!(opts, :tag)

    quote bind_quoted: [
            fields: fields,
            required: required,
            enums: enums,
            limit: limit,
            session_fenced?: session_fenced?,
            tag: tag
          ] do
      @runner_task_required required
      @runner_task_enums enums
      @runner_task_limit limit
      @runner_task_session_fenced session_fenced?
      @runner_task_tag tag
      @enforce_keys required
      defstruct [version: 13] ++ fields
      @type t :: %__MODULE__{}

      @doc false
      def validate(%__MODULE__{} = value),
        do:
          Favn.Contracts.RunnerTask.Contract.validate(
            value,
            @runner_task_required,
            @runner_task_enums,
            @runner_task_limit,
            @runner_task_session_fenced
          )

      def validate(value), do: {:error, {:invalid_runner_task_message, __MODULE__, value}}

      @doc false
      def redact(%__MODULE__{} = value), do: Favn.Contracts.RunnerTask.Contract.redact(value)

      @doc false
      def payload_size_limit, do: @runner_task_limit

      @doc false
      def encode(%__MODULE__{} = value),
        do: Favn.Contracts.RunnerTask.Codec.encode(value, @runner_task_tag, &validate/1)

      @doc false
      def decode(value),
        do: Favn.Contracts.RunnerTask.Codec.decode_for(__MODULE__, @runner_task_tag, value)

      defoverridable validate: 1
    end
  end
end

defmodule Favn.Contracts.RunnerTask.Registration do
  use Favn.Contracts.RunnerTask.Message,
    tag: "registration",
    fields: [
      runner_instance_id: nil,
      boot_id: nil,
      runner_session_generation: 0,
      beam_node: nil,
      runner_pool: nil,
      required_runner_release_id: nil,
      protocol_version: 13,
      slots: 1,
      lifecycle_mode: :elastic,
      supported_task_kinds: [],
      capabilities: [],
      active_assignment: nil
    ],
    required: [
      :runner_instance_id,
      :boot_id,
      :beam_node,
      :runner_pool,
      :required_runner_release_id,
      :lifecycle_mode,
      :supported_task_kinds
    ],
    enums: [protocol_version: [13], lifecycle_mode: [:elastic, :resident]]

  def validate(%__MODULE__{} = registration) do
    with :ok <- super(registration),
         :ok <- validate_active_assignment(registration.active_assignment) do
      :ok
    end
  end

  def validate(value), do: super(value)

  defp validate_active_assignment(nil), do: :ok

  defp validate_active_assignment(%{
         workspace_id: workspace_id,
         task_id: task_id,
         assignment_generation: generation
       })
       when is_binary(workspace_id) and byte_size(workspace_id) in 1..255 and
              is_binary(task_id) and byte_size(task_id) in 1..255 and
              is_integer(generation) and generation > 0,
       do: :ok

  defp validate_active_assignment(value),
    do: {:error, {:invalid_runner_task_active_assignment, value}}
end

defmodule Favn.Contracts.RunnerTask.RegistrationAck do
  use Favn.Contracts.RunnerTask.Message,
    tag: "registration_ack",
    fields: [
      runner_instance_id: nil,
      runner_session_generation: 0,
      status: :accepted,
      reason: nil
    ],
    required: [:runner_instance_id],
    enums: [status: [:accepted, :rejected]]

  def validate(%__MODULE__{status: :accepted} = acknowledgement) do
    with :ok <- super(acknowledgement),
         true <- acknowledgement.runner_session_generation > 0 do
      :ok
    else
      false ->
        {:error, {:invalid_runner_task_session_fence, acknowledgement.runner_session_generation}}

      {:error, _reason} = error ->
        error
    end
  end

  def validate(%__MODULE__{} = acknowledgement), do: super(acknowledgement)
  def validate(value), do: super(value)
end

defmodule Favn.Contracts.RunnerTask.ClaimRequest do
  use Favn.Contracts.RunnerTask.Message,
    tag: "claim_request",
    session_fenced: true,
    fields: [
      command_id: nil,
      issued_at: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      runner_pool: nil,
      required_runner_release_id: nil,
      supported_task_kinds: [],
      capabilities: []
    ],
    required: [
      :command_id,
      :issued_at,
      :runner_instance_id,
      :runner_pool,
      :required_runner_release_id,
      :supported_task_kinds
    ]
end

defmodule Favn.Contracts.RunnerTask.Assignment do
  use Favn.Contracts.RunnerTask.Message,
    tag: "assignment",
    limit: Favn.Contracts.RunnerTask.Limits.assignment_bytes(),
    session_fenced: true,
    fields: [
      command_id: nil,
      workspace_id: nil,
      task_id: nil,
      task_kind: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      runner_pool: nil,
      required_runner_release_id: nil,
      assigned_at: nil,
      lease_expires_at: nil,
      retry_class: nil,
      payload: nil
    ],
    required: [
      :command_id,
      :workspace_id,
      :task_id,
      :task_kind,
      :runner_instance_id,
      :runner_pool,
      :required_runner_release_id,
      :assigned_at,
      :lease_expires_at,
      :retry_class,
      :payload
    ],
    enums: [
      task_kind: Favn.Contracts.RunnerTask.task_kinds(),
      retry_class: Favn.Contracts.RunnerTask.retry_classes()
    ]

  def validate(%__MODULE__{} = assignment) do
    with :ok <- super(assignment),
         :ok <-
           Favn.Contracts.RunnerTask.validate_payload(
             assignment.task_kind,
             assignment.payload
           ),
         :ok <-
           Favn.Contracts.RunnerTask.Limits.validate_payload(
             assignment.task_kind,
             assignment.payload
           ),
         :ok <- validate_asset_lease(assignment.task_kind, assignment.payload),
         :ok <- validate_asset_binding(assignment) do
      :ok
    end
  end

  def validate(value), do: super(value)

  defp validate_asset_lease(:asset_attempt, %Favn.Contracts.RunnerWork{manifest_lease_id: nil}),
    do: :ok

  defp validate_asset_lease(:asset_attempt, _payload),
    do: {:error, :runner_task_manifest_lease_not_allowed}

  defp validate_asset_lease(_kind, _payload), do: :ok

  defp validate_asset_binding(%__MODULE__{
         task_kind: :asset_attempt,
         runner_pool: pool,
         required_runner_release_id: release,
         payload: %Favn.Contracts.RunnerWork{} = work
       }) do
    with {:ok, work_pool} <- Favn.RunnerPool.encode(work.runner_pool),
         true <- work_pool == pool,
         true <- work.required_runner_release_id == release do
      :ok
    else
      _other -> {:error, :runner_task_asset_binding_mismatch}
    end
  end

  defp validate_asset_binding(_assignment), do: :ok
end

defmodule Favn.Contracts.RunnerTask.NoWork do
  @max_wait_ms 3_600_000

  use Favn.Contracts.RunnerTask.Message,
    tag: "no_work",
    session_fenced: true,
    fields: [
      command_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      action: :wait,
      wait_ms: 0
    ],
    required: [:command_id, :runner_instance_id],
    enums: [action: [:wait, :stop]]

  def validate(%__MODULE__{} = no_work) do
    with :ok <- super(no_work),
         true <-
           is_integer(no_work.wait_ms) and no_work.wait_ms >= 0 and
             no_work.wait_ms <= @max_wait_ms do
      :ok
    else
      false -> {:error, {:invalid_runner_task_wait_ms, no_work.wait_ms}}
      {:error, _reason} = error -> error
    end
  end

  def validate(value), do: super(value)
end

defmodule Favn.Contracts.RunnerTask.Wake do
  use Favn.Contracts.RunnerTask.Message,
    tag: "wake",
    session_fenced: true,
    fields: [
      runner_instance_id: nil,
      runner_session_generation: 0,
      runner_pool: nil,
      required_runner_release_id: nil
    ],
    required: [:runner_instance_id, :runner_pool, :required_runner_release_id]
end

defmodule Favn.Contracts.RunnerTask.Started do
  use Favn.Contracts.RunnerTask.Message,
    tag: "started",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      issued_at: nil,
      occurred_at: nil
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :issued_at, :occurred_at]
end

defmodule Favn.Contracts.RunnerTask.LeaseRenewal do
  use Favn.Contracts.RunnerTask.Message,
    tag: "lease_renewal",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      lease_expires_at: nil
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :lease_expires_at]
end

defmodule Favn.Contracts.RunnerTask.RuntimeInputsResolved do
  use Favn.Contracts.RunnerTask.Message,
    tag: "runtime_inputs_resolved",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      resolution_id: nil,
      issued_at: nil,
      status: :resolved,
      runtime_inputs: nil,
      error: nil
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :resolution_id, :issued_at],
    enums: [status: [:resolved, :failed]]

  def validate(%__MODULE__{status: :resolved} = message) do
    with :ok <- super(message),
         %Favn.RuntimeInput.Resolution{} = resolution <- message.runtime_inputs,
         nil <- message.error,
         :ok <- Favn.RuntimeInput.Resolution.validate(resolution) do
      :ok
    else
      _reason -> {:error, {:invalid_runtime_inputs_resolved, :resolved}}
    end
  end

  def validate(%__MODULE__{status: :failed} = message) do
    with :ok <- super(message),
         nil <- message.runtime_inputs,
         %Favn.Contracts.RunnerError{} = error <- message.error,
         :ok <- Favn.Contracts.RunnerError.validate(error) do
      :ok
    else
      _reason -> {:error, {:invalid_runtime_inputs_resolved, :failed}}
    end
  end

  def validate(value), do: super(value)
end

defmodule Favn.Contracts.RunnerTask.RuntimeInputsAck do
  use Favn.Contracts.RunnerTask.Message,
    tag: "runtime_inputs_ack",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      resolution_id: nil,
      payload_fingerprint: nil,
      status: :persisted
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :resolution_id],
    enums: [status: [:persisted, :stale, :rejected]]

  def validate(%__MODULE__{payload_fingerprint: fingerprint} = message)
      when is_nil(fingerprint) or is_binary(fingerprint),
      do: super(message)

  def validate(%__MODULE__{}), do: {:error, {:invalid_field, :payload_fingerprint}}
  def validate(value), do: super(value)
end

defmodule Favn.Contracts.RunnerTask.LogBatch do
  use Favn.Contracts.RunnerTask.Message,
    tag: "log_batch",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      batch_id: nil,
      issued_at: nil,
      sequence: 0,
      entries: [],
      truncated?: false
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :batch_id, :issued_at],
    limit: 262_144
end

defmodule Favn.Contracts.RunnerTask.LogAck do
  use Favn.Contracts.RunnerTask.Message,
    tag: "log_ack",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      batch_id: nil,
      sequence: 0
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :batch_id]
end

defmodule Favn.Contracts.RunnerTask.Result do
  use Favn.Contracts.RunnerTask.Message,
    tag: "result",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      task_kind: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      result_version: 1,
      outcome: nil,
      retry_class: nil,
      result: nil,
      error: nil,
      finished_at: nil
    ],
    required: [
      :workspace_id,
      :task_id,
      :task_kind,
      :runner_instance_id,
      :outcome,
      :retry_class,
      :finished_at
    ],
    enums: [
      task_kind: Favn.Contracts.RunnerTask.task_kinds(),
      outcome: Favn.Contracts.RunnerTask.terminal_outcomes(),
      retry_class: Favn.Contracts.RunnerTask.retry_classes()
    ]

  def validate(%__MODULE__{} = result) do
    with :ok <- super(result),
         :ok <-
           Favn.Contracts.RunnerTask.validate_result(
             result.task_kind,
             result.outcome,
             result.result
           ),
         :ok <- validate_error(result.outcome, result.error),
         :ok <-
           Favn.Contracts.RunnerTask.validate_terminal_retry(
             result.task_kind,
             result.outcome,
             result.retry_class,
             result.error
           ) do
      :ok
    end
  end

  def validate(value), do: super(value)

  defp validate_error(:succeeded, nil), do: :ok
  defp validate_error(:cancelled, nil), do: :ok

  defp validate_error(outcome, %Favn.Contracts.RunnerError{} = error)
       when outcome in [:failed, :cancelled, :unknown],
       do: Favn.Contracts.RunnerError.validate(error)

  defp validate_error(outcome, error),
    do: {:error, {:invalid_runner_task_error, outcome, error}}
end

defmodule Favn.Contracts.RunnerTask.ResultAck do
  use Favn.Contracts.RunnerTask.Message,
    tag: "result_ack",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      result_version: 1,
      status: :persisted
    ],
    required: [:workspace_id, :task_id, :runner_instance_id],
    enums: [status: [:persisted, :stale, :rejected]]
end

defmodule Favn.Contracts.RunnerTask.Cancellation do
  use Favn.Contracts.RunnerTask.Message,
    tag: "cancellation",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      command_id: nil,
      reason: nil,
      requested_at: nil
    ],
    required: [:workspace_id, :task_id, :runner_instance_id, :command_id, :requested_at]
end

defmodule Favn.Contracts.RunnerTask.CancellationAck do
  use Favn.Contracts.RunnerTask.Message,
    tag: "cancellation_ack",
    session_fenced: true,
    fields: [
      workspace_id: nil,
      task_id: nil,
      runner_instance_id: nil,
      runner_session_generation: 0,
      assignment_generation: 0,
      command_id: nil,
      status: :observed,
      issued_at: nil,
      acknowledged_at: nil
    ],
    required: [
      :workspace_id,
      :task_id,
      :runner_instance_id,
      :command_id,
      :issued_at,
      :acknowledged_at
    ],
    enums: [status: [:observed, :stale, :rejected]]
end

defmodule Favn.Contracts.RunnerTask.Shutdown do
  use Favn.Contracts.RunnerTask.Message,
    tag: "shutdown",
    session_fenced: true,
    fields: [
      runner_instance_id: nil,
      runner_session_generation: 0,
      action: :stop,
      wait_ms: 0,
      reason: nil
    ],
    required: [:runner_instance_id],
    enums: [action: [:wait, :stop]]
end
