defmodule FavnOrchestrator.Storage.MaterializationClaimCodec do
  @moduledoc false

  alias Favn.Window.Key, as: WindowKey
  alias FavnOrchestrator.MaterializationClaim
  alias FavnOrchestrator.Storage.JsonSafe

  @format "favn.materialization_claim.storage.v1"

  @spec normalize(MaterializationClaim.t() | map()) ::
          {:ok, MaterializationClaim.t()} | {:error, term()}
  def normalize(%MaterializationClaim{} = claim),
    do: MaterializationClaim.new(Map.from_struct(claim))

  def normalize(claim) when is_map(claim), do: MaterializationClaim.new(claim)
  def normalize(_claim), do: {:error, :invalid_materialization_claim}

  @spec encode(MaterializationClaim.t() | map()) :: {:ok, binary()} | {:error, term()}
  def encode(claim) do
    with {:ok, normalized} <- normalize(claim) do
      Jason.encode(to_dto(normalized))
    end
  rescue
    exception -> {:error, {:invalid_materialization_claim_payload, exception}}
  end

  @spec decode(binary()) :: {:ok, MaterializationClaim.t()} | {:error, term()}
  def decode(payload) when is_binary(payload) do
    decode_json_or_legacy(payload)
  rescue
    exception -> {:error, {:invalid_materialization_claim_payload, exception}}
  end

  def decode(_payload), do: {:error, :invalid_materialization_claim_payload}

  defp decode_json_or_legacy(payload) do
    case Jason.decode(payload) do
      {:ok, %{"format" => @format, "schema_version" => 1} = dto} ->
        from_dto(dto)

      {:ok, %{"format" => @format, "schema_version" => version}} ->
        {:error, {:unsupported_materialization_claim_schema_version, version}}

      {:ok, other} ->
        {:error, {:invalid_materialization_claim_dto, other}}

      {:error, reason} ->
        if json_like?(payload) do
          {:error, {:invalid_materialization_claim_json, reason}}
        else
          decode_legacy_payload(payload)
        end
    end
  end

  defp to_dto(%MaterializationClaim{} = claim) do
    %{
      "format" => @format,
      "schema_version" => 1,
      "claim_key" => claim.claim_key,
      "asset_ref_module" => Atom.to_string(claim.asset_ref_module),
      "asset_ref_name" => Atom.to_string(claim.asset_ref_name),
      "freshness_key" => claim.freshness_key,
      "input_fingerprint" => claim.input_fingerprint,
      "run_id" => claim.run_id,
      "asset_step_id" => claim.asset_step_id,
      "node_key" => node_key_to_dto(claim.node_key),
      "runner_execution_id" => claim.runner_execution_id,
      "manifest_version_id" => claim.manifest_version_id,
      "manifest_content_hash" => claim.manifest_content_hash,
      "freshness_version" => claim.freshness_version,
      "status" => Atom.to_string(claim.status),
      "error" => JsonSafe.error(claim.error),
      "claimed_at" => datetime_to_dto(claim.claimed_at),
      "heartbeat_at" => datetime_to_dto(claim.heartbeat_at),
      "expires_at" => datetime_to_dto(claim.expires_at),
      "finished_at" => datetime_to_dto(claim.finished_at),
      "metadata" => JsonSafe.data(claim.metadata || %{})
    }
  end

  defp from_dto(dto) do
    with {:ok, asset_ref_module} <- trusted_persisted_atom(Map.get(dto, "asset_ref_module")),
         {:ok, asset_ref_name} <- trusted_persisted_atom(Map.get(dto, "asset_ref_name")),
         {:ok, node_key} <- node_key_from_dto(Map.get(dto, "node_key")),
         {:ok, claimed_at} <- datetime(Map.get(dto, "claimed_at")),
         {:ok, heartbeat_at} <- optional_datetime(Map.get(dto, "heartbeat_at")),
         {:ok, expires_at} <- datetime(Map.get(dto, "expires_at")),
         {:ok, finished_at} <- optional_datetime(Map.get(dto, "finished_at")),
         {:ok, error} <- error_field(dto, "error"),
         {:ok, metadata} <- map_field(dto, "metadata") do
      MaterializationClaim.new(%{
        claim_key: Map.get(dto, "claim_key"),
        asset_ref_module: asset_ref_module,
        asset_ref_name: asset_ref_name,
        freshness_key: Map.get(dto, "freshness_key"),
        input_fingerprint: Map.get(dto, "input_fingerprint"),
        run_id: Map.get(dto, "run_id"),
        asset_step_id: Map.get(dto, "asset_step_id"),
        node_key: node_key,
        runner_execution_id: Map.get(dto, "runner_execution_id"),
        manifest_version_id: Map.get(dto, "manifest_version_id"),
        manifest_content_hash: Map.get(dto, "manifest_content_hash"),
        freshness_version: Map.get(dto, "freshness_version"),
        status: Map.get(dto, "status"),
        error: error,
        claimed_at: claimed_at,
        heartbeat_at: heartbeat_at,
        expires_at: expires_at,
        finished_at: finished_at,
        metadata: metadata
      })
    end
  end

  defp decode_legacy_payload(payload) do
    with {:ok, binary} <- Base.decode64(payload) do
      binary
      |> decode_trusted_legacy_term()
      |> normalize()
    else
      :error -> {:error, :invalid_materialization_claim_payload}
    end
  end

  defp json_like?(payload) do
    payload
    |> String.trim_leading()
    |> String.starts_with?(["{", "["])
  end

  defp node_key_to_dto(nil), do: nil

  defp node_key_to_dto(value) when is_binary(value) do
    %{"type" => "string", "value" => value}
  end

  defp node_key_to_dto({ref, identity}) do
    %{
      "type" => "asset_node",
      "ref" => ref_to_dto(ref),
      "identity" => node_identity_to_dto(identity)
    }
  end

  defp node_key_to_dto(value), do: %{"type" => "json", "value" => JsonSafe.data(value)}

  defp node_key_from_dto(nil), do: {:ok, nil}

  defp node_key_from_dto(%{"type" => "string", "value" => value}) when is_binary(value),
    do: {:ok, value}

  defp node_key_from_dto(%{"type" => "asset_node", "ref" => ref, "identity" => identity}) do
    with {:ok, decoded_ref} <- ref_from_dto(ref),
         {:ok, decoded_identity} <- node_identity_from_dto(identity) do
      {:ok, {decoded_ref, decoded_identity}}
    end
  end

  defp node_key_from_dto(%{"type" => "json", "value" => value}), do: {:ok, value}
  defp node_key_from_dto(value), do: {:error, {:invalid_node_key, value}}

  # Keep identity encoding local to this versioned DTO so the persisted shape can
  # evolve independently. If another storage codec needs this same shape, extract
  # a private orchestrator storage identity codec instead of copying it again.
  defp node_identity_to_dto(nil), do: nil

  defp node_identity_to_dto(%{kind: _kind, start_at_us: _start_at_us, timezone: _timezone} = key) do
    %{"type" => "window_key", "value" => WindowKey.encode(key)}
  end

  defp node_identity_to_dto(identity), do: %{"type" => "json", "value" => JsonSafe.data(identity)}

  defp node_identity_from_dto(nil), do: {:ok, nil}

  defp node_identity_from_dto(%{"type" => "window_key", "value" => value}),
    do: WindowKey.decode(value)

  defp node_identity_from_dto(%{"type" => "json", "value" => value}), do: {:ok, value}
  defp node_identity_from_dto(value), do: {:error, {:invalid_node_identity, value}}

  defp ref_to_dto({module, name}) when is_atom(module) and is_atom(name) do
    %{"module" => Atom.to_string(module), "name" => Atom.to_string(name)}
  end

  defp ref_to_dto(_value), do: nil

  defp ref_from_dto(%{"module" => module, "name" => name}) do
    with {:ok, module_atom} <- trusted_persisted_atom(module),
         {:ok, name_atom} <- trusted_persisted_atom(name) do
      {:ok, {module_atom, name_atom}}
    end
  end

  defp ref_from_dto(value), do: {:error, {:invalid_ref, value}}

  defp datetime_to_dto(nil), do: nil
  defp datetime_to_dto(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      {:error, reason} -> {:error, {:invalid_datetime, value, reason}}
    end
  end

  defp datetime(value), do: {:error, {:invalid_datetime, value}}

  defp optional_datetime(nil), do: {:ok, nil}
  defp optional_datetime(value), do: datetime(value)

  defp error_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) or is_nil(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end

  defp map_field(dto, field) do
    case Map.fetch(dto, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, value} -> {:error, {:invalid_dto_field, field, value}}
      :error -> {:error, {:missing_dto_field, field}}
    end
  end

  # Materialization claims are trusted orchestrator-owned durable state. The
  # current struct still requires atom refs, so JSON decoding recreates only
  # atoms read from this storage payload. The long-term fix is string-backed
  # persisted identities or a richer identity value object, not broader atom
  # recreation; external inputs must not call this path.
  defp trusted_persisted_atom(value) when is_binary(value), do: {:ok, String.to_atom(value)}
  defp trusted_persisted_atom(value), do: {:error, {:invalid_atom, value}}

  # Claims are internal durable state written by storage adapters, not external
  # input. Older claim payloads used ETF and may contain consumer module atoms
  # before those modules are loaded; keep this trusted compatibility path only
  # until legacy claim payloads have aged out or been rewritten.
  defp decode_trusted_legacy_term(binary) when is_binary(binary) do
    :erlang.binary_to_term(binary, [:safe])
  rescue
    ArgumentError -> :erlang.binary_to_term(binary)
  end
end
