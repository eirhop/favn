defmodule FavnOrchestrator.RunSubmission.Intent do
  @moduledoc """
  Versioned input consumed by asynchronous run-submission workers.

  The intent contains only the selector and planning options required to
  reproduce a submission against its frozen deployment and manifest. It uses
  the bounded allowlisted payload codec so tuples, existing atoms, dates, and
  approved value structs round-trip without accepting executable terms.
  """

  alias FavnOrchestrator.Storage.PayloadCodec

  @format "favn.run_submission.intent.v1"
  @maximum_payload_bytes 240_000
  @operations [:asset, :pipeline, :pipeline_assets, :rerun]
  @option_keys [
    :anchor_window,
    :dependencies,
    :exact_windows,
    :input_mode,
    :lineage_depth,
    :metadata,
    :params,
    :parent_run_id,
    :rebuild,
    :refresh,
    :refresh_policy,
    :replay_mode,
    :replay_node_keys,
    :required_generation,
    :retry_policy,
    :root_run_id,
    :target_refs,
    :timeout_ms,
    :trigger,
    :window_evaluated_at,
    :window_request,
    :window_selection
  ]

  @type operation :: :asset | :pipeline | :pipeline_assets | :rerun
  @type t :: map()

  @doc "Builds one canonical, round-trip-validated submission intent."
  @spec new(operation(), term(), keyword()) ::
          {:ok, t()} | {:error, :invalid_run_submission_intent}
  def new(operation, selector, opts)
      when operation in @operations and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- @option_keys,
         payload = %{operation: operation, selector: selector, options: opts},
         {:ok, encoded} <- PayloadCodec.encode(payload),
         true <- byte_size(encoded) <= @maximum_payload_bytes,
         {:ok, ^payload} <- PayloadCodec.decode(encoded) do
      {:ok, %{"format" => @format, "payload" => encoded}}
    else
      _invalid -> {:error, :invalid_run_submission_intent}
    end
  rescue
    _error -> {:error, :invalid_run_submission_intent}
  end

  def new(_operation, _selector, _opts), do: {:error, :invalid_run_submission_intent}

  @doc false
  @spec decode(t()) ::
          {:ok, {operation(), term(), keyword()}}
          | {:error, :invalid_run_submission_intent}
  def decode(%{"format" => @format, "payload" => payload})
      when is_binary(payload) and byte_size(payload) <= @maximum_payload_bytes do
    with {:ok, %{operation: operation, selector: selector, options: opts}}
         when operation in @operations and is_list(opts) <- PayloadCodec.decode(payload),
         true <- Keyword.keyword?(opts),
         [] <- Keyword.keys(opts) -- @option_keys do
      {:ok, {operation, selector, opts}}
    else
      _invalid -> {:error, :invalid_run_submission_intent}
    end
  rescue
    _error -> {:error, :invalid_run_submission_intent}
  end

  def decode(_intent), do: {:error, :invalid_run_submission_intent}
end
