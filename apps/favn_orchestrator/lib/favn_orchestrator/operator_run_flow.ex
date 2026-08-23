defmodule FavnOrchestrator.OperatorRunFlow do
  @moduledoc """
  Bounded operator read model for one exact run's Flow screen.

  Pages contain only the fields rendered by Flow. Complete attempt diagnostics
  are available through `attempt/4`, never as part of a page.
  """

  require Logger

  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Error
  alias FavnOrchestrator.Persistence.Queries.GetRunAssetAttempt
  alias FavnOrchestrator.Persistence.Queries.GetRunFlowPage
  alias FavnOrchestrator.Persistence.Queries.GetRunFlowDelta
  alias FavnOrchestrator.Persistence.Results.RunAssetAttempt
  alias FavnOrchestrator.Persistence.Results.RunFlowPage
  alias FavnOrchestrator.Persistence.Results.RunFlowDelta
  alias FavnOrchestrator.Persistence.Results.RunFlowStep
  alias FavnOrchestrator.Persistence.WorkspaceContext

  @max_limit 200
  @max_prefix_bytes 128
  @cursor_version 1

  defmodule Step do
    @moduledoc "One lean Flow row."
    @enforce_keys [:run_id, :asset_step_id, :asset_ref, :status]
    defstruct [
      :run_id,
      :asset_step_id,
      :target_id,
      :asset_ref,
      :display_name,
      :status,
      :stage,
      :window_kind,
      :window_start_at,
      :window_end_at,
      :window_timezone,
      :started_at,
      :finished_at,
      :duration_ms,
      :attempt_number,
      :execution_pool,
      :queue_reason,
      :failure_summary
    ]

    @type t :: %__MODULE__{}
  end

  defmodule Header do
    @moduledoc "One compact exact-run header with exact lifecycle counts."
    @enforce_keys [:run_id, :root_run_id, :status, :counts, :projection_cursor]
    defstruct [
      :run_id,
      :root_run_id,
      :parent_run_id,
      :rerun_of_run_id,
      :manifest_version_id,
      :status,
      :trigger_type,
      :started_at,
      :updated_at,
      :finished_at,
      :target_id,
      :target_label,
      :counts,
      :filtered_total,
      :unfiltered_total,
      :projection_cursor,
      :window_counts,
      :window_failure_total,
      :window_failures,
      :cancellable?,
      :retry_remaining?
    ]

    @type t :: %__MODULE__{}
  end

  defmodule Page do
    @moduledoc "A bounded exact-run Flow page with opaque navigation cursors."
    @enforce_keys [:header, :items, :has_next?, :has_previous?]
    defstruct [
      :header,
      :items,
      :next_cursor,
      :previous_cursor,
      :asset_prefix,
      :has_next?,
      :has_previous?
    ]

    @type t :: %__MODULE__{}
  end

  defmodule Attempt do
    @moduledoc "One keyed asset-step detail."
    @enforce_keys [:summary]
    defstruct [:summary, :error, :output_metadata, :window]

    @type t :: %__MODULE__{}
  end

  defmodule DeltaPage do
    @moduledoc "Changed retained rows under one frozen projection watermark."
    @enforce_keys [:header, :items, :through_publication_id, :has_more?]
    defstruct @enforce_keys ++ [:next_cursor]
    @type t :: %__MODULE__{}
  end

  @doc "Returns one exact-run Flow page under an already authorized workspace."
  @spec page(WorkspaceContext.t(), String.t(), keyword()) ::
          {:ok, Page.t()} | {:error, atom()}
  def page(%WorkspaceContext{} = context, run_id, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, normalized} <- normalize_page_opts(context, run_id, opts),
         {:ok, %RunFlowPage{} = page} <-
           Persistence.stores().operator_reads.get_run_flow_page(%GetRunFlowPage{
             workspace_context: context,
             run_id: run_id,
             asset_prefix: normalized.asset_prefix,
             after: normalized.after,
             before: normalized.before,
             limit: normalized.limit
           }) do
      public = public_page(page, context.workspace_id, run_id, normalized.asset_prefix)
      emit_payload_budget(:flow_page, public, 1_048_576)
      {:ok, public}
    else
      {:error, %Error{kind: kind}} -> {:error, kind}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Returns exactly one asset-step detail under an already authorized workspace."
  @spec attempt(WorkspaceContext.t(), String.t(), String.t()) ::
          {:ok, Attempt.t()} | {:error, atom()}
  def attempt(%WorkspaceContext{} = context, run_id, asset_step_id)
      when is_binary(run_id) and is_binary(asset_step_id) do
    case Persistence.stores().operator_reads.get_run_asset_attempt(%GetRunAssetAttempt{
           workspace_context: context,
           run_id: run_id,
           asset_step_id: asset_step_id
         }) do
      {:ok, %RunAssetAttempt{} = detail} ->
        {:ok,
         %Attempt{
           summary: public_step(detail.summary),
           error: detail.error,
           output_metadata: detail.output_metadata,
           window: detail.window
         }}

      {:error, %Error{kind: kind}} ->
        {:error, kind}
    end
  end

  @doc "Returns changed rows only for the caller's bounded retained asset-step IDs."
  @spec delta(
          WorkspaceContext.t(),
          String.t(),
          [String.t()],
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          {:ok, DeltaPage.t()} | {:error, atom()}
  def delta(%WorkspaceContext{} = context, run_id, asset_step_ids, acknowledged, through, opts)
      when is_binary(run_id) and is_list(asset_step_ids) and is_integer(acknowledged) and
             is_integer(through) and is_list(opts) do
    with true <-
           length(asset_step_ids) <= 500 and
             Enum.all?(asset_step_ids, &valid_cursor_value?(&1, 255)),
         {:ok, prefix} <- normalize_prefix(Keyword.get(opts, :asset_prefix)),
         {:ok, after_cursor} <- decode_delta_cursor(Keyword.get(opts, :after)),
         :ok <- validate_limit(Keyword.get(opts, :limit, @max_limit)),
         {:ok, %RunFlowDelta{} = page} <-
           Persistence.stores().operator_reads.get_run_flow_delta(%GetRunFlowDelta{
             workspace_context: context,
             run_id: run_id,
             asset_step_ids: asset_step_ids,
             asset_prefix: prefix,
             after_publication_id: acknowledged,
             through_publication_id: through,
             after: after_cursor,
             limit: Keyword.get(opts, :limit, @max_limit)
           }) do
      public =
        %DeltaPage{
          header: public_header(page.header),
          items: Enum.map(page.items, &public_step/1),
          through_publication_id: page.through_publication_id,
          has_more?: page.has_more?,
          next_cursor: page.next_cursor && encode_delta_cursor(page.next_cursor)
        }

      emit_payload_budget(:flow_delta, public, 1_048_576)
      {:ok, public}
    else
      false -> {:error, :invalid_scope}
      {:error, %Error{kind: kind}} -> {:error, kind}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_page_opts(context, run_id, opts) do
    limit = Keyword.get(opts, :limit, @max_limit)

    with {:ok, prefix} <- normalize_prefix(Keyword.get(opts, :asset_prefix)),
         :ok <- validate_limit(limit),
         :ok <- validate_cursor_shape(opts),
         {:ok, after_cursor} <- decode_optional_cursor(opts[:after], context, run_id, prefix),
         {:ok, before_cursor} <- decode_optional_cursor(opts[:before], context, run_id, prefix) do
      {:ok, %{asset_prefix: prefix, limit: limit, after: after_cursor, before: before_cursor}}
    end
  end

  defp normalize_prefix(nil), do: {:ok, nil}

  defp normalize_prefix(prefix) when is_binary(prefix) do
    prefix = String.trim(prefix)

    cond do
      not String.valid?(prefix) -> {:error, :invalid_filter}
      byte_size(prefix) > @max_prefix_bytes -> {:error, :invalid_filter}
      prefix == "" -> {:ok, nil}
      true -> {:ok, prefix}
    end
  end

  defp normalize_prefix(_prefix), do: {:error, :invalid_filter}

  defp validate_limit(limit) when is_integer(limit) and limit in 1..@max_limit, do: :ok
  defp validate_limit(_limit), do: {:error, :invalid_limit}

  defp validate_cursor_shape(opts) do
    if opts[:after] && opts[:before], do: {:error, :invalid_cursor}, else: :ok
  end

  defp decode_optional_cursor(nil, _context, _run_id, _prefix), do: {:ok, nil}

  defp decode_optional_cursor(cursor, context, run_id, prefix) when is_binary(cursor) do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false),
         {:ok, value} <- Jason.decode(json),
         %{
           "v" => @cursor_version,
           "run" => ^run_id,
           "filter" => cursor_filter,
           "asset_ref" => asset_ref,
           "asset_step_id" => asset_step_id,
           "fingerprint" => fingerprint
         } <- value,
         true <- cursor_filter == prefix,
         true <- fingerprint == cursor_fingerprint(context.workspace_id, run_id, prefix),
         true <- valid_cursor_value?(asset_ref, 1_024),
         true <- valid_cursor_value?(asset_step_id, 255) do
      {:ok, %{asset_ref: asset_ref, asset_step_id: asset_step_id}}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp decode_optional_cursor(_cursor, _context, _run_id, _prefix),
    do: {:error, :invalid_cursor}

  defp decode_delta_cursor(nil), do: {:ok, nil}

  defp decode_delta_cursor(cursor) do
    with {:ok, json} <- Base.url_decode64(cursor, padding: false),
         {:ok, %{"publication" => publication, "step" => step}} <- Jason.decode(json),
         true <- is_integer(publication) and publication >= 0,
         true <- valid_cursor_value?(step, 255) do
      {:ok, %{source_publication_id: publication, asset_step_id: step}}
    else
      _invalid -> {:error, :invalid_cursor}
    end
  end

  defp encode_delta_cursor(cursor) do
    %{"publication" => cursor.source_publication_id, "step" => cursor.asset_step_id}
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp public_page(page, workspace_id, run_id, prefix) do
    items = Enum.map(page.items, &public_step/1)
    first = List.first(page.items)
    last = List.last(page.items)

    %Page{
      header: public_header(page.header),
      items: items,
      asset_prefix: prefix,
      has_next?: page.has_next?,
      has_previous?: page.has_previous?,
      next_cursor: last && encode_cursor(last, workspace_id, run_id, prefix),
      previous_cursor: first && encode_cursor(first, workspace_id, run_id, prefix)
    }
  end

  defp public_step(%RunFlowStep{} = step) do
    step
    |> Map.from_struct()
    |> Map.drop([:source_publication_id])
    |> Map.put(:display_name, display_name(step.asset_ref))
    |> then(&struct(Step, &1))
  end

  defp public_header(header) do
    active? = header.status in [:pending, :running]

    header
    |> Map.from_struct()
    |> Map.put(:cancellable?, active?)
    |> Map.put(:retry_remaining?, not active? and header.counts.failed > 0)
    |> then(&struct(Header, &1))
  end

  defp encode_cursor(step, workspace_id, run_id, prefix) do
    Jason.encode!(%{
      "v" => @cursor_version,
      "run" => run_id,
      "filter" => prefix,
      "asset_ref" => step.asset_ref,
      "asset_step_id" => step.asset_step_id,
      "fingerprint" => cursor_fingerprint(workspace_id, run_id, prefix)
    })
    |> Base.url_encode64(padding: false)
  end

  defp cursor_fingerprint(workspace_id, run_id, prefix) do
    :crypto.hash(:sha256, [workspace_id, 0, run_id, 0, prefix || ""])
    |> Base.url_encode64(padding: false)
  end

  defp valid_cursor_value?(value, max),
    do: is_binary(value) and value != "" and byte_size(value) <= max

  defp emit_payload_budget(use_case, value, budget) do
    bytes = :erlang.external_size(value)

    :telemetry.execute(
      [:favn, :orchestrator, :operator_read, :payload],
      %{bytes: bytes, budget_bytes: budget},
      %{use_case: use_case, exceeded?: bytes > budget}
    )

    if bytes > budget do
      Logger.warning(
        "operator read payload budget exceeded use_case=#{use_case} budget_bytes=#{budget} observed_bytes=#{bytes}"
      )
    end

    :ok
  end

  defp display_name(asset_ref) do
    asset_ref
    |> String.split(":")
    |> List.last()
  end
end
