defmodule Favn.Backfill do
  @moduledoc """
  Internal backfill helpers that extract and normalize range information.
  """

  alias Favn.Window.Anchor

  @type range :: %{
          kind: Anchor.kind(),
          start_at: DateTime.t(),
          end_at: DateTime.t(),
          timezone: String.t()
        }

  @doc """
  Extract and validate backfill range from opts, returning normalized range and expanded anchors.

  Returns `{:ok, normalized_range, anchor_ranges}` on success.
  """
  @spec fetch_range(keyword()) :: {:ok, range(), [Anchor.t()]} | {:error, term()}
  def fetch_range(opts) when is_list(opts) do
    case Keyword.fetch(opts, :range) do
      {:ok, %{kind: kind, start_at: %DateTime{} = start_at, end_at: %DateTime{} = end_at} = range} ->
        timezone = Map.get(range, :timezone, "Etc/UTC")
        normalized = %{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone}

        case Anchor.expand_range(kind, start_at, end_at, timezone: timezone) do
          {:ok, [_ | _] = anchor_ranges} -> {:ok, normalized, anchor_ranges}
          {:ok, []} -> {:error, :empty_backfill_range}
          {:error, _} = error -> error
        end

      {:ok, _invalid} ->
        {:error, :invalid_backfill_range}

      :error ->
        {:error, :backfill_range_required}
    end
  end

  @doc """
  Drop the range option from a keyword list.
  """
  @spec drop_range_opt(keyword()) :: keyword()
  def drop_range_opt(opts) when is_list(opts), do: Keyword.delete(opts, :range)

  @doc """
  Build pipeline context for a backfill run.
  """
  @spec build_pipeline_context(map(), range(), [Anchor.t()]) :: map()
  def build_pipeline_context(pipeline_ctx, range, anchor_ranges) do
    pipeline_ctx
    |> Map.put(:run_kind, :pipeline_backfill)
    |> Map.put(:backfill_range, range)
    |> Map.put(:anchor_ranges, anchor_ranges)
  end
end
