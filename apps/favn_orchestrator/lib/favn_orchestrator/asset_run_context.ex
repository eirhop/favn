defmodule FavnOrchestrator.AssetRunContext do
  @moduledoc """
  Manifest-pinned operational context for running one asset through a pipeline.

  An asset can be selected by multiple pipelines with different window policies
  and schedule timezones. This contract gives catalogue reads and operator
  commands one stable context id so they cannot silently choose a pipeline by
  manifest ordering.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.Version
  alias Favn.Window.Anchor
  alias Favn.Window.Policy
  alias Favn.Window.Selection
  alias FavnOrchestrator.ManifestIndexCache
  alias FavnOrchestrator.ManifestTarget

  @default_timezone "Etc/UTC"

  @enforce_keys [:id, :pipeline_ref, :pipeline, :index, :timezone]
  defstruct [:id, :pipeline_ref, :pipeline, :index, :policy, :schedule_timezone, :timezone]

  @type t :: %__MODULE__{
          id: String.t(),
          pipeline_ref: {module(), atom()},
          pipeline: Pipeline.t(),
          index: Index.t(),
          policy: Policy.t() | nil,
          schedule_timezone: String.t() | nil,
          timezone: String.t()
        }

  @type status :: :selected | :ambiguous | :unavailable
  @type selection :: %{
          required(:contexts) => [t()],
          required(:selected) => t() | nil,
          required(:status) => status()
        }

  @doc "Returns every stable pipeline context selecting the asset, sorted by id."
  @spec list(Version.t(), Asset.t() | Favn.Ref.t()) :: {:ok, [t()]} | {:error, term()}
  def list(%Version{} = version, %Asset{ref: asset_ref}), do: list(version, asset_ref)

  def list(%Version{} = version, asset_ref) when is_tuple(asset_ref) do
    with {:ok, %Index{} = index} <- ManifestIndexCache.fetch(version) do
      build_contexts(index, List.wrap(version.manifest.pipelines), asset_ref)
    end
  end

  @doc "Selects an explicit context, auto-selects a unique context, or reports ambiguity."
  @spec select(Version.t(), Asset.t() | Favn.Ref.t(), String.t() | nil) ::
          {:ok, selection()} | {:error, term()}
  def select(%Version{} = version, asset, context_id \\ nil) do
    with {:ok, contexts} <- list(version, asset) do
      select_from(contexts, context_id)
    end
  end

  @doc "Resolves the context policy's scheduled anchor at an occurrence time."
  @spec anchor(t(), DateTime.t()) :: {:ok, Anchor.t()} | {:error, term()}
  def anchor(%__MODULE__{policy: %Policy{} = policy} = context, %DateTime{} = due_at) do
    Policy.resolve_scheduled(policy, due_at, context.schedule_timezone)
  end

  def anchor(%__MODULE__{}, %DateTime{}), do: {:error, :asset_run_context_has_no_window_policy}

  @doc "Resolves the exact or scheduled selection represented by this run context."
  @spec selection(t(), DateTime.t()) :: {:ok, Selection.t()} | {:error, term()}
  def selection(
        %__MODULE__{policy: %Policy{}, pipeline: %Pipeline{schedule: nil}} = context,
        %DateTime{} = due_at
      ) do
    with {:ok, anchor} <- anchor(context, due_at) do
      Selection.manual(anchor, anchor.timezone)
    end
  end

  def selection(%__MODULE__{policy: %Policy{} = policy} = context, %DateTime{} = due_at) do
    Policy.select_scheduled(policy, due_at, context.schedule_timezone)
  end

  def selection(%__MODULE__{}, %DateTime{}),
    do: {:error, :asset_run_context_has_no_window_policy}

  @doc "Projects a context into the browser-safe operator DTO."
  @spec descriptor(t()) :: map()
  def descriptor(%__MODULE__{} = context) do
    %{
      id: context.id,
      label: context_label(context.pipeline_ref),
      pipeline_ref: pipeline_ref_string(context.pipeline_ref),
      policy: policy_descriptor(context.policy),
      timezone: context.timezone
    }
  end

  defp select_from(contexts, nil) do
    case contexts do
      [] -> {:ok, %{contexts: [], selected: nil, status: :unavailable}}
      [context] -> {:ok, %{contexts: contexts, selected: context, status: :selected}}
      contexts -> {:ok, %{contexts: contexts, selected: nil, status: :ambiguous}}
    end
  end

  defp select_from(contexts, context_id) when is_binary(context_id) and context_id != "" do
    case Enum.find(contexts, &(&1.id == context_id)) do
      %__MODULE__{} = context ->
        {:ok, %{contexts: contexts, selected: context, status: :selected}}

      nil ->
        {:error, :invalid_asset_run_context}
    end
  end

  defp select_from(_contexts, _context_id), do: {:error, :invalid_asset_run_context}

  defp build(%Index{} = index, %Pipeline{} = pipeline) do
    pipeline_ref = {pipeline.module, pipeline.name}
    schedule_timezone = schedule_timezone(index, pipeline)
    policy = pipeline.window

    %__MODULE__{
      id: ManifestTarget.pipeline_id(pipeline_ref),
      pipeline_ref: pipeline_ref,
      pipeline: pipeline,
      index: index,
      policy: policy,
      schedule_timezone: schedule_timezone,
      timezone: policy_timezone(policy) || schedule_timezone || @default_timezone
    }
  end

  defp build_contexts(index, pipelines, asset_ref) do
    pipelines
    |> Enum.reduce_while({:ok, []}, fn pipeline, {:ok, contexts} ->
      case PipelineResolver.resolve(index, pipeline, trigger: %{kind: :asset_run_context}) do
        {:ok, resolution} ->
          if asset_ref in resolution.target_refs do
            {:cont, {:ok, [build(index, pipeline) | contexts]}}
          else
            {:cont, {:ok, contexts}}
          end

        {:error, reason} ->
          {:halt, {:error, {:asset_run_context_resolution_failed, pipeline.name, reason}}}
      end
    end)
    |> then(fn
      {:ok, contexts} -> {:ok, Enum.sort_by(contexts, & &1.id)}
      {:error, _reason} = error -> error
    end)
  end

  defp schedule_timezone(index, %Pipeline{schedule: {:ref, ref}}) do
    case Index.fetch_schedule(index, ref) do
      {:ok, %Schedule{timezone: timezone}} -> timezone
      {:error, _reason} -> nil
    end
  end

  defp schedule_timezone(_index, %Pipeline{schedule: {:inline, %Schedule{} = schedule}}),
    do: schedule.timezone

  defp schedule_timezone(_index, %Pipeline{schedule: %Schedule{} = schedule}),
    do: schedule.timezone

  defp schedule_timezone(_index, _pipeline), do: nil

  defp policy_timezone(%Policy{timezone: timezone}), do: timezone
  defp policy_timezone(_policy), do: nil

  defp context_label({module, name}), do: "#{inspect(module)} / #{name}"
  defp pipeline_ref_string({module, name}), do: "#{Atom.to_string(module)}:#{name}"

  defp policy_descriptor(nil), do: nil

  defp policy_descriptor(%Policy{} = policy) do
    %{
      kind: policy.kind,
      anchor: policy.anchor,
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end
end
