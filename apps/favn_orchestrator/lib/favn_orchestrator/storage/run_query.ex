defmodule FavnOrchestrator.Storage.RunQuery do
  @moduledoc false

  alias FavnOrchestrator.RunState

  @type metadata :: %{
          required(:root_execution_group_id) => String.t(),
          required(:parent_run_id) => String.t() | nil,
          required(:root_run_id) => String.t() | nil,
          required(:submit_kind) => String.t(),
          required(:trigger_type) => String.t(),
          required(:asset_ref_text) => String.t(),
          required(:target_refs_text) => String.t(),
          required(:window_key) => String.t() | nil,
          required(:pipeline_submit_ref_text) => String.t()
        }

  @spec metadata(RunState.t()) :: metadata()
  def metadata(%RunState{} = run) do
    targets = target_refs(run)

    %{
      root_execution_group_id: root_execution_group_id(run),
      parent_run_id: run.parent_run_id,
      root_run_id: run.root_run_id,
      submit_kind: Atom.to_string(run.submit_kind),
      trigger_type: Atom.to_string(trigger_type(run)),
      asset_ref_text: public_ref(run.asset_ref),
      target_refs_text: Enum.map_join(targets, "\n", &public_ref/1),
      window_key: window_key(run),
      pipeline_submit_ref_text: pipeline_submit_ref_text(run)
    }
  end

  @spec root_execution_group_id(RunState.t()) :: String.t()
  def root_execution_group_id(%RunState{root_run_id: root_run_id}) when is_binary(root_run_id),
    do: root_run_id

  def root_execution_group_id(%RunState{parent_run_id: parent_run_id})
      when is_binary(parent_run_id),
      do: parent_run_id

  def root_execution_group_id(%RunState{id: id}), do: id

  @spec public_ref(term()) :: String.t()
  def public_ref({module, name}), do: "#{module_label(module)}.#{name}"
  def public_ref(%{module: module, name: name}), do: "#{module_label(module)}.#{name}"
  def public_ref(%{"module" => module, "name" => name}), do: "#{module_label(module)}.#{name}"
  def public_ref(nil), do: "Unknown asset"
  def public_ref(ref) when is_atom(ref), do: ref |> Atom.to_string() |> strip_elixir_prefix()
  def public_ref(ref) when is_binary(ref), do: strip_elixir_prefix(ref)
  def public_ref(ref), do: inspect(ref)

  @spec target_refs(RunState.t()) :: [term()]
  def target_refs(%RunState{target_refs: [_ | _] = refs}), do: refs
  def target_refs(%RunState{asset_ref: ref}), do: [ref]

  @spec trigger_type(RunState.t()) :: atom()
  def trigger_type(%RunState{submit_kind: :rerun}), do: :retry

  def trigger_type(%RunState{submit_kind: submit_kind})
      when submit_kind in [:backfill_asset, :backfill_pipeline],
      do: :backfill

  def trigger_type(%RunState{trigger: trigger}) when is_map(trigger) do
    case map_get(trigger, :kind) do
      kind when is_atom(kind) and not is_nil(kind) -> kind
      "schedule" -> :schedule
      "manual" -> :manual
      "backfill" -> :backfill
      "retry" -> :retry
      _other -> :manual
    end
  end

  def trigger_type(_run), do: :manual

  defp module_label(module) when is_atom(module),
    do: module |> Atom.to_string() |> strip_elixir_prefix()

  defp module_label(module), do: module |> to_string() |> strip_elixir_prefix()

  defp strip_elixir_prefix("Elixir." <> module), do: module
  defp strip_elixir_prefix(module), do: module

  defp window_key(%RunState{} = run) do
    run.trigger
    |> map_get(:window_key)
    |> case do
      value when is_binary(value) -> value
      value when not is_nil(value) -> to_string(value)
      nil -> window_key_from_metadata(run.metadata)
    end
  end

  defp window_key_from_metadata(metadata) do
    metadata
    |> map_get(:pipeline_context)
    |> case do
      context when is_map(context) -> context |> map_get(:anchor_window) |> anchor_key()
      _other -> nil
    end
  end

  defp pipeline_submit_ref_text(%RunState{} = run) do
    run.metadata
    |> map_get(:pipeline_submit_ref)
    |> case do
      value when is_atom(value) or is_binary(value) -> public_ref(value)
      _other -> ""
    end
  end

  defp anchor_key(%{key: key}), do: inspect(key)
  defp anchor_key(%{"key" => key}), do: inspect(key)
  defp anchor_key(_value), do: nil

  defp map_get(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp map_get(_value, _key), do: nil
end
