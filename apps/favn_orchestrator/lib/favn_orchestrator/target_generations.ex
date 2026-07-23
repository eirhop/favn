defmodule FavnOrchestrator.TargetGenerations do
  @moduledoc """
  Resolves manifest assets to the evidence generation used by planning and writes.

  Non-persisted assets use their deterministic semantic generation. Persisted
  SQL targets read or establish a durable target generation through the
  generation store.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.TargetDescriptor
  alias Favn.Manifest.Version
  alias Favn.Plan
  alias FavnOrchestrator.Persistence
  alias FavnOrchestrator.Persistence.Commands.EnsureWritableTargetGeneration
  alias FavnOrchestrator.Persistence.Queries.GetTargetBindings
  alias FavnOrchestrator.Persistence.TargetIdentity
  alias FavnOrchestrator.Persistence.WorkspaceContext
  alias FavnOrchestrator.TargetAdmission

  @type identity :: %{
          required(:target_id) => String.t(),
          required(:evidence_generation_id) => String.t(),
          required(:target_generation_id) => String.t() | nil,
          optional(:physical_relation) => map() | nil
        }

  @binding_batch 500

  @doc "Resolves one asset generation before an ordinary materialization claim."
  @spec for_write(WorkspaceContext.t(), Version.t(), Asset.t(), DateTime.t()) ::
          {:ok, identity()} | {:error, term()}
  def for_write(
        %WorkspaceContext{} = context,
        %Version{} = version,
        %Asset{} = asset,
        %DateTime{} = occurred_at
      ) do
    writable_identity(context, version, asset, occurred_at)
  end

  @doc "Pins one generation identity for every unique asset in a normal run plan."
  @spec pin_plan(WorkspaceContext.t(), Version.t(), Index.t(), Plan.t(), DateTime.t()) ::
          {:ok, Plan.t()} | {:error, term()}
  def pin_plan(
        %WorkspaceContext{} = context,
        %Version{} = version,
        %Index{} = index,
        %Plan{} = plan,
        %DateTime{} = occurred_at
      ),
      do: pin_plan(context, version, index, plan, occurred_at, [])

  @doc "Pins plan generations and optionally requires one exact output generation."
  @spec pin_plan(
          WorkspaceContext.t(),
          Version.t(),
          Index.t(),
          Plan.t(),
          DateTime.t(),
          keyword()
        ) :: {:ok, Plan.t()} | {:error, term()}
  def pin_plan(
        %WorkspaceContext{} = context,
        %Version{} = version,
        %Index{} = index,
        %Plan{} = plan,
        %DateTime{} = occurred_at,
        opts
      ) do
    with :ok <- validate_pin_options(opts),
         :ok <- TargetAdmission.preflight(context, index, plan) do
      refs = plan.nodes |> Map.values() |> Enum.map(& &1.ref) |> Enum.uniq()

      with {:ok, identities} <- pin_identities(refs, context, version, index, occurred_at) do
        nodes =
          Map.new(plan.nodes, fn {node_key, node} ->
            identity = Map.fetch!(identities, node.ref)

            input_generations =
              node
              |> Map.get(:upstream, [])
              |> Enum.map(fn upstream_key ->
                upstream_ref = plan.nodes |> Map.fetch!(upstream_key) |> Map.fetch!(:ref)
                Map.fetch!(identities, upstream_ref)
              end)
              |> Enum.uniq_by(&{&1.target_id, &1.evidence_generation_id})
              |> Enum.sort_by(& &1.target_id)

            pinned =
              identity
              |> Map.take([
                :target_id,
                :target_generation_id,
                :evidence_generation_id,
                :physical_relation
              ])
              |> Map.put(:input_generations, input_generations)

            {node_key, Map.merge(node, pinned)}
          end)

        plan
        |> Map.put(:nodes, nodes)
        |> require_generation(Keyword.get(opts, :required_generation))
      end
    end
  end

  @doc "Pins one isolated rebuild-candidate target and its immutable active inputs."
  @spec pin_rebuild_plan(WorkspaceContext.t(), Version.t(), Index.t(), Plan.t(), map()) ::
          {:ok, Plan.t()} | {:error, term()}
  def pin_rebuild_plan(
        %WorkspaceContext{} = context,
        %Version{},
        %Index{} = index,
        %Plan{} = plan,
        rebuild
      )
      when is_map(rebuild) do
    with :ok <- validate_rebuild_pin(rebuild),
         :ok <- validate_rebuild_inputs(context, Map.fetch!(rebuild, :input_generations)),
         {:ok, asset} <- rebuild_asset(index, Map.fetch!(rebuild, :target_id)),
         :ok <- validate_rebuild_plan_target(plan, asset.ref) do
      nodes =
        Map.new(plan.nodes, fn {node_key, node} ->
          target_operation = Map.get(rebuild, :target_operation, :rebuild_candidate)

          pinned = %{
            target_id: rebuild.target_id,
            target_generation_id: rebuild.candidate_generation_id,
            evidence_generation_id: rebuild.candidate_generation_id,
            physical_relation: rebuild.candidate_relation,
            input_generations: rebuild.input_generations,
            target_operation: target_operation,
            active_relation: rebuild.active_relation,
            write_relation: rebuild.candidate_relation,
            rebuild_operation_id: rebuild.operation_id,
            rebuild_action_id: rebuild.action_id,
            rebuild_item_id: rebuild.item_id,
            rebuild_empty_generation: Map.get(rebuild, :empty_generation, false),
            rebuild_final_item: Map.get(rebuild, :final_item, false)
          }

          {node_key, Map.merge(node, pinned)}
        end)

      {:ok, %{plan | nodes: nodes}}
    end
  end

  def pin_rebuild_plan(_context, _version, _index, _plan, _rebuild),
    do: {:error, :invalid_rebuild_generation_pin}

  @doc "Resolves active evidence generations for a bounded manifest asset map."
  @spec for_reads(WorkspaceContext.t(), map()) ::
          {:ok, %{optional(Favn.Ref.t()) => identity()}} | {:error, term()}
  def for_reads(%WorkspaceContext{} = context, assets_by_ref) when is_map(assets_by_ref) do
    persisted =
      assets_by_ref
      |> Map.values()
      |> Enum.filter(&match?(%Asset{target_descriptor: %TargetDescriptor{}}, &1))

    bindings_by_target =
      case persisted do
        [] ->
          {:ok, %{}}

        assets ->
          assets
          |> Enum.map(&TargetIdentity.for_asset(&1.ref))
          |> Enum.uniq()
          |> Enum.chunk_every(@binding_batch)
          |> Enum.reduce_while({:ok, %{}}, fn target_ids, {:ok, acc} ->
            case Persistence.stores().target_generations.get_bindings(%GetTargetBindings{
                   workspace_context: context,
                   target_ids: target_ids
                 }) do
              {:ok, bindings} ->
                {:cont, {:ok, Enum.reduce(bindings, acc, &Map.put(&2, &1.target_id, &1))}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
      end

    with {:ok, bindings_by_target} <- bindings_by_target do
      identities =
        Enum.reduce(assets_by_ref, %{}, fn {ref, asset}, acc ->
          case read_identity(asset, bindings_by_target) do
            nil -> acc
            identity -> Map.put(acc, ref, identity)
          end
        end)

      {:ok, identities}
    end
  end

  defp writable_identity(
         _context,
         %Version{} = version,
         %Asset{target_descriptor: nil} = asset,
         _at
       ) do
    generation_id =
      asset.semantic_generation_id ||
        TargetDescriptor.semantic_generation_id(
          Map.from_struct(asset),
          version.required_runner_release_id
        )

    {:ok,
     %{
       target_id: TargetIdentity.for_asset(asset.ref),
       evidence_generation_id: generation_id,
       target_generation_id: nil,
       physical_relation: nil
     }}
  end

  defp writable_identity(context, version, %Asset{} = asset, occurred_at) do
    target_id = TargetIdentity.for_asset(asset.ref)

    command = %EnsureWritableTargetGeneration{
      workspace_context: context,
      command_id:
        command_id(
          context.workspace_id,
          version.manifest_version_id,
          target_id,
          asset.target_descriptor.descriptor_hash
        ),
      target_id: target_id,
      manifest_version_id: version.manifest_version_id,
      descriptor: asset.target_descriptor,
      occurred_at: occurred_at
    }

    case Persistence.stores().target_generations.ensure_writable(command) do
      {:ok, result} ->
        generation_id = result.generation.target_generation_id

        {:ok,
         %{
           target_id: target_id,
           evidence_generation_id: generation_id,
           target_generation_id: generation_id,
           physical_relation: result.generation.physical_relation
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp read_identity(%Asset{target_descriptor: nil} = asset, _bindings) do
    if is_binary(asset.semantic_generation_id) do
      %{
        target_id: TargetIdentity.for_asset(asset.ref),
        evidence_generation_id: asset.semantic_generation_id,
        target_generation_id: nil
      }
    end
  end

  defp read_identity(%Asset{} = asset, bindings) do
    target_id = TargetIdentity.for_asset(asset.ref)

    case Map.get(bindings, target_id) do
      %{active_generation_id: generation_id} = binding when is_binary(generation_id) ->
        %{
          target_id: target_id,
          evidence_generation_id: generation_id,
          target_generation_id: generation_id,
          physical_relation: Map.get(binding, :active_physical_relation)
        }

      _other ->
        nil
    end
  end

  defp pin_identities(refs, context, version, index, occurred_at) do
    Enum.reduce_while(refs, {:ok, %{}}, fn ref, {:ok, identities} ->
      with {:ok, asset} <- Index.fetch_asset(index, ref),
           {:ok, identity} <- for_write(context, version, asset, occurred_at) do
        {:cont, {:ok, Map.put(identities, ref, identity)}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_pin_options(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) -- [:required_generation] == [],
      do: :ok,
      else: {:error, :invalid_generation_pin_options}
  end

  defp validate_rebuild_pin(rebuild) do
    ids = [
      Map.get(rebuild, :target_id),
      Map.get(rebuild, :candidate_generation_id),
      Map.get(rebuild, :operation_id),
      Map.get(rebuild, :action_id),
      Map.get(rebuild, :item_id)
    ]

    if Enum.all?(ids, &(is_binary(&1) and byte_size(&1) in 1..255)) and
         Map.get(rebuild, :target_operation, :rebuild_candidate) in [
           :rebuild_candidate,
           :normal_materialization
         ] and
         is_boolean(Map.get(rebuild, :empty_generation, false)) and
         is_boolean(Map.get(rebuild, :final_item, false)) and
         is_map(Map.get(rebuild, :active_relation)) and
         is_map(Map.get(rebuild, :candidate_relation)) and
         is_list(Map.get(rebuild, :input_generations)),
       do: :ok,
       else: {:error, :invalid_rebuild_generation_pin}
  end

  defp rebuild_asset(index, target_id) do
    case Enum.find(index.assets_by_ref, fn {_ref, asset} ->
           match?(%TargetDescriptor{target_id: ^target_id}, asset.target_descriptor)
         end) do
      {_ref, %Asset{} = asset} -> {:ok, asset}
      nil -> {:error, :invalid_rebuild_target}
    end
  end

  defp validate_rebuild_plan_target(%Plan{target_refs: [ref], nodes: nodes}, ref)
       when map_size(nodes) == 1,
       do: :ok

  defp validate_rebuild_plan_target(_plan, _ref), do: {:error, :invalid_rebuild_run_plan}

  defp validate_rebuild_inputs(_context, []), do: :ok

  defp validate_rebuild_inputs(context, inputs) do
    target_ids = Enum.map(inputs, &Map.get(&1, :target_id))

    if Enum.all?(target_ids, &is_binary/1) and target_ids == Enum.uniq(target_ids) do
      case Persistence.stores().target_generations.get_bindings(%GetTargetBindings{
             workspace_context: context,
             target_ids: target_ids
           }) do
        {:ok, bindings} ->
          current = Map.new(bindings, &{&1.target_id, &1.active_generation_id})

          if Enum.all?(inputs, fn input ->
               Map.get(current, Map.get(input, :target_id)) ==
                 Map.get(input, :target_generation_id)
             end),
             do: :ok,
             else: {:error, :pinned_input_changed}

        {:error, _reason} = error ->
          error
      end
    else
      {:error, :invalid_rebuild_input_generations}
    end
  end

  defp require_generation(%Plan{} = plan, nil), do: {:ok, plan}

  defp require_generation(
         %Plan{} = plan,
         %{
           target_id: target_id,
           evidence_generation_id: evidence_generation_id,
           target_generation_id: target_generation_id
         }
       )
       when is_binary(target_id) and is_binary(evidence_generation_id) and
              (is_nil(target_generation_id) or is_binary(target_generation_id)) do
    matching_nodes =
      plan.nodes
      |> Map.values()
      |> Enum.filter(&(&1.target_id == target_id))

    if matching_nodes != [] and
         Enum.all?(matching_nodes, fn node ->
           node.evidence_generation_id == evidence_generation_id and
             node.target_generation_id == target_generation_id
         end) do
      {:ok, plan}
    else
      {:error, :coverage_selection_stale}
    end
  end

  defp require_generation(%Plan{}, _invalid), do: {:error, :invalid_required_generation}

  defp command_id(workspace_id, manifest_id, target_id, descriptor_hash) do
    digest =
      :crypto.hash(
        :sha256,
        :erlang.term_to_binary({workspace_id, manifest_id, target_id, descriptor_hash})
      )
      |> Base.url_encode64(padding: false)

    "target-generation:ensure:" <> digest
  end
end
