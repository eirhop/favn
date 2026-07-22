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
      ) do
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

      {:ok, %{plan | nodes: nodes}}
    end
  end

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
      %{active_generation_id: generation_id} when is_binary(generation_id) ->
        %{
          target_id: target_id,
          evidence_generation_id: generation_id,
          target_generation_id: generation_id
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
