defmodule FavnOrchestrator.Operator.Catalogue.Targets do
  @moduledoc """
  Projects manifest assets and pipelines into operator-facing target DTOs.

  Target ids, selector resolution, capability flags, and browser-safe manifest
  values are normalized here so catalogue list and detail paths cannot drift.
  """

  alias Favn.Manifest.Asset
  alias Favn.Manifest.Index
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.PipelineResolver
  alias Favn.Asset.RelationResolver
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Ref, as: RuntimeConfigRef
  alias Favn.Window.Policy
  alias Favn.Window.Spec, as: WindowSpec
  alias FavnOrchestrator.ManifestTarget

  @descriptor_keys ~w(
    target_id label asset_ref type relation metadata execution_pool runtime_config
    depends_on materialization partition_spec window max_concurrency can_run_without_window?
    can_backfill? selected_assets dependencies persisted?
  )a

  # `use Favn.Asset`, `use Favn.SQLAsset`, and `use Favn.Source` all name their
  # single asset `:asset`, so the ref name distinguishes nothing unless the module
  # declared several through `Favn.MultiAsset`.
  @anonymous_ref_name "asset"

  @doc "Returns sorted asset targets for a manifest version."
  @spec assets(Version.t()) :: [map()]
  def assets(%Version{} = version) do
    version.manifest.assets
    |> List.wrap()
    |> Enum.map(&asset/1)
    |> Enum.sort_by(& &1.label)
  end

  @doc "Projects one manifest asset into an operator target."
  @spec asset(Asset.t()) :: map()
  def asset(%Asset{} = asset) do
    target_ref = asset.ref
    asset_ref = ref_string(target_ref)
    relation = relation_dto(asset.relation)

    %{
      target_id: ManifestTarget.asset_id(target_ref),
      label: inspect(target_ref),
      asset_ref: asset_ref,
      name: asset_name(asset_ref, relation),
      module_path: module_namespace_segments(asset_ref),
      type: atom_name(asset.type),
      relation: relation,
      metadata: normalize_map(asset.metadata),
      execution_pool: atom_name(asset.execution_pool),
      runtime_config: normalize_data(asset.runtime_config),
      depends_on: Enum.map(List.wrap(asset.depends_on), &ref_string/1),
      materialization: normalize_data(asset.materialization),
      partition_spec: normalize_data(asset.partition_spec),
      window: normalize_data(asset.window),
      persisted?: not is_nil(asset.target_descriptor)
    }
  end

  @doc """
  Projects one manifest asset into the little that naming it takes.

  `asset/1` resolves a whole target: its relation, its descriptor, its runtime
  config. A dependency list needs a name and somewhere to link to, so it takes this
  instead of paying for all of that once per neighbour.
  """
  @spec asset_reference(Asset.t()) :: map()
  def asset_reference(%Asset{} = asset) do
    asset_ref = ref_string(asset.ref)

    %{
      target_id: ManifestTarget.asset_id(asset.ref),
      asset_ref: asset_ref,
      name: asset_name(asset_ref, relation_dto(asset.relation))
    }
  end

  @doc "Returns sorted basic pipeline targets for a manifest version."
  @spec pipelines(Version.t()) :: [map()]
  def pipelines(%Version{} = version) do
    version.manifest.pipelines
    |> List.wrap()
    |> Enum.map(&pipeline/1)
    |> Enum.sort_by(& &1.label)
  end

  @doc "Projects one manifest pipeline without resolving its selectors."
  @spec pipeline(Pipeline.t()) :: map()
  def pipeline(%Pipeline{} = pipeline) do
    target_module = pipeline.module

    %{
      target_id: ManifestTarget.pipeline_id(target_module, pipeline.name),
      label: inspect(target_module),
      window: window_policy_dto(pipeline.window),
      max_concurrency: pipeline.max_concurrency,
      execution_pool: atom_name(pipeline.execution_pool),
      can_run_without_window?: can_run_without_window?(pipeline.window),
      can_backfill?: can_backfill?(pipeline.window)
    }
  end

  @doc "Projects one manifest pipeline with its selected assets and dependency mode."
  @spec pipeline(Index.t(), Pipeline.t()) :: map()
  def pipeline(%Index{} = index, %Pipeline{} = pipeline) do
    pipeline
    |> pipeline()
    |> Map.put(:name, pipeline_name(pipeline))
    |> Map.put(:selected_assets, Enum.map(selected_refs(index, pipeline), &ref_string/1))
    |> Map.put(:dependencies, pipeline_dependencies(pipeline))
  end

  @doc "Restores the fixed top-level keys of a descriptor read from JSONB."
  @spec restore_descriptor(map()) :: map()
  def restore_descriptor(descriptor) when is_map(descriptor) do
    restored =
      Enum.reduce(@descriptor_keys, %{}, fn key, acc ->
        case fetch_descriptor_value(descriptor, key) do
          {:ok, value} -> Map.put(acc, key, value)
          :error -> acc
        end
      end)

    restored
    |> restore_dependencies()
    |> put_derived_names()
  end

  defp restore_dependencies(restored) do
    case Map.get(restored, :dependencies) do
      value when value in ["all", "none", "unknown"] ->
        Map.put(restored, :dependencies, String.to_existing_atom(value))

      _other ->
        restored
    end
  end

  # `name` and `module_path` are derived on read instead of persisted, so a
  # deployment planned before they existed resolves them too — without them an
  # operator sees every single-asset module as "asset" until someone republishes.
  # Both come from `asset_ref`, which asset descriptors have always carried.
  defp put_derived_names(%{asset_ref: asset_ref} = restored) when is_binary(asset_ref) do
    restored
    |> Map.put(:name, asset_name(asset_ref, Map.get(restored, :relation)))
    |> Map.put(:module_path, module_namespace_segments(asset_ref))
  end

  defp put_derived_names(restored), do: restored

  @doc "Serializes a target descriptor into its JSONB-safe persistence form."
  @spec serialize_descriptor(map()) :: %{optional(String.t()) => term()}
  def serialize_descriptor(descriptor) when is_map(descriptor) do
    Map.new(descriptor, fn {key, value} -> {to_string(key), normalize_data(value)} end)
  end

  @doc "Resolves a pipeline's selected asset refs with a safe raw-selector fallback."
  @spec selected_refs(Index.t(), Pipeline.t()) :: [Favn.Ref.t()]
  def selected_refs(%Index{} = index, %Pipeline{} = pipeline) do
    case PipelineResolver.resolve(index, pipeline, trigger: %{kind: :catalogue}) do
      {:ok, resolution} -> resolution.target_refs
      {:error, _reason} -> raw_selector_refs(index, pipeline)
    end
  end

  @doc """
  The name an operator should see for one asset.

  In order of preference: the name the asset's relation owns, its ref name, then
  the last segment of its module. The last case is not a fallback for broken
  input — `use Favn.Asset`, `use Favn.SQLAsset`, and `use Favn.Source` all
  produce the ref name `:asset`, so for a single-asset module the module is the
  only thing that distinguishes it. Without this every plain Elixir asset in a
  project reads "asset".

  Derived from the persisted `asset_ref` string rather than from a loaded module,
  because the control plane serves deployments whose project code it never loads.
  That is also why this cannot call
  `Favn.Asset.RelationResolver.inferred_relation_name_for_asset/1`, which settles
  the same question with `function_exported?/3` on the asset's own module; the
  rule the two share lives in `inferred_relation_name_for_segments/1`.

  ## Examples

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.asset_name("Elixir.MyApp.Lifecycle.RetryProbe:asset", nil)
      "retry_probe"

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.asset_name("Elixir.MyApp.Landing.Crm.Daily:activities", nil)
      "activities"

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.asset_name("Elixir.MyApp.Warehouse.Account:asset", %{"name" => "account"})
      "account"
  """
  @spec asset_name(String.t() | nil, map() | nil) :: String.t() | nil
  def asset_name(asset_ref, relation) when is_binary(asset_ref) do
    relation_name(relation) || ref_display_name(asset_ref)
  end

  def asset_name(_asset_ref, relation), do: relation_name(relation)

  @doc """
  An asset module's namespace segments, project root and name-bearing leaf removed.

  A well-structured project's module path mirrors its relation levels, so this is
  what an operator UI can show for an asset that owns no relation at all — a
  landing asset writing files, a probe. The leaf is dropped only when it is
  already carrying the asset's name, so no segment is shown twice.

  The root is assumed, not detected: the first segment is dropped because a Favn
  project nests its assets under one root module. A project that nests them
  deeper, or not at all, shifts every segment, so treat the result as a module
  path and not as a resolved relation address.

  ## Examples

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.module_namespace_segments("Elixir.MyApp.Warehouse.Source.Crm.Events.Activity:asset")
      ["warehouse", "source", "crm", "events"]

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.module_namespace_segments("Elixir.MyApp.Landing.Crm.Daily:activities")
      ["landing", "crm", "daily"]

      iex> alias FavnOrchestrator.Operator.Catalogue.Targets
      iex> Targets.module_namespace_segments("Elixir.Orders:asset")
      []
  """
  @spec module_namespace_segments(String.t() | nil) :: [String.t()]
  def module_namespace_segments(asset_ref) when is_binary(asset_ref) do
    case split_ref(asset_ref) do
      {segments, name} ->
        segments
        |> Enum.drop(1)
        |> drop_name_bearing_leaf(name)
        |> Enum.map(&Macro.underscore/1)

      :error ->
        []
    end
  end

  def module_namespace_segments(_asset_ref), do: []

  @doc "Formats a canonical asset ref for operator DTOs."
  @spec ref_string(term()) :: String.t()
  def ref_string({module, name}) when is_atom(module) and is_atom(name) do
    Atom.to_string(module) <> ":" <> Atom.to_string(name)
  end

  def ref_string(value), do: inspect(value)

  defp fetch_descriptor_value(descriptor, key) do
    case Map.fetch(descriptor, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(descriptor, Atom.to_string(key))
    end
  end

  defp raw_selector_refs(%Index{} = index, pipeline) do
    pipeline.selectors
    |> List.wrap()
    |> Enum.map(&raw_selector_ref/1)
    |> Enum.filter(&(not is_nil(&1) and Map.has_key?(index.assets_by_ref, &1)))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp raw_selector_ref({:asset, ref}), do: ref

  defp raw_selector_ref({module, name} = ref) when is_atom(module) and is_atom(name),
    do: ref

  defp raw_selector_ref(%{"module" => module, "name" => name})
       when is_binary(module) and is_binary(name) do
    {String.to_existing_atom(module), String.to_existing_atom(name)}
  rescue
    ArgumentError -> nil
  end

  defp raw_selector_ref(_selector), do: nil

  defp pipeline_name(%{name: name}) when is_atom(name), do: Atom.to_string(name)

  defp pipeline_name(%{module: module}) when is_atom(module),
    do: module |> Atom.to_string() |> String.split(".") |> List.last()

  defp pipeline_dependencies(%{deps: deps}) when deps in [:all, :none], do: deps
  defp pipeline_dependencies(_pipeline), do: :unknown

  defp can_run_without_window?(nil), do: true
  defp can_run_without_window?(%Policy{allow_full_load: true}), do: true
  defp can_run_without_window?(_window), do: false

  defp can_backfill?(nil), do: false
  defp can_backfill?(_window), do: true

  defp window_policy_dto(nil), do: nil

  defp window_policy_dto(%Policy{} = policy) do
    %{
      kind: Atom.to_string(policy.kind),
      anchor: Atom.to_string(policy.anchor),
      timezone: policy.timezone,
      allow_full_load: policy.allow_full_load
    }
  end

  defp window_policy_dto(policy) do
    case Policy.from_value(policy) do
      {:ok, normalized} -> window_policy_dto(normalized)
      {:error, _reason} -> normalize_data(policy)
    end
  end

  defp ref_display_name(asset_ref) do
    case split_ref(asset_ref) do
      {segments, @anonymous_ref_name} ->
        RelationResolver.inferred_relation_name_for_segments(segments)

      {_segments, name} ->
        name

      :error ->
        nil
    end
  end

  defp drop_name_bearing_leaf(segments, @anonymous_ref_name), do: Enum.drop(segments, -1)
  defp drop_name_bearing_leaf(segments, _name), do: segments

  # `ref_string/1` joins the module and name atoms with a colon, so the two come
  # back apart the same way. A ref that is not that shape — `inspect/1` output for
  # a malformed ref — yields no name and no segments rather than a guess.
  defp split_ref(asset_ref) do
    case String.split(asset_ref, ":", parts: 2) do
      [module, name] when module != "" and name != "" -> {module_segments(module), name}
      _other -> :error
    end
  end

  # `Atom.to_string/1` prefixes an Elixir module with "Elixir."; an Erlang module
  # or a plain atom has no prefix and is its own single segment.
  defp module_segments("Elixir." <> module), do: String.split(module, ".")
  defp module_segments(module), do: [module]

  defp relation_name(nil), do: nil
  defp relation_name(%{name: name}) when is_binary(name), do: name
  defp relation_name(%{"name" => name}) when is_binary(name), do: name
  defp relation_name(_relation), do: nil

  defp relation_dto(nil), do: nil

  defp relation_dto(relation) do
    ref = RelationRef.new!(relation)

    %{
      connection: atom_name(ref.connection),
      catalog: ref.catalog,
      schema: ref.schema,
      name: ref.name
    }
  rescue
    ArgumentError -> normalize_data(relation)
  end

  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp normalize_data(%RuntimeConfigRef{} = ref) do
    %{
      provider: atom_name(ref.provider),
      key: ref.key,
      secret: ref.secret?,
      required: ref.required?
    }
  end

  defp normalize_data(%WindowSpec{} = spec) do
    %{
      kind: atom_name(spec.kind),
      refresh_from: normalize_data(spec.refresh_from),
      required: spec.required,
      timezone: spec.timezone,
      timezone_source: normalize_data(spec.timezone_source)
    }
  end

  defp normalize_data(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp normalize_data(value) when is_struct(value) do
    value
    |> Map.from_struct()
    |> normalize_data()
  end

  defp normalize_data(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} ->
      {normalize_key(key), normalize_data(nested_value)}
    end)
  end

  defp normalize_data(value) when is_list(value), do: Enum.map(value, &normalize_data/1)
  defp normalize_data({module, name}), do: ref_string({module, name})
  defp normalize_data(value) when is_boolean(value), do: value
  defp normalize_data(value) when is_atom(value), do: atom_name(value)
  defp normalize_data(value), do: value

  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: inspect(key)

  defp atom_name(nil), do: nil
  defp atom_name(value) when is_atom(value), do: Atom.to_string(value)
  defp atom_name(value), do: to_string(value)
end
