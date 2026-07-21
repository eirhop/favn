defmodule FavnRunner.ManifestStore do
  @moduledoc """
  Byte- and entry-bounded immutable manifest runtime index.

  Registration verifies and compiles one manifest exactly once into asset and
  SQL-relation lookup maps. Execution paths receive only a small manifest handle,
  the exact selected asset, and relation metadata referenced by its package. This
  keeps per-asset preparation independent of total manifest size.
  """

  use GenServer

  alias Favn.Manifest.Asset
  alias Favn.Manifest.ExecutionPackage
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Version
  alias Favn.RelationRef
  alias Favn.SQL.Check
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template
  alias Favn.SQL.Template.AssetRef
  alias FavnRunner.ManifestHandle
  alias FavnRunner.ReleaseVerifier

  @default_max_entries 128
  @default_max_bytes 512 * 1_024 * 1_024
  @term_budget_multiplier 4

  @type fetch_error :: :manifest_not_found | :manifest_hash_mismatch
  @type lease_error :: :manifest_lease_not_found | :manifest_lease_mismatch

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    options = Keyword.drop(opts, [:name])

    if is_nil(name),
      do: GenServer.start_link(__MODULE__, options),
      else: GenServer.start_link(__MODULE__, options, name: name)
  end

  @spec register(Version.t(), keyword()) :: :ok | {:error, term()}
  def register(%Version{} = version, opts \\ []) do
    with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
      cache = server(opts)

      case ensure(version.manifest_version_id, version.content_hash, server: cache) do
        :ok ->
          :ok

        :missing ->
          with {:ok, entry} <- compile_entry(version) do
            bytes = @term_budget_multiplier * :erlang.external_size(entry)

            GenServer.call(
              cache,
              {:put_compiled, entry, bytes},
              Keyword.get(opts, :timeout, 30_000)
            )
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc "Checks a manifest identity without copying, verifying, or compiling its payload."
  @spec ensure(String.t(), String.t(), keyword()) :: :ok | :missing | {:error, term()}
  def ensure(manifest_version_id, content_hash, opts \\ [])

  def ensure(manifest_version_id, content_hash, opts)
      when is_binary(manifest_version_id) and is_binary(content_hash) and is_list(opts) do
    GenServer.call(server(opts), {:ensure, manifest_version_id, content_hash})
  end

  def ensure(_manifest_version_id, _content_hash, _opts), do: {:error, :invalid_manifest_identity}

  @doc "Atomically registers and leases one manifest for an active run."
  @spec acquire(Version.t(), String.t(), DateTime.t(), keyword()) ::
          :ok | {:error, term()}
  def acquire(version, lease_id, expires_at, opts \\ [])

  def acquire(%Version{} = version, lease_id, %DateTime{} = expires_at, opts)
      when is_binary(lease_id) and byte_size(lease_id) in 1..512 and is_list(opts) do
    with :ok <- ReleaseVerifier.verify_required_release(version.required_runner_release_id) do
      cache = server(opts)
      expires_at_ms = DateTime.to_unix(expires_at, :millisecond)
      timeout = Keyword.get(opts, :timeout, 30_000)

      case GenServer.call(
             cache,
             {:acquire_existing, version.manifest_version_id, version.content_hash, lease_id,
              expires_at_ms},
             timeout
           ) do
        :ok ->
          :ok

        :missing ->
          with {:ok, entry} <- compile_entry(version) do
            bytes = @term_budget_multiplier * :erlang.external_size(entry)

            GenServer.call(
              cache,
              {:put_compiled_and_acquire, entry, bytes, lease_id, expires_at_ms},
              timeout
            )
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  def acquire(%Version{}, _lease_id, %DateTime{}, _opts), do: {:error, :invalid_manifest_lease}

  @doc "Leases an already-registered manifest without copying its payload."
  @spec acquire_registered(String.t(), String.t(), String.t(), DateTime.t(), keyword()) ::
          :ok | {:error, term()}
  def acquire_registered(manifest_version_id, content_hash, lease_id, expires_at, opts \\ [])

  def acquire_registered(
        manifest_version_id,
        content_hash,
        lease_id,
        %DateTime{} = expires_at,
        opts
      )
      when is_binary(manifest_version_id) and is_binary(content_hash) and is_binary(lease_id) and
             byte_size(lease_id) in 1..512 and is_list(opts) do
    case GenServer.call(
           server(opts),
           {:acquire_existing, manifest_version_id, content_hash, lease_id,
            DateTime.to_unix(expires_at, :millisecond)},
           Keyword.get(opts, :timeout, 30_000)
         ) do
      :missing -> {:error, :manifest_not_found}
      result -> result
    end
  end

  def acquire_registered(_manifest_version_id, _content_hash, _lease_id, _expires_at, _opts),
    do: {:error, :invalid_manifest_lease}

  @doc "Extends one live manifest lease without transferring the manifest again."
  @spec renew(String.t(), DateTime.t(), keyword()) :: :ok | {:error, term()}
  def renew(lease_id, %DateTime{} = expires_at, opts \\ [])
      when is_binary(lease_id) and is_list(opts) do
    GenServer.call(
      server(opts),
      {:renew, lease_id, DateTime.to_unix(expires_at, :millisecond)}
    )
  end

  @doc "Releases one active-run manifest lease. Release is idempotent."
  @spec release(String.t(), keyword()) :: :ok
  def release(lease_id, opts \\ []) when is_binary(lease_id) and is_list(opts) do
    GenServer.call(server(opts), {:release, lease_id})
  end

  @spec fetch(String.t(), String.t() | nil, keyword()) ::
          {:ok, Version.t()} | {:error, fetch_error()}
  def fetch(manifest_version_id, expected_hash \\ nil, opts \\ [])

  def fetch(manifest_version_id, expected_hash, opts)
      when is_binary(manifest_version_id) and (is_binary(expected_hash) or is_nil(expected_hash)) and
             is_list(opts) do
    GenServer.call(server(opts), {:fetch, manifest_version_id, expected_hash})
  end

  def fetch(_manifest_version_id, _expected_hash, _opts), do: {:error, :manifest_not_found}

  @doc "Returns a small identity handle for an indexed manifest."
  @spec fetch_handle(String.t(), String.t() | nil, keyword()) ::
          {:ok, ManifestHandle.t()} | {:error, fetch_error()}
  def fetch_handle(manifest_version_id, expected_hash, opts \\ [])

  def fetch_handle(manifest_version_id, expected_hash, opts)
      when is_binary(manifest_version_id) and (is_binary(expected_hash) or is_nil(expected_hash)) and
             is_list(opts) do
    GenServer.call(server(opts), {:fetch_handle, manifest_version_id, expected_hash})
  end

  def fetch_handle(_manifest_version_id, _expected_hash, _opts),
    do: {:error, :manifest_not_found}

  @doc "Fetches one asset from a compiled manifest index."
  @spec fetch_asset(ManifestHandle.t(), Favn.Ref.t(), keyword()) ::
          {:ok, Asset.t()} | {:error, :asset_not_found | fetch_error()}
  def fetch_asset(handle, asset_ref, opts \\ [])

  def fetch_asset(%ManifestHandle{} = handle, asset_ref, opts) when is_tuple(asset_ref) do
    GenServer.call(server(opts), {:fetch_asset, handle, asset_ref})
  end

  def fetch_asset(%ManifestHandle{}, _asset_ref, _opts), do: {:error, :asset_not_found}

  @doc "Fetches the existing assets for one normalized planned-ref batch."
  @spec fetch_assets(ManifestHandle.t(), [Favn.Ref.t()], keyword()) ::
          {:ok, [Asset.t()]} | {:error, fetch_error()}
  def fetch_assets(%ManifestHandle{} = handle, asset_refs, opts \\ [])
      when is_list(asset_refs) and is_list(opts) do
    GenServer.call(server(opts), {:fetch_assets, handle, asset_refs})
  end

  @doc "Returns relation metadata for only the requested asset modules."
  @spec fetch_relations(ManifestHandle.t(), [module()], keyword()) ::
          {:ok, %{optional(module()) => RelationRef.t()}} | {:error, fetch_error()}
  def fetch_relations(%ManifestHandle{} = handle, modules, opts \\ []) when is_list(modules) do
    GenServer.call(server(opts), {:fetch_relations, handle, modules})
  end

  @doc "Returns the bounded SQL relation map referenced by one execution package."
  @spec fetch_package_relations(ManifestHandle.t(), ExecutionPackage.t() | nil, keyword()) ::
          {:ok, %{optional(module()) => RelationRef.t()}} | {:error, fetch_error()}
  def fetch_package_relations(handle, package, opts \\ [])

  def fetch_package_relations(
        %ManifestHandle{} = handle,
        %ExecutionPackage{} = package,
        opts
      ) do
    fetch_relations(handle, package_relation_modules(package), opts)
  end

  def fetch_package_relations(%ManifestHandle{}, nil, _opts), do: {:ok, %{}}

  @doc "Fetches the exact execution-time manifest bundle under one active lease."
  @spec fetch_execution_bundle(
          String.t(),
          String.t(),
          String.t(),
          Favn.Ref.t(),
          ExecutionPackage.t() | nil,
          keyword()
        ) ::
          {:ok, ManifestHandle.t(), Asset.t(), %{optional(module()) => RelationRef.t()}}
          | {:error, :asset_not_found | fetch_error() | lease_error()}
  def fetch_execution_bundle(
        lease_id,
        manifest_version_id,
        content_hash,
        asset_ref,
        package,
        opts \\ []
      )

  def fetch_execution_bundle(
        lease_id,
        manifest_version_id,
        content_hash,
        asset_ref,
        package,
        opts
      )
      when is_binary(lease_id) and is_binary(manifest_version_id) and is_binary(content_hash) and
             is_tuple(asset_ref) and (is_struct(package, ExecutionPackage) or is_nil(package)) and
             is_list(opts) do
    modules = if package, do: package_relation_modules(package), else: []

    GenServer.call(
      server(opts),
      {:fetch_execution_bundle, lease_id, manifest_version_id, content_hash, asset_ref, modules}
    )
  end

  def fetch_execution_bundle(
        _lease_id,
        _manifest_version_id,
        _content_hash,
        _asset_ref,
        _package,
        _opts
      ),
      do: {:error, :manifest_lease_not_found}

  @doc "Returns bounded cache usage, build, and lookup counters."
  @spec diagnostics(keyword()) :: map()
  def diagnostics(opts \\ []) when is_list(opts) do
    GenServer.call(server(opts), :diagnostics)
  end

  @impl true
  def init(opts) do
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)

    if valid_max_entries?(max_entries) and valid_max_bytes?(max_bytes) do
      {:ok,
       %{
         entries: %{},
         sizes: %{},
         order: :queue.new(),
         leases: %{},
         lease_counts: %{},
         count: 0,
         bytes: 0,
         max_entries: max_entries,
         max_bytes: max_bytes,
         evictions: 0,
         oversized_rejections: 0,
         index_builds: 0,
         asset_lookups: 0,
         relation_lookups: 0
       }}
    else
      {:stop, :invalid_manifest_cache_size}
    end
  end

  @impl true
  def handle_call({:ensure, manifest_version_id, content_hash}, _from, state) do
    reply =
      case Map.fetch(state.entries, manifest_version_id) do
        {:ok, %{version: %Version{content_hash: ^content_hash}}} ->
          :ok

        {:ok, %{version: %Version{content_hash: existing_hash}}} ->
          {:error, {:manifest_version_conflict, manifest_version_id, existing_hash, content_hash}}

        :error ->
          :missing
      end

    {:reply, reply, state}
  end

  def handle_call(
        {:acquire_existing, manifest_version_id, content_hash, lease_id, expires_at_ms},
        _from,
        state
      ) do
    case Map.get(state.leases, lease_id) do
      %{manifest_version_id: existing} when existing != manifest_version_id ->
        {:reply, {:error, :manifest_lease_conflict}, state}

      _available ->
        case fetch_entry(state, manifest_version_id, content_hash) do
          {:ok, _entry} ->
            case put_lease(state, lease_id, manifest_version_id, expires_at_ms) do
              {:ok, state} -> {:reply, :ok, state}
              {:error, reason} -> {:reply, {:error, reason}, state}
            end

          {:error, :manifest_not_found} ->
            {:reply, :missing, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call({:put_compiled, entry, bytes}, _from, state) do
    version = entry.version
    state = %{state | index_builds: state.index_builds + 1}

    case Map.fetch(state.entries, version.manifest_version_id) do
      {:ok, %{version: %Version{content_hash: existing_hash}}}
      when existing_hash == version.content_hash ->
        {:reply, :ok, state}

      {:ok, %{version: %Version{content_hash: existing_hash}}} ->
        {:reply,
         {:error,
          {:manifest_version_conflict, version.manifest_version_id, existing_hash,
           version.content_hash}}, state}

      :error ->
        case insert_entry(entry, bytes, state) do
          {:ok, next_state} -> {:reply, :ok, next_state}
          {:error, reason, next_state} -> {:reply, {:error, reason}, next_state}
        end
    end
  end

  def handle_call(
        {:put_compiled_and_acquire, entry, bytes, lease_id, expires_at_ms},
        _from,
        state
      ) do
    version = entry.version
    state = %{state | index_builds: state.index_builds + 1}

    result =
      case Map.fetch(state.entries, version.manifest_version_id) do
        {:ok, %{version: %Version{content_hash: existing_hash}}}
        when existing_hash == version.content_hash ->
          {:ok, state}

        {:ok, %{version: %Version{content_hash: existing_hash}}} ->
          {:error,
           {:manifest_version_conflict, version.manifest_version_id, existing_hash,
            version.content_hash}, state}

        :error ->
          insert_entry(entry, bytes, state)
      end

    case result do
      {:ok, next_state} ->
        case put_lease(next_state, lease_id, version.manifest_version_id, expires_at_ms) do
          {:ok, leased_state} -> {:reply, :ok, leased_state}
          {:error, reason} -> {:reply, {:error, reason}, next_state}
        end

      {:error, reason, next_state} ->
        {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call({:release, lease_id}, _from, state) do
    {:reply, :ok, delete_lease(state, lease_id)}
  end

  def handle_call({:renew, lease_id, expires_at_ms}, _from, state) do
    case Map.fetch(state.leases, lease_id) do
      {:ok, %{manifest_version_id: manifest_version_id, expires_at_ms: current_expiry}} ->
        now_ms = System.system_time(:millisecond)

        cond do
          current_expiry <= now_ms ->
            {:reply, {:error, :manifest_lease_not_found}, delete_lease(state, lease_id)}

          expires_at_ms <= now_ms ->
            {:reply, {:error, :invalid_manifest_lease_expiry}, state}

          true ->
            lease = %{manifest_version_id: manifest_version_id, expires_at_ms: expires_at_ms}
            {:reply, :ok, %{state | leases: Map.put(state.leases, lease_id, lease)}}
        end

      :error ->
        {:reply, {:error, :manifest_lease_not_found}, state}
    end
  end

  def handle_call({:fetch, manifest_version_id, expected_hash}, _from, state) do
    reply =
      with {:ok, entry} <- fetch_entry(state, manifest_version_id, expected_hash) do
        {:ok, entry.version}
      end

    {:reply, reply, state}
  end

  def handle_call({:fetch_handle, manifest_version_id, expected_hash}, _from, state) do
    reply =
      with {:ok, entry} <- fetch_entry(state, manifest_version_id, expected_hash) do
        {:ok, handle(entry.version)}
      end

    {:reply, reply, state}
  end

  def handle_call({:fetch_asset, %ManifestHandle{} = handle, asset_ref}, _from, state) do
    {reply, lookup_count} =
      with {:ok, entry} <- fetch_entry(state, handle.manifest_version_id, handle.content_hash) do
        case Map.fetch(entry.assets_by_ref, asset_ref) do
          {:ok, %Asset{} = asset} -> {{:ok, asset}, 1}
          :error -> {{:error, :asset_not_found}, 1}
        end
      else
        {:error, reason} -> {{:error, reason}, 0}
      end

    {:reply, reply, %{state | asset_lookups: state.asset_lookups + lookup_count}}
  end

  def handle_call({:fetch_assets, %ManifestHandle{} = handle, asset_refs}, _from, state) do
    case fetch_entry(state, handle.manifest_version_id, handle.content_hash) do
      {:ok, entry} ->
        assets =
          Enum.flat_map(asset_refs, fn asset_ref ->
            case Map.fetch(entry.assets_by_ref, asset_ref) do
              {:ok, %Asset{} = asset} -> [asset]
              :error -> []
            end
          end)

        {:reply, {:ok, assets},
         %{state | asset_lookups: state.asset_lookups + length(asset_refs)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:fetch_relations, %ManifestHandle{} = handle, modules}, _from, state) do
    case fetch_entry(state, handle.manifest_version_id, handle.content_hash) do
      {:ok, entry} ->
        relations = Map.take(entry.relations_by_module, modules)

        {:reply, {:ok, relations},
         %{state | relation_lookups: state.relation_lookups + length(modules)}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(
        {:fetch_execution_bundle, lease_id, manifest_version_id, content_hash, asset_ref,
         modules},
        _from,
        state
      ) do
    with {:ok, state} <- validate_lease(state, lease_id, manifest_version_id),
         {:ok, entry} <- fetch_entry(state, manifest_version_id, content_hash),
         {:ok, %Asset{} = asset} <- fetch_asset_from_entry(entry, asset_ref) do
      relations = Map.take(entry.relations_by_module, modules)

      next_state = %{
        state
        | asset_lookups: state.asset_lookups + 1,
          relation_lookups: state.relation_lookups + length(modules)
      }

      {:reply, {:ok, handle(entry.version), asset, relations}, next_state}
    else
      {:error, reason, next_state} -> {:reply, {:error, reason}, next_state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:diagnostics, _from, state) do
    {:reply,
     Map.take(state, [
       :count,
       :bytes,
       :max_entries,
       :max_bytes,
       :evictions,
       :oversized_rejections,
       :index_builds,
       :asset_lookups,
       :relation_lookups
     ])
     |> Map.put(:active_leases, map_size(state.leases)), state}
  end

  defp insert_entry(%{version: %Version{} = version} = entry, bytes, state) do
    if bytes > state.max_bytes do
      emit(:oversized, bytes)

      {:error, :manifest_exceeds_runner_cache_budget,
       %{state | oversized_rejections: state.oversized_rejections + 1}}
    else
      id = version.manifest_version_id

      with {:ok, state} <- make_room(state, 1, bytes) do
        {:ok,
         %{
           state
           | entries: Map.put(state.entries, id, entry),
             sizes: Map.put(state.sizes, id, bytes),
             order: :queue.in(id, state.order),
             count: state.count + 1,
             bytes: state.bytes + bytes
         }}
      else
        {:error, next_state} ->
          {:error, :manifest_cache_capacity_exhausted, next_state}
      end
    end
  end

  defp make_room(state, added_count, added_bytes) do
    state = prune_expired_leases(state)

    if state.count + added_count <= state.max_entries and
         state.bytes + added_bytes <= state.max_bytes do
      {:ok, state}
    else
      evict_unleased(state, added_count, added_bytes, state.count)
    end
  end

  defp evict_unleased(state, _added_count, _added_bytes, 0), do: {:error, state}

  defp evict_unleased(state, added_count, added_bytes, remaining) do
    case :queue.out(state.order) do
      {{:value, id}, order} ->
        if Map.get(state.lease_counts, id, 0) > 0 do
          evict_unleased(
            %{state | order: :queue.in(id, order)},
            added_count,
            added_bytes,
            remaining - 1
          )
        else
          bytes = Map.fetch!(state.sizes, id)
          emit(:eviction, bytes)

          next_state = %{
            state
            | entries: Map.delete(state.entries, id),
              sizes: Map.delete(state.sizes, id),
              order: order,
              count: state.count - 1,
              bytes: state.bytes - bytes,
              evictions: state.evictions + 1
          }

          make_room(next_state, added_count, added_bytes)
        end

      {:empty, _order} ->
        {:error, state}
    end
  end

  defp fetch_entry(state, manifest_version_id, expected_hash) do
    with {:ok, entry} <- Map.fetch(state.entries, manifest_version_id),
         :ok <- match_expected_hash(entry.version, expected_hash) do
      {:ok, entry}
    else
      :error -> {:error, :manifest_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_asset_from_entry(entry, asset_ref) do
    case Map.fetch(entry.assets_by_ref, asset_ref) do
      {:ok, %Asset{} = asset} -> {:ok, asset}
      :error -> {:error, :asset_not_found}
    end
  end

  defp put_lease(state, lease_id, manifest_version_id, expires_at_ms) do
    now_ms = System.system_time(:millisecond)

    cond do
      expires_at_ms <= now_ms ->
        {:error, :invalid_manifest_lease_expiry}

      match?(
        %{manifest_version_id: existing} when existing != manifest_version_id,
        state.leases[lease_id]
      ) ->
        {:error, :manifest_lease_conflict}

      match?(%{manifest_version_id: ^manifest_version_id}, state.leases[lease_id]) ->
        existing_expiry = state.leases[lease_id].expires_at_ms

        lease = %{
          manifest_version_id: manifest_version_id,
          expires_at_ms: max(existing_expiry, expires_at_ms)
        }

        {:ok, %{state | leases: Map.put(state.leases, lease_id, lease)}}

      true ->
        lease = %{manifest_version_id: manifest_version_id, expires_at_ms: expires_at_ms}

        {:ok,
         %{
           state
           | leases: Map.put(state.leases, lease_id, lease),
             lease_counts: Map.update(state.lease_counts, manifest_version_id, 1, &(&1 + 1))
         }}
    end
  end

  defp validate_lease(state, lease_id, manifest_version_id) do
    case Map.fetch(state.leases, lease_id) do
      {:ok, %{manifest_version_id: ^manifest_version_id, expires_at_ms: expires_at_ms}} ->
        if expires_at_ms > System.system_time(:millisecond) do
          {:ok, state}
        else
          {:error, :manifest_lease_not_found, delete_lease(state, lease_id)}
        end

      {:ok, _other_manifest} ->
        {:error, :manifest_lease_mismatch, state}

      :error ->
        {:error, :manifest_lease_not_found, state}
    end
  end

  defp delete_lease(state, lease_id) do
    case Map.pop(state.leases, lease_id) do
      {nil, _leases} ->
        state

      {%{manifest_version_id: manifest_version_id}, leases} ->
        lease_counts =
          case Map.fetch!(state.lease_counts, manifest_version_id) do
            1 -> Map.delete(state.lease_counts, manifest_version_id)
            count -> Map.put(state.lease_counts, manifest_version_id, count - 1)
          end

        %{state | leases: leases, lease_counts: lease_counts}
    end
  end

  defp prune_expired_leases(state) do
    now_ms = System.system_time(:millisecond)

    Enum.reduce(state.leases, state, fn
      {lease_id, %{expires_at_ms: expires_at_ms}}, acc when expires_at_ms <= now_ms ->
        delete_lease(acc, lease_id)

      _active, acc ->
        acc
    end)
  end

  defp compile_entry(%Version{} = version) do
    with {:ok, verified} <- Version.verify(version),
         {:ok, assets_by_ref, relations_by_module} <-
           build_runtime_maps(verified.manifest.assets) do
      {:ok,
       %{
         version: verified,
         assets_by_ref: assets_by_ref,
         relations_by_module: relations_by_module
       }}
    end
  end

  defp build_runtime_maps(assets) when is_list(assets) do
    Enum.reduce_while(assets, {:ok, %{}, %{}}, fn
      %Asset{ref: {module, name}} = asset, {:ok, assets_by_ref, relations_by_module}
      when is_atom(module) and is_atom(name) and not is_nil(module) and not is_nil(name) ->
        if Map.has_key?(assets_by_ref, asset.ref) do
          {:halt, {:error, {:duplicate_asset_ref, asset.ref}}}
        else
          relations_by_module = maybe_put_relation(relations_by_module, asset)

          {:cont, {:ok, Map.put(assets_by_ref, asset.ref, asset), relations_by_module}}
        end

      %Asset{ref: ref}, _acc ->
        {:halt, {:error, {:invalid_asset_ref, ref}}}

      other, _acc ->
        {:halt, {:error, {:invalid_asset_ref, other}}}
    end)
  end

  defp build_runtime_maps(_assets), do: {:error, :invalid_manifest}

  defp maybe_put_relation(relations, %Asset{
         module: module,
         relation: %RelationRef{} = relation
       })
       when is_atom(module),
       do: Map.put(relations, module, relation)

  defp maybe_put_relation(relations, _asset), do: relations

  defp package_relation_modules(%ExecutionPackage{
         sql_execution: %SQLExecution{} = execution
       }) do
    templates =
      [execution.template] ++
        Enum.map(execution.checks, fn %Check{} = check -> check.template end) ++
        Enum.map(execution.sql_definitions, fn %SQLDefinition{} = definition ->
          definition.template
        end)

    templates
    |> Enum.flat_map(&Template.asset_refs/1)
    |> Enum.flat_map(fn
      %AssetRef{module: module} when is_atom(module) -> [module]
      _asset_ref -> []
    end)
    |> Enum.uniq()
  end

  defp package_relation_modules(%ExecutionPackage{}), do: []

  defp handle(%Version{} = version) do
    %ManifestHandle{
      manifest_version_id: version.manifest_version_id,
      content_hash: version.content_hash,
      required_runner_release_id: version.required_runner_release_id
    }
  end

  defp match_expected_hash(_version, nil), do: :ok

  defp match_expected_hash(%Version{content_hash: content_hash}, expected_hash)
       when content_hash == expected_hash,
       do: :ok

  defp match_expected_hash(_version, _expected_hash), do: {:error, :manifest_hash_mismatch}

  defp emit(event, bytes) do
    :telemetry.execute(
      [:favn, :runner, :manifest_cache, event],
      %{count: 1, bytes: bytes},
      %{}
    )
  end

  defp server(opts), do: Keyword.get(opts, :server, __MODULE__)

  defp valid_max_entries?(value), do: is_integer(value) and value in 1..100_000

  defp valid_max_bytes?(value),
    do: is_integer(value) and value >= 1 and value <= 16 * 1_024 * 1_024 * 1_024
end
