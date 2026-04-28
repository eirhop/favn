defmodule Favn.Manifest.Rehydrate do
  @moduledoc """
  Rehydrates decoded manifest payloads into canonical runtime structs.
  """

  alias Favn.Asset.RelationInput
  alias Favn.Manifest
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Build
  alias Favn.Manifest.Graph
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.SQLExecution
  alias Favn.RelationRef
  alias Favn.RuntimeConfig.Ref, as: RuntimeConfigRef
  alias Favn.SQL.Definition, as: SQLDefinition
  alias Favn.SQL.Template

  alias Favn.SQL.Template.{
    AssetRef,
    Call,
    DefinitionRef,
    Fragment,
    Placeholder,
    Relation,
    Requirements,
    Text
  }

  alias Favn.SQLAsset.Materialization
  alias Favn.Window.{Policy, Spec}

  @max_manifest_atom_length 128
  @max_manifest_module_length 512

  @type error :: {:invalid_manifest_input, term()} | {:invalid_manifest_payload, term()}

  @spec manifest(map() | struct() | Build.t()) :: {:ok, Manifest.t()} | {:error, error()}
  def manifest(%Build{manifest: manifest}), do: manifest(manifest)
  def manifest(%Manifest{} = manifest), do: {:ok, manifest}

  def manifest(value) when is_map(value) do
    {:ok, build_manifest(value)}
  rescue
    error -> {:error, {:invalid_manifest_payload, error}}
  end

  def manifest(other), do: {:error, {:invalid_manifest_input, other}}

  defp build_manifest(value) do
    %Manifest{
      schema_version: field_value(value, :schema_version),
      runner_contract_version: field_value(value, :runner_contract_version),
      assets: value |> field_value(:assets, []) |> build_assets(),
      pipelines: value |> field_value(:pipelines, []) |> build_pipelines(),
      schedules: value |> field_value(:schedules, []) |> build_schedules(),
      graph: value |> field_value(:graph, %Graph{}) |> build_graph(),
      metadata: value |> field_value(:metadata, %{}) |> plain_map()
    }
  end

  defp build_assets(values) when is_list(values), do: Enum.map(values, &build_asset/1)
  defp build_assets(_other), do: []

  defp build_asset(value) when is_map(value) do
    %Asset{
      ref: value |> field_value(:ref) |> decode_ref(),
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      type: value |> field_value(:type, :elixir) |> decode_known_atom([:elixir, :sql, :source]),
      depends_on: value |> field_value(:depends_on, []) |> build_refs(),
      execution: value |> field_value(:execution, %{}) |> build_execution(),
      config: value |> field_value(:config, %{}) |> plain_map(),
      relation: value |> field_value(:relation) |> build_relation(),
      window: value |> field_value(:window) |> build_window_spec(),
      materialization: value |> field_value(:materialization) |> decode_materialization(),
      relation_inputs: value |> field_value(:relation_inputs, []) |> build_relation_inputs(),
      runtime_config: value |> field_value(:runtime_config, %{}) |> build_runtime_config(),
      sql_execution: value |> field_value(:sql_execution) |> build_sql_execution(),
      metadata: value |> field_value(:metadata, %{}) |> build_metadata()
    }
  end

  defp build_asset(other), do: other

  defp build_execution(value) when is_map(value) do
    %{
      entrypoint: value |> field_value(:entrypoint) |> decode_atom_optional(),
      arity: field_value(value, :arity)
    }
  end

  defp build_execution(_other), do: %{entrypoint: nil, arity: nil}

  defp build_relation(nil), do: nil
  defp build_relation(%RelationRef{} = value), do: value

  defp build_relation(value) when is_map(value) or is_list(value) do
    RelationRef.new!(%{
      connection: value |> field_value(:connection) |> decode_atom_optional(),
      catalog: field_value(value, :catalog),
      schema: field_value(value, :schema),
      name: field_value(value, :name)
    })
  rescue
    _error -> plain_map(value)
  end

  defp build_relation(other), do: other

  defp build_window_spec(nil), do: nil
  defp build_window_spec(%Spec{} = value), do: value

  defp build_window_spec(value) when is_map(value) do
    kind = value |> field_value(:kind) |> decode_known_atom([:hour, :day, :month, :year])

    opts = [
      lookback: field_value(value, :lookback, 0),
      required: field_value(value, :required, false),
      timezone: field_value(value, :timezone, "Etc/UTC")
    ]

    opts =
      case field_value(value, :refresh_from) do
        nil ->
          opts

        refresh_from ->
          Keyword.put(
            opts,
            :refresh_from,
            decode_known_atom(refresh_from, [:hour, :day, :month, :year])
          )
      end

    try do
      Spec.new!(kind, opts)
    rescue
      _error -> plain_map(value)
    end
  end

  defp build_window_spec(other), do: other

  defp decode_materialization(nil), do: nil
  defp decode_materialization(:view), do: :view
  defp decode_materialization(:table), do: :table
  defp decode_materialization("view"), do: :view
  defp decode_materialization("table"), do: :table

  defp decode_materialization({:incremental, opts}) when is_list(opts) do
    Materialization.normalize!({:incremental, decode_materialization_opts(opts)})
  rescue
    _error -> {:incremental, decode_materialization_opts(opts)}
  end

  defp decode_materialization([kind, opts]) when kind in [:incremental, "incremental"] do
    decode_materialization({:incremental, opts})
  end

  defp decode_materialization(other), do: other

  defp decode_materialization_opts(values) when is_list(values) do
    Enum.map(values, fn
      {key, value} ->
        {decode_materialization_opt_key(key), decode_materialization_opt_value(key, value)}

      [key, value] ->
        {decode_materialization_opt_key(key), decode_materialization_opt_value(key, value)}

      other ->
        other
    end)
  end

  defp decode_materialization_opts(other), do: other

  defp decode_materialization_opt_key(key),
    do: decode_known_atom(key, [:strategy, :unique_key, :window_column])

  defp decode_materialization_opt_value(key, value) do
    case decode_materialization_opt_key(key) do
      :strategy -> decode_known_atom(value, [:append, :replace, :delete_insert, :merge])
      :unique_key when is_list(value) -> Enum.map(value, &decode_atom_optional/1)
      :window_column when is_binary(value) -> value
      :window_column -> decode_atom_optional(value)
      _ -> value
    end
  end

  defp build_relation_inputs(values) when is_list(values),
    do: Enum.map(values, &build_relation_input/1)

  defp build_relation_inputs(_other), do: []

  defp build_relation_input(value) when is_map(value) do
    %RelationInput{
      kind:
        value
        |> field_value(:kind)
        |> decode_known_atom([:plain_relation, :direct_asset_ref]),
      relation_ref: value |> field_value(:relation_ref) |> build_relation(),
      raw: field_value(value, :raw),
      asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
      resolution:
        value |> field_value(:resolution) |> decode_known_atom_optional([:resolved, :deferred]),
      span: value |> field_value(:span) |> plain_map_or_nil()
    }
  end

  defp build_relation_input(other), do: other

  defp build_runtime_config(values) when is_map(values) do
    Map.new(values, fn {scope, fields} ->
      {decode_atom_optional(scope), build_runtime_config_fields(fields)}
    end)
  end

  defp build_runtime_config(_other), do: %{}

  defp build_runtime_config_fields(fields) when is_map(fields) do
    Map.new(fields, fn {field, value} ->
      {decode_atom_optional(field), build_runtime_config_ref(value)}
    end)
  end

  defp build_runtime_config_fields(_other), do: %{}

  defp build_runtime_config_ref(%RuntimeConfigRef{} = ref), do: ref

  defp build_runtime_config_ref(value) when is_map(value) do
    %RuntimeConfigRef{
      provider: value |> field_value(:provider) |> decode_known_atom([:env]),
      key: field_value(value, :key),
      secret?: field_value(value, :secret?, field_value(value, :secret, false)),
      required?: field_value(value, :required?, field_value(value, :required, true))
    }
  end

  defp build_runtime_config_ref(other), do: other

  defp build_sql_execution(nil), do: nil

  defp build_sql_execution(value) when is_map(value) do
    %SQLExecution{
      sql: field_value(value, :sql),
      template: value |> field_value(:template) |> build_template(),
      sql_definitions: value |> field_value(:sql_definitions, []) |> build_sql_definitions()
    }
  end

  defp build_sql_execution(other), do: other

  defp build_sql_definitions(values) when is_list(values),
    do: Enum.map(values, &build_sql_definition/1)

  defp build_sql_definitions(_other), do: []

  defp build_sql_definition(value) when is_map(value) do
    %SQLDefinition{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      arity: field_value(value, :arity),
      params: value |> field_value(:params, []) |> build_sql_params(),
      shape: value |> field_value(:shape) |> decode_known_atom([:expression, :relation]),
      sql: field_value(value, :sql),
      template: value |> field_value(:template) |> build_template(),
      file: field_value(value, :file),
      line: field_value(value, :line),
      declared_file: field_value(value, :declared_file),
      declared_line: field_value(value, :declared_line)
    }
  end

  defp build_sql_definition(other), do: other

  defp build_sql_params(values) when is_list(values), do: Enum.map(values, &build_sql_param/1)
  defp build_sql_params(_other), do: []

  defp build_sql_param(value) when is_map(value) do
    %SQLDefinition.Param{
      name: value |> field_value(:name) |> decode_atom_optional(),
      index: field_value(value, :index)
    }
  end

  defp build_sql_param(other), do: other

  defp build_template(%Template{} = value), do: value

  defp build_template(value) when is_map(value) do
    %Template{
      source: field_value(value, :source),
      root_kind: value |> field_value(:root_kind) |> decode_known_atom([:query, :expression]),
      nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
      span: value |> field_value(:span) |> build_template_span(),
      requires: value |> field_value(:requires) |> build_requirements()
    }
  end

  defp build_template(other), do: other

  defp build_template_nodes(values) when is_list(values),
    do: Enum.map(values, &build_template_node/1)

  defp build_template_nodes(_other), do: []

  defp build_template_node(value) when is_map(value) do
    cond do
      template_relation?(value) ->
        %Relation{
          raw: field_value(value, :raw),
          segments: value |> field_value(:segments, []) |> build_string_list(),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_placeholder?(value) ->
        %Placeholder{
          name: value |> field_value(:name) |> decode_atom_optional(),
          source: value |> field_value(:source) |> decode_placeholder_source(),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_call?(value) ->
        %Call{
          definition: value |> field_value(:definition) |> build_definition_ref(),
          args: value |> field_value(:args, []) |> build_template_fragments(),
          context: value |> field_value(:context) |> decode_known_atom([:expression, :relation]),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_asset_ref?(value) ->
        %AssetRef{
          module: value |> field_value(:module) |> decode_module(),
          asset_ref: value |> field_value(:asset_ref) |> decode_ref(),
          relation: value |> field_value(:relation) |> build_relation(),
          resolution:
            value |> field_value(:resolution) |> decode_known_atom([:resolved, :deferred]),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_fragment?(value) ->
        %Fragment{
          nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
          span: value |> field_value(:span) |> build_template_span()
        }

      template_text?(value) ->
        %Text{
          sql: field_value(value, :sql),
          span: value |> field_value(:span) |> build_template_span()
        }

      true ->
        plain_map(value)
    end
  end

  defp build_template_node(other), do: other

  defp build_template_fragments(values) when is_list(values),
    do: Enum.map(values, &build_template_fragment/1)

  defp build_template_fragments(_other), do: []

  defp build_template_fragment(value) when is_map(value) do
    %Fragment{
      nodes: value |> field_value(:nodes, []) |> build_template_nodes(),
      span: value |> field_value(:span) |> build_template_span()
    }
  end

  defp build_template_fragment(other), do: other

  defp build_definition_ref(value) when is_map(value) do
    %DefinitionRef{
      provider: value |> field_value(:provider) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      arity: field_value(value, :arity),
      kind: value |> field_value(:kind) |> decode_known_atom([:expression, :relation])
    }
  end

  defp build_definition_ref(other), do: other

  defp decode_placeholder_source(:runtime), do: :runtime
  defp decode_placeholder_source(:query_param), do: :query_param
  defp decode_placeholder_source("runtime"), do: :runtime
  defp decode_placeholder_source("query_param"), do: :query_param
  defp decode_placeholder_source({:local_arg, index}), do: {:local_arg, index}

  defp decode_placeholder_source([kind, index]) when kind in [:local_arg, "local_arg"],
    do: {:local_arg, index}

  defp decode_placeholder_source(other), do: other

  defp build_requirements(%Requirements{} = value), do: value

  defp build_requirements(value) when is_map(value) do
    %Requirements{
      runtime_inputs: value |> field_value(:runtime_inputs) |> build_atom_mapset(),
      query_params: value |> field_value(:query_params) |> build_atom_mapset()
    }
  end

  defp build_requirements(_other),
    do: %Requirements{runtime_inputs: MapSet.new(), query_params: MapSet.new()}

  defp build_atom_mapset(%MapSet{} = value), do: value

  defp build_atom_mapset(value) when is_map(value) do
    entries =
      case field_value(value, :map) do
        nested when is_map(nested) -> Map.keys(nested)
        _other -> Map.keys(value)
      end

    entries
    |> Enum.map(&decode_atom_optional/1)
    |> MapSet.new()
  end

  defp build_atom_mapset(value) when is_list(value) do
    value
    |> Enum.map(&decode_atom_optional/1)
    |> MapSet.new()
  end

  defp build_atom_mapset(_other), do: MapSet.new()

  defp build_pipelines(values) when is_list(values), do: Enum.map(values, &build_pipeline/1)
  defp build_pipelines(_other), do: []

  defp build_pipeline(value) when is_map(value) do
    %Pipeline{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      selectors: value |> field_value(:selectors, []) |> build_selectors(),
      deps: value |> field_value(:deps, :all) |> decode_known_atom([:all, :none]),
      schedule: value |> field_value(:schedule) |> decode_pipeline_schedule(),
      window: value |> field_value(:window) |> build_window_policy(),
      source: value |> field_value(:source) |> decode_atom_optional(),
      outputs: value |> field_value(:outputs, []) |> build_atom_list(),
      config: value |> field_value(:config, %{}) |> plain_map(),
      metadata: value |> field_value(:metadata, %{}) |> build_metadata()
    }
  end

  defp build_pipeline(other), do: other

  defp build_window_policy(nil), do: nil
  defp build_window_policy(%Policy{} = policy), do: policy
  defp build_window_policy(value), do: Policy.from_value!(value)

  defp build_selectors(values) when is_list(values), do: Enum.map(values, &decode_selector/1)
  defp build_selectors(_other), do: []

  defp decode_selector({kind, value}) when is_atom(kind), do: decode_selector([kind, value])

  defp decode_selector([kind, value]) when kind in [:asset, "asset"],
    do: {:asset, decode_selector_asset(value)}

  defp decode_selector([kind, value]) when kind in [:module, "module"],
    do: {:module, decode_module(value)}

  defp decode_selector([kind, value]) when kind in [:tag, "tag"],
    do: {:tag, decode_atom_or_binary(value)}

  defp decode_selector([kind, value]) when kind in [:category, "category"],
    do: {:category, decode_atom_or_binary(value)}

  defp decode_selector(value) when is_map(value) do
    case value |> field_value(:module) |> decode_atom_or_binary() do
      :asset -> {:asset, value |> field_value(:name) |> decode_selector_asset()}
      :module -> {:module, value |> field_value(:name) |> decode_module()}
      :tag -> {:tag, value |> field_value(:name) |> decode_atom_or_binary()}
      :category -> {:category, value |> field_value(:name) |> decode_atom_or_binary()}
      _other -> value
    end
  end

  defp decode_selector(other), do: other

  defp decode_selector_asset(value) do
    case decode_ref(value) do
      {module, name} = ref when is_atom(module) and is_atom(name) -> ref
      other -> decode_module(other)
    end
  end

  defp decode_pipeline_schedule(nil), do: nil
  defp decode_pipeline_schedule(%Schedule{} = value), do: value
  defp decode_pipeline_schedule({:ref, ref}), do: {:ref, decode_ref(ref)}
  defp decode_pipeline_schedule({:inline, schedule}), do: {:inline, build_schedule(schedule)}

  defp decode_pipeline_schedule([kind, value]) when kind in [:ref, "ref"],
    do: {:ref, decode_ref(value)}

  defp decode_pipeline_schedule([kind, value]) when kind in [:inline, "inline"],
    do: {:inline, build_schedule(value)}

  defp decode_pipeline_schedule(value) when is_map(value), do: build_schedule(value)
  defp decode_pipeline_schedule(other), do: other

  defp build_schedules(values) when is_list(values), do: Enum.map(values, &build_schedule/1)
  defp build_schedules(_other), do: []

  defp build_schedule(value) when is_map(value) do
    %Schedule{
      module: value |> field_value(:module) |> decode_module(),
      name: value |> field_value(:name) |> decode_atom_optional(),
      ref: value |> field_value(:ref) |> decode_ref(),
      kind: value |> field_value(:kind, :cron) |> decode_known_atom([:cron]),
      cron: field_value(value, :cron),
      timezone: field_value(value, :timezone),
      missed: value |> field_value(:missed, :skip) |> decode_known_atom([:skip, :one, :all]),
      overlap:
        value
        |> field_value(:overlap, :forbid)
        |> decode_known_atom([:forbid, :allow, :queue_one]),
      active: field_value(value, :active, true),
      origin: value |> field_value(:origin, :named) |> decode_known_atom([:inline, :named])
    }
  end

  defp build_schedule(other), do: other

  defp build_graph(%Graph{} = value), do: value

  defp build_graph(value) when is_map(value) do
    %Graph{
      nodes: value |> field_value(:nodes, []) |> build_refs(),
      edges: value |> field_value(:edges, []) |> build_edges(),
      topo_order: value |> field_value(:topo_order, []) |> build_refs()
    }
  end

  defp build_graph(_other), do: %Graph{}

  defp build_edges(values) when is_list(values) do
    Enum.map(values, fn
      edge when is_map(edge) ->
        %{
          from: edge |> field_value(:from) |> decode_ref(),
          to: edge |> field_value(:to) |> decode_ref()
        }

      other ->
        plain_map(other)
    end)
  end

  defp build_edges(_other), do: []

  defp build_refs(values) when is_list(values), do: Enum.map(values, &decode_ref/1)
  defp build_refs(_other), do: []

  defp decode_ref(nil), do: nil
  defp decode_ref({module, name}) when is_atom(module) and is_atom(name), do: {module, name}
  defp decode_ref([module, name]), do: {decode_module(module), decode_atom_optional(name)}

  defp decode_ref(value) when is_map(value) do
    {value |> field_value(:module) |> decode_module(),
     value |> field_value(:name) |> decode_atom_optional()}
  end

  defp decode_ref(other), do: other

  defp build_atom_list(values) when is_list(values), do: Enum.map(values, &decode_atom_optional/1)
  defp build_atom_list(_other), do: []

  defp build_string_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  defp build_string_list(_other), do: []

  defp build_metadata(value) when is_map(value) do
    plain = plain_map(value)

    plain
    |> Map.delete("category")
    |> Map.delete("tags")
    |> maybe_put(:category, field_value(value, :category) |> decode_atom_or_binary())
    |> maybe_put(:tags, field_value(value, :tags) |> build_metadata_tags())
  end

  defp build_metadata(_other), do: %{}

  defp build_metadata_tags(values) when is_list(values),
    do: Enum.map(values, &decode_atom_or_binary/1)

  defp build_metadata_tags(_other), do: nil

  defp build_template_span(nil), do: nil

  defp build_template_span(%Template.Span{} = value), do: value

  defp build_template_span(value) when is_map(value) do
    %Template.Span{
      start_offset: field_value(value, :start_offset),
      end_offset: field_value(value, :end_offset),
      start_line: field_value(value, :start_line),
      start_column: field_value(value, :start_column),
      end_line: field_value(value, :end_line),
      end_column: field_value(value, :end_column)
    }
  end

  defp build_template_span(other), do: other

  defp template_text?(value), do: map_has_key?(value, :sql) and map_has_key?(value, :span)

  defp template_relation?(value),
    do:
      map_has_key?(value, :raw) and map_has_key?(value, :segments) and map_has_key?(value, :span)

  defp template_placeholder?(value),
    do: map_has_key?(value, :name) and map_has_key?(value, :source) and map_has_key?(value, :span)

  defp template_call?(value),
    do:
      map_has_key?(value, :definition) and map_has_key?(value, :args) and
        map_has_key?(value, :context) and map_has_key?(value, :span)

  defp template_asset_ref?(value),
    do:
      map_has_key?(value, :module) and map_has_key?(value, :asset_ref) and
        map_has_key?(value, :resolution) and map_has_key?(value, :span)

  defp template_fragment?(value), do: map_has_key?(value, :nodes) and map_has_key?(value, :span)

  defp plain_map_or_nil(nil), do: nil
  defp plain_map_or_nil(value), do: plain_map(value)

  defp plain_map(%_{} = value), do: value |> Map.from_struct() |> plain_map()

  defp plain_map(value) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      Map.put(acc, key, plain_value(item))
    end)
  end

  defp plain_map(value) when is_list(value), do: Enum.map(value, &plain_value/1)
  defp plain_map(other), do: other

  defp plain_value(%_{} = value), do: value |> Map.from_struct() |> plain_map()
  defp plain_value(value) when is_map(value), do: plain_map(value)
  defp plain_value(value) when is_list(value), do: Enum.map(value, &plain_value/1)
  defp plain_value(other), do: other

  defp map_has_key?(value, key),
    do: Map.has_key?(value, key) or Map.has_key?(value, Atom.to_string(key))

  defp field_value(value, field, default \\ nil)

  defp field_value(value, field, default) when is_map(value) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> Map.get(value, atom_key)
      Map.has_key?(value, string_key) -> Map.get(value, string_key)
      true -> default
    end
  end

  defp field_value(_value, _field, default), do: default

  defp decode_module(nil), do: nil
  defp decode_module(value) when is_atom(value), do: value

  defp decode_module("Elixir." <> _rest = value) when is_binary(value),
    do: decode_manifest_module!(value)

  defp decode_module(value) when is_binary(value), do: decode_existing_atom!(value)
  defp decode_module(other), do: other

  defp decode_atom_optional(nil), do: nil
  defp decode_atom_optional(value) when is_atom(value), do: value
  defp decode_atom_optional(value) when is_binary(value), do: decode_manifest_atom!(value)
  defp decode_atom_optional(other), do: other

  defp decode_known_atom(value, allowed) do
    allowed_strings = Enum.map(allowed, &Atom.to_string/1)

    cond do
      is_atom(value) and value in allowed ->
        value

      is_binary(value) and value in allowed_strings ->
        String.to_existing_atom(value)

      true ->
        raise ArgumentError,
              "invalid enum value #{inspect(value)} expected one of #{inspect(allowed)}"
    end
  end

  defp decode_known_atom_optional(nil, _allowed), do: nil
  defp decode_known_atom_optional(value, allowed), do: decode_known_atom(value, allowed)

  defp decode_atom_or_binary(value) when is_atom(value), do: value

  defp decode_atom_or_binary(value) when is_binary(value) do
    case maybe_existing_atom(value) do
      {:ok, atom} -> atom
      :error -> value
    end
  end

  defp decode_atom_or_binary(other), do: other

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp decode_existing_atom!(value) do
    case maybe_existing_atom(value) do
      {:ok, atom} -> atom
      :error -> raise ArgumentError, "unknown atom #{inspect(value)}"
    end
  end

  defp decode_manifest_module!(value) do
    if valid_manifest_module?(value) do
      String.to_atom(value)
    else
      raise ArgumentError, "invalid module reference #{inspect(value)}"
    end
  end

  defp decode_manifest_atom!(value) do
    if valid_manifest_atom?(value) do
      String.to_atom(value)
    else
      raise ArgumentError, "invalid atom reference #{inspect(value)}"
    end
  end

  defp valid_manifest_module?(value) when is_binary(value) do
    byte_size(value) <= @max_manifest_module_length and
      Regex.match?(~r/^Elixir\.[A-Z][A-Za-z0-9_]*(\.[A-Z][A-Za-z0-9_]*)*$/, value)
  end

  defp valid_manifest_atom?(value) when is_binary(value) do
    byte_size(value) in 1..@max_manifest_atom_length and
      Regex.match?(~r/^[A-Za-z_][A-Za-z0-9_]*[!?=]?$/, value)
  end

  defp maybe_existing_atom(value) when is_binary(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> :error
  end
end
