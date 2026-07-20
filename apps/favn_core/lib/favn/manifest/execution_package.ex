defmodule Favn.Manifest.ExecutionPackage do
  @moduledoc """
  Immutable, content-addressed SQL execution artifact for one manifest asset.

  Manifest indexes contain only `content_hash`. The complete SQL execution tree
  is persisted and transferred independently so catalogue operations scale with
  compact asset metadata rather than generated SQL template size.
  """

  alias Favn.Manifest.Rehydrate
  alias Favn.Manifest.Asset
  alias Favn.Manifest.SQLExecution
  alias Favn.Manifest.Serializer
  alias Favn.RelationRef
  alias Favn.SQL.Check
  alias Favn.SQL.Definition
  alias Favn.SQLAsset.Compiler
  alias Favn.SQL.Template

  alias Favn.SQL.Template.{
    AssetRef,
    Call,
    DefinitionRef,
    Fragment,
    Placeholder,
    Relation,
    Requirements,
    RuntimeRelation,
    Span,
    Text
  }

  @schema_version 2

  @enforce_keys [:content_hash, :asset_ref, :sql_execution]
  defstruct schema_version: @schema_version,
            content_hash: nil,
            asset_ref: nil,
            sql_execution: nil

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          content_hash: String.t(),
          asset_ref: Favn.Ref.t(),
          sql_execution: SQLExecution.t()
        }

  @type error ::
          :invalid_execution_package
          | {:invalid_execution_package_hash, term()}
          | {:execution_package_hash_mismatch, String.t(), String.t()}
          | {:execution_package_asset_mismatch, Favn.Ref.t(), Favn.Ref.t()}
          | {:execution_package_not_required, Favn.Ref.t() | nil}
          | {:unsupported_execution_package_schema, term(), pos_integer()}
          | Rehydrate.error()
          | Serializer.error()

  @doc "Builds and hashes an immutable package for one SQL asset."
  @spec new(Favn.Ref.t(), SQLExecution.t()) :: {:ok, t()} | {:error, error()}
  def new({module, name} = asset_ref, %SQLExecution{} = execution)
      when is_atom(module) and is_atom(name) do
    with {:ok, canonical, encoded} <-
           canonicalize_payload(@schema_version, asset_ref, execution) do
      {:ok, %{canonical | content_hash: hash_bytes(encoded)}}
    end
  end

  def new(_asset_ref, _execution), do: {:error, :invalid_execution_package}

  @doc "Builds the execution package for a compiled SQL asset descriptor."
  @spec from_asset(map()) :: {:ok, t() | nil} | {:error, term()}
  def from_asset(%{type: :sql, ref: {module, name} = ref, module: module})
      when is_atom(module) and is_atom(name) do
    with {:ok, definition} <- Compiler.fetch_definition(module) do
      new(ref, SQLExecution.from_definition(definition))
    end
  end

  def from_asset(%{type: type}) when type in [:elixir, :source], do: {:ok, nil}
  def from_asset(_asset), do: {:error, :invalid_execution_package}

  @doc "Rehydrates and verifies a package received across a persistence boundary."
  @spec from_published(map() | t()) :: {:ok, t()} | {:error, error()}
  def from_published(%__MODULE__{} = package), do: verify(package)

  def from_published(value) when is_map(value) do
    with {:ok, published_encoded} <- Serializer.encode_manifest(value),
         {:ok, package} <- Rehydrate.execution_package(value),
         {:ok, canonical} <- verify(package),
         {:ok, canonical_encoded} <- Serializer.encode_manifest(canonical),
         :ok <- require_canonical_round_trip(published_encoded, canonical_encoded) do
      {:ok, canonical}
    end
  end

  def from_published(_value), do: {:error, :invalid_execution_package}

  @doc "Verifies the package schema and canonical content hash."
  @spec verify(t()) :: {:ok, t()} | {:error, error()}
  def verify(%__MODULE__{} = package) do
    with :ok <- validate_schema(package.schema_version),
         :ok <- validate_hash(package.content_hash),
         {:ok, canonical, encoded} <-
           canonicalize_payload(
             package.schema_version,
             package.asset_ref,
             package.sql_execution
           ),
         computed_hash = hash_bytes(encoded),
         :ok <- match_hash(computed_hash, package.content_hash) do
      {:ok, %{canonical | content_hash: computed_hash}}
    end
  end

  @doc "Verifies that a package is the artifact referenced by an indexed asset."
  @spec verify_for_asset(t() | map() | nil, Asset.t()) ::
          {:ok, t() | nil} | {:error, error() | :execution_package_required}
  def verify_for_asset(nil, %Asset{type: :sql}), do: {:error, :execution_package_required}

  def verify_for_asset(package, %Asset{type: :sql} = asset) do
    with {:ok, canonical} <- from_published(package),
         :ok <- match_asset_ref(canonical.asset_ref, asset.ref),
         :ok <- match_hash(canonical.content_hash, asset.execution_package_hash) do
      {:ok, canonical}
    end
  end

  def verify_for_asset(nil, %Asset{}), do: {:ok, nil}

  def verify_for_asset(_package, %Asset{ref: asset_ref}),
    do: {:error, {:execution_package_not_required, asset_ref}}

  @doc "Returns the current execution-package schema version."
  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @schema_version

  defp canonical_payload(schema_version, asset_ref, execution) do
    %{
      schema_version: schema_version,
      asset_ref: asset_ref,
      sql_execution: execution
    }
  end

  defp canonicalize_payload(schema_version, asset_ref, execution) do
    with {:ok, encoded} <-
           Serializer.encode_manifest(canonical_payload(schema_version, asset_ref, execution)),
         {:ok, decoded} <- Serializer.decode_manifest(encoded),
         {:ok, canonical} <- Rehydrate.execution_package(decoded),
         :ok <- validate_schema(canonical.schema_version),
         :ok <- validate_payload(canonical.asset_ref, canonical.sql_execution),
         {:ok, canonical_encoded} <-
           Serializer.encode_manifest(
             canonical_payload(
               canonical.schema_version,
               canonical.asset_ref,
               canonical.sql_execution
             )
           ),
         :ok <- require_canonical_round_trip(encoded, canonical_encoded) do
      {:ok, canonical, canonical_encoded}
    end
  end

  defp hash_bytes(bytes),
    do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp require_canonical_round_trip(encoded, encoded), do: :ok

  defp require_canonical_round_trip(_original, _canonical),
    do: {:error, :invalid_execution_package}

  defp validate_schema(@schema_version), do: :ok

  defp validate_schema(actual),
    do: {:error, {:unsupported_execution_package_schema, actual, @schema_version}}

  defp validate_hash(hash) when is_binary(hash) do
    if canonical_hash?(hash),
      do: :ok,
      else: {:error, {:invalid_execution_package_hash, hash}}
  end

  defp validate_hash(hash), do: {:error, {:invalid_execution_package_hash, hash}}

  defp canonical_hash?(hash), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)

  defp validate_payload(
         {module, name},
         %SQLExecution{} = execution
       )
       when is_atom(module) and is_atom(name) do
    validate_sql_execution!(execution)
    :ok
  rescue
    _error -> {:error, :invalid_execution_package}
  end

  defp validate_payload(_asset_ref, _execution), do: {:error, :invalid_execution_package}

  defp validate_sql_execution!(%SQLExecution{
         sql: sql,
         template: template,
         sql_definitions: definitions,
         checks: checks
       }) do
    ensure!(is_binary(sql))
    validate_template!(template, sql, :query)
    ensure!(is_list(definitions))
    Enum.each(definitions, &validate_definition!/1)
    validate_unique_definitions!(definitions)
    ensure!(is_list(checks))
    Check.validate_list!(checks)
    Enum.each(checks, &validate_check!/1)

    validate_called_definitions!(
      [template | Enum.map(definitions, & &1.template)] ++
        Enum.map(checks, & &1.template),
      definitions
    )
  end

  defp validate_definition!(%Definition{} = definition) do
    ensure!(valid_atom?(definition.module))
    ensure!(valid_atom?(definition.name))
    ensure!(non_neg_integer?(definition.arity))
    ensure!(is_list(definition.params))
    ensure!(length(definition.params) == definition.arity)
    Enum.each(definition.params, &validate_definition_param!/1)

    ensure!(
      Enum.map(definition.params, & &1.index) ==
        Enum.to_list(0..(definition.arity - 1)//1)
    )

    ensure!(definition.shape in [:expression, :relation])
    ensure!(is_binary(definition.sql))

    validate_template!(
      definition.template,
      definition.sql,
      {:definition, definition.shape, Map.new(definition.params, &{&1.name, &1.index})}
    )

    ensure!(is_binary(definition.file))
    ensure!(pos_integer?(definition.line))
    ensure!(is_binary(definition.declared_file))
    ensure!(pos_integer?(definition.declared_line))
    validate_relation_defaults!(definition.relation_defaults)
  end

  defp validate_definition!(_definition), do: invalid!()

  defp validate_definition_param!(%Definition.Param{name: name, index: index}) do
    ensure!(valid_atom?(name))
    ensure!(non_neg_integer?(index))
  end

  defp validate_definition_param!(_param), do: invalid!()

  defp validate_unique_definitions!(definitions) do
    keys = Enum.map(definitions, &Definition.key/1)
    ensure!(length(keys) == MapSet.size(MapSet.new(keys)))
  end

  defp validate_called_definitions!(templates, definitions) do
    available = Map.new(definitions, &{Definition.key(&1), &1})

    Enum.each(templates, fn template ->
      template
      |> Template.calls()
      |> Enum.each(&validate_call_contract!(&1, available))
    end)

    validate_definition_cycles!(definitions)
  end

  defp validate_call_contract!(
         %Call{
           definition: %DefinitionRef{} = reference,
           context: context
         },
         available
       ) do
    definition = Map.fetch!(available, {reference.name, reference.arity})
    ensure!(reference.provider == definition.module)
    ensure!(reference.kind == definition.shape)
    ensure!(context == call_context(definition.shape))
  end

  defp validate_definition_root_kind!(:expression, :expression), do: :ok
  defp validate_definition_root_kind!(:relation, :query), do: :ok
  defp validate_definition_root_kind!(_shape, _root_kind), do: invalid!()

  defp call_context(:expression), do: :expression
  defp call_context(:relation), do: :relation

  defp validate_definition_cycles!(definitions) do
    graph =
      Map.new(definitions, fn definition ->
        {Definition.key(definition), Template.called_definition_keys(definition.template)}
      end)

    Enum.reduce(Map.keys(graph), %{}, fn key, states ->
      visit_definition!(key, graph, states)
    end)
  end

  defp visit_definition!(key, graph, states) do
    case Map.get(states, key) do
      :visited ->
        states

      :visiting ->
        invalid!()

      nil ->
        states = Map.put(states, key, :visiting)

        states =
          graph
          |> Map.fetch!(key)
          |> Enum.reduce(states, fn child, acc -> visit_definition!(child, graph, acc) end)

        Map.put(states, key, :visited)
    end
  end

  defp validate_check!(%Check{} = check) do
    ensure!(is_binary(check.sql))
    validate_template!(check.template, check.sql, :query)
    ensure!(is_nil(check.file) or is_binary(check.file))
    ensure!(is_nil(check.line) or pos_integer?(check.line))
  end

  defp validate_check!(_check), do: invalid!()

  defp validate_template!(
         %Template{
           source: source,
           root_kind: root_kind,
           nodes: nodes,
           span: span,
           requires: requirements
         },
         expected_source,
         role
       ) do
    ensure!(is_binary(source) and source == expected_source)
    ensure!(root_kind in [:query, :expression])
    validate_nodes!(nodes)
    validate_span!(span)
    validate_requirements!(requirements, nodes)
    validate_template_role!(role, root_kind, nodes)
  end

  defp validate_template!(_template, _expected_source, _role), do: invalid!()

  defp validate_template_role!(:query, :query, nodes) do
    ensure!(collect_local_placeholders(nodes) == [])
  end

  defp validate_template_role!(
         {:definition, shape, params},
         root_kind,
         nodes
       ) do
    validate_definition_root_kind!(shape, root_kind)

    Enum.each(collect_local_placeholders(nodes), fn {name, index} ->
      ensure!(Map.get(params, name) == index)
    end)
  end

  defp validate_template_role!(_role, _root_kind, _nodes), do: invalid!()

  defp validate_nodes!(nodes) when is_list(nodes), do: Enum.each(nodes, &validate_node!/1)
  defp validate_nodes!(_nodes), do: invalid!()

  defp validate_node!(%Text{sql: sql, span: span}) do
    ensure!(is_binary(sql))
    validate_span!(span)
  end

  defp validate_node!(%Placeholder{name: name, source: :runtime, span: span}) do
    ensure!(valid_atom?(name))
    validate_span!(span)
  end

  defp validate_node!(%Placeholder{name: name, source: :query_param, span: span}) do
    ensure!(is_binary(name))
    validate_span!(span)
  end

  defp validate_node!(%Placeholder{name: name, source: {:local_arg, index}, span: span}) do
    ensure!(valid_atom?(name))
    ensure!(non_neg_integer?(index))
    validate_span!(span)
  end

  defp validate_node!(%Call{
         definition: definition,
         args: args,
         context: context,
         span: span
       }) do
    validate_definition_ref!(definition)
    ensure!(is_list(args) and length(args) == definition.arity)
    Enum.each(args, &validate_fragment!/1)
    ensure!(context in [:expression, :relation])
    validate_span!(span)
  end

  defp validate_node!(%AssetRef{
         module: module,
         asset_ref: {asset_module, :asset},
         relation: relation,
         resolution: resolution,
         span: span
       }) do
    ensure!(valid_atom?(module) and asset_module == module)
    ensure!(resolution in [:resolved, :deferred])
    validate_asset_relation!(resolution, relation)
    validate_span!(span)
  end

  defp validate_node!(%Relation{raw: raw, segments: segments, span: span}) do
    ensure!(is_binary(raw))

    ensure!(is_list(segments) and length(segments) in 1..3 and Enum.all?(segments, &is_binary/1))

    validate_span!(span)
  end

  defp validate_node!(%RuntimeRelation{kind: kind, span: span}) do
    ensure!(kind in [:query, :target])
    validate_span!(span)
  end

  defp validate_node!(_node), do: invalid!()

  defp validate_definition_ref!(%DefinitionRef{
         provider: provider,
         name: name,
         arity: arity,
         kind: kind
       }) do
    ensure!(valid_atom?(provider))
    ensure!(valid_atom?(name))
    ensure!(non_neg_integer?(arity))
    ensure!(kind in [:expression, :relation])
  end

  defp validate_definition_ref!(_definition), do: invalid!()

  defp validate_fragment!(%Fragment{nodes: nodes, span: span}) do
    validate_nodes!(nodes)
    validate_span!(span)
  end

  defp validate_fragment!(_fragment), do: invalid!()

  defp validate_asset_relation!(:resolved, %RelationRef{} = relation),
    do: RelationRef.validate!(relation)

  defp validate_asset_relation!(:deferred, nil), do: :ok
  defp validate_asset_relation!(_resolution, _relation), do: invalid!()

  defp validate_span!(%Span{
         start_offset: start_offset,
         end_offset: end_offset,
         start_line: start_line,
         start_column: start_column,
         end_line: end_line,
         end_column: end_column
       }) do
    ensure!(non_neg_integer?(start_offset))
    ensure!(non_neg_integer?(end_offset) and end_offset >= start_offset)
    ensure!(pos_integer?(start_line))
    ensure!(pos_integer?(start_column))
    ensure!(pos_integer?(end_line) and end_line >= start_line)
    ensure!(pos_integer?(end_column))
  end

  defp validate_span!(_span), do: invalid!()

  defp validate_requirements!(
         %Requirements{runtime_inputs: runtime_inputs, query_params: query_params},
         nodes
       ) do
    ensure!(match?(%MapSet{}, runtime_inputs))
    ensure!(match?(%MapSet{}, query_params))
    ensure!(Enum.all?(runtime_inputs, &valid_atom?/1))
    ensure!(Enum.all?(query_params, &is_binary/1))

    {actual_runtime_inputs, actual_query_params} = collect_requirements(nodes)
    ensure!(MapSet.equal?(runtime_inputs, actual_runtime_inputs))
    ensure!(MapSet.equal?(query_params, actual_query_params))
  end

  defp validate_requirements!(_requirements, _nodes), do: invalid!()

  defp collect_requirements(nodes) do
    Enum.reduce(nodes, {MapSet.new(), MapSet.new()}, fn
      %Placeholder{name: name, source: :runtime}, {runtime_inputs, query_params} ->
        {MapSet.put(runtime_inputs, name), query_params}

      %Placeholder{name: name, source: :query_param}, {runtime_inputs, query_params} ->
        {runtime_inputs, MapSet.put(query_params, name)}

      %Call{args: args}, acc ->
        Enum.reduce(args, acc, fn %Fragment{nodes: arg_nodes}, nested_acc ->
          merge_requirements(nested_acc, collect_requirements(arg_nodes))
        end)

      _node, acc ->
        acc
    end)
  end

  defp collect_local_placeholders(nodes) do
    Enum.flat_map(nodes, fn
      %Placeholder{name: name, source: {:local_arg, index}} ->
        [{name, index}]

      %Call{args: args} ->
        Enum.flat_map(args, fn %Fragment{nodes: arg_nodes} ->
          collect_local_placeholders(arg_nodes)
        end)

      _node ->
        []
    end)
  end

  defp merge_requirements({runtime_a, query_a}, {runtime_b, query_b}) do
    {MapSet.union(runtime_a, runtime_b), MapSet.union(query_a, query_b)}
  end

  defp validate_relation_defaults!(defaults) when is_map(defaults) do
    ensure!(Enum.all?(Map.keys(defaults), &(&1 in [:connection, :catalog, :schema])))

    ensure!(
      Enum.all?(defaults, fn
        {:connection, value} -> is_nil(value) or is_atom(value)
        {field, value} when field in [:catalog, :schema] -> is_nil(value) or is_binary(value)
      end)
    )
  end

  defp validate_relation_defaults!(_defaults), do: invalid!()

  defp valid_atom?(value), do: is_atom(value) and not is_nil(value)
  defp non_neg_integer?(value), do: is_integer(value) and value >= 0
  defp pos_integer?(value), do: is_integer(value) and value > 0

  defp ensure!(true), do: :ok
  defp ensure!(false), do: invalid!()

  defp invalid!(), do: raise(ArgumentError, "invalid execution package payload")

  defp match_hash(hash, hash), do: :ok

  defp match_hash(computed, expected),
    do: {:error, {:execution_package_hash_mismatch, expected, computed}}

  defp match_asset_ref(ref, ref), do: :ok

  defp match_asset_ref(actual, expected),
    do: {:error, {:execution_package_asset_mismatch, expected, actual}}
end
