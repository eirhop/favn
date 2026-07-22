defmodule Favn.Manifest.TargetDescriptor do
  @moduledoc """
  Canonical desired identity for one persisted SQL table target.

  The descriptor separates physical-compatibility fingerprints from metadata
  such as effective coverage. Compatibility code compares the named fields;
  `descriptor_hash` identifies the complete desired descriptor.
  """

  alias Favn.Coverage.Effective, as: EffectiveCoverage
  alias Favn.Manifest.Serializer
  alias Favn.RelationRef
  alias Favn.TimePeriod
  alias Favn.Window.Spec, as: WindowSpec

  @schema_version 1
  @hash_pattern ~r/\A[0-9a-f]{64}\z/
  @period_kinds ~w(hour day month year)
  @strategies ~w(append replace delete_insert merge)

  @type t :: %__MODULE__{
          schema_version: pos_integer(),
          target_id: String.t(),
          relation: map(),
          adapter: String.t(),
          connection_identity: map(),
          materialization: map(),
          write_semantics: map(),
          execution_package_hash: String.t(),
          contract_fingerprint: String.t() | nil,
          grain_fingerprint: String.t() | nil,
          window_identity: map() | nil,
          window_identity_fingerprint: String.t() | nil,
          coverage: map() | nil,
          manifest_schema_version: pos_integer(),
          runner_contract_version: pos_integer(),
          descriptor_hash: String.t()
        }

  @enforce_keys [
    :target_id,
    :relation,
    :connection_identity,
    :materialization,
    :write_semantics,
    :execution_package_hash,
    :manifest_schema_version,
    :runner_contract_version,
    :descriptor_hash
  ]
  defstruct [
    :target_id,
    :relation,
    :adapter,
    :connection_identity,
    :materialization,
    :write_semantics,
    :execution_package_hash,
    :contract_fingerprint,
    :grain_fingerprint,
    :window_identity,
    :window_identity_fingerprint,
    :coverage,
    :manifest_schema_version,
    :runner_contract_version,
    :descriptor_hash,
    schema_version: @schema_version
  ]

  @doc "Builds a descriptor for a persisted SQL table, or nil for other assets."
  @spec from_asset(map(), keyword()) :: t() | nil
  def from_asset(asset, opts \\ []) when is_map(asset) and is_list(opts) do
    if persisted_table?(asset) do
      relation = asset |> Map.fetch!(:relation) |> RelationRef.validate!()
      connection = connection_definition!(relation.connection, opts)
      contract = asset |> Map.get(:assurance) |> contract()
      window_identity = window_identity(Map.get(asset, :window))

      descriptor = %__MODULE__{
        target_id: target_id(Map.fetch!(asset, :ref)),
        relation: relation_identity(relation),
        adapter: connection.adapter |> Atom.to_string(),
        connection_identity: %{
          name: identifier(relation.connection),
          definition_module: module_name(Map.get(connection, :module))
        },
        materialization: materialization_identity(Map.fetch!(asset, :materialization)),
        write_semantics: write_semantics(Map.fetch!(asset, :materialization)),
        execution_package_hash: Map.fetch!(asset, :execution_package_hash),
        contract_fingerprint: fingerprint(contract_columns(contract)),
        grain_fingerprint: fingerprint(contract_grain_and_keys(contract)),
        window_identity: window_identity,
        window_identity_fingerprint: fingerprint(window_identity),
        coverage: coverage_identity(Map.get(asset, :coverage)),
        manifest_schema_version: Keyword.fetch!(opts, :manifest_schema_version),
        runner_contract_version: Keyword.fetch!(opts, :runner_contract_version),
        descriptor_hash: ""
      }

      %{descriptor | descriptor_hash: descriptor |> hash_payload() |> hash()}
    end
  end

  @doc "Returns a deterministic semantic evidence generation id for a manifest asset."
  @spec semantic_generation_id(map(), term()) :: String.t()
  def semantic_generation_id(asset, execution_identity \\ nil) when is_map(asset) do
    payload = %{
      ref: Map.get(asset, :ref),
      type: Map.get(asset, :type, :elixir),
      execution: Map.get(asset, :execution),
      execution_package_hash: Map.get(asset, :execution_package_hash),
      execution_identity: execution_identity,
      relation: Map.get(asset, :relation),
      materialization: Map.get(asset, :materialization),
      window: window_identity(Map.get(asset, :window))
    }

    "ag_" <> hash(payload)
  end

  @doc "Rehydrates and validates a descriptor from a decoded manifest value."
  @spec from_value(term()) :: {:ok, t() | nil} | {:error, term()}
  def from_value(nil), do: {:ok, nil}
  def from_value(%__MODULE__{} = descriptor), do: validate(descriptor)

  def from_value(value) when is_map(value) do
    with :ok <- reject_unknown_fields(value) do
      descriptor = %__MODULE__{
        schema_version: field(value, :schema_version, @schema_version),
        target_id: field(value, :target_id),
        relation: value |> field(:relation) |> canonical_relation(),
        adapter: field(value, :adapter),
        connection_identity:
          value |> field(:connection_identity) |> canonical_connection_identity(),
        materialization: value |> field(:materialization) |> canonical_materialization(),
        write_semantics: value |> field(:write_semantics) |> canonical_write_semantics(),
        execution_package_hash: field(value, :execution_package_hash),
        contract_fingerprint: field(value, :contract_fingerprint),
        grain_fingerprint: field(value, :grain_fingerprint),
        window_identity: value |> field(:window_identity) |> canonical_window_identity(),
        window_identity_fingerprint: field(value, :window_identity_fingerprint),
        coverage: value |> field(:coverage) |> canonical_coverage(),
        manifest_schema_version: field(value, :manifest_schema_version),
        runner_contract_version: field(value, :runner_contract_version),
        descriptor_hash: field(value, :descriptor_hash)
      }

      validate(descriptor)
    end
  end

  def from_value(value), do: {:error, {:invalid_target_descriptor, value}}

  @doc "Validates the complete descriptor and its canonical hash."
  @spec validate(t()) :: {:ok, t()} | {:error, term()}
  def validate(%__MODULE__{} = descriptor) do
    cond do
      descriptor.schema_version != @schema_version ->
        {:error, {:unsupported_target_descriptor_schema, descriptor.schema_version}}

      not nonempty_string?(descriptor.target_id) ->
        {:error, {:invalid_target_id, descriptor.target_id}}

      not valid_relation?(descriptor.relation) ->
        {:error, {:invalid_target_relation, descriptor.relation}}

      not nonempty_string?(descriptor.adapter) ->
        {:error, {:invalid_target_adapter, descriptor.adapter}}

      not valid_connection_identity?(descriptor.connection_identity) ->
        {:error, {:invalid_target_connection_identity, descriptor.connection_identity}}

      not valid_materialization?(descriptor.materialization) or
          not valid_write_semantics?(descriptor.write_semantics) ->
        {:error, :invalid_target_materialization}

      not canonical_hash?(descriptor.execution_package_hash) ->
        {:error, {:invalid_target_execution_package_hash, descriptor.execution_package_hash}}

      not optional_hash?(descriptor.contract_fingerprint) ->
        {:error, {:invalid_target_contract_fingerprint, descriptor.contract_fingerprint}}

      not optional_hash?(descriptor.grain_fingerprint) ->
        {:error, {:invalid_target_grain_fingerprint, descriptor.grain_fingerprint}}

      not valid_window_identity?(descriptor.window_identity) ->
        {:error, {:invalid_target_window_identity, descriptor.window_identity}}

      descriptor.window_identity_fingerprint != fingerprint(descriptor.window_identity) ->
        {:error, :target_window_fingerprint_mismatch}

      not valid_coverage?(descriptor.coverage) ->
        {:error, {:invalid_target_coverage, descriptor.coverage}}

      not positive_integer?(descriptor.manifest_schema_version) or
          not positive_integer?(descriptor.runner_contract_version) ->
        {:error, :invalid_target_contract_versions}

      not canonical_hash?(descriptor.descriptor_hash) ->
        {:error, {:invalid_target_descriptor_hash, descriptor.descriptor_hash}}

      descriptor.descriptor_hash != descriptor |> hash_payload() |> hash() ->
        {:error, :target_descriptor_hash_mismatch}

      true ->
        {:ok, descriptor}
    end
  end

  @doc false
  @spec validate_asset(t(), map(), pos_integer(), pos_integer()) :: :ok | {:error, term()}
  def validate_asset(descriptor, asset, manifest_schema_version, runner_contract_version)
      when is_map(asset) do
    relation = asset |> Map.fetch!(:relation) |> RelationRef.validate!()
    window = window_identity(Map.get(asset, :window))
    contract = asset |> Map.get(:assurance) |> contract()

    expected = %{
      target_id: target_id(Map.fetch!(asset, :ref)),
      relation: relation_identity(relation),
      connection_name: identifier(relation.connection),
      materialization: materialization_identity(Map.fetch!(asset, :materialization)),
      write_semantics: write_semantics(Map.fetch!(asset, :materialization)),
      execution_package_hash: Map.fetch!(asset, :execution_package_hash),
      contract_fingerprint: fingerprint(contract_columns(contract)),
      grain_fingerprint: fingerprint(contract_grain_and_keys(contract)),
      window_identity: window,
      window_identity_fingerprint: fingerprint(window),
      coverage: coverage_identity(Map.get(asset, :coverage)),
      manifest_schema_version: manifest_schema_version,
      runner_contract_version: runner_contract_version
    }

    with {:ok, descriptor} <- validate(descriptor),
         :ok <- match_field(:target_id, descriptor.target_id, expected.target_id),
         :ok <- match_field(:relation, descriptor.relation, expected.relation),
         :ok <-
           match_field(
             :connection_name,
             descriptor.connection_identity.name,
             expected.connection_name
           ),
         :ok <-
           match_field(:materialization, descriptor.materialization, expected.materialization),
         :ok <-
           match_field(:write_semantics, descriptor.write_semantics, expected.write_semantics),
         :ok <-
           match_field(
             :execution_package_hash,
             descriptor.execution_package_hash,
             expected.execution_package_hash
           ),
         :ok <-
           match_field(
             :contract_fingerprint,
             descriptor.contract_fingerprint,
             expected.contract_fingerprint
           ),
         :ok <-
           match_field(
             :grain_fingerprint,
             descriptor.grain_fingerprint,
             expected.grain_fingerprint
           ),
         :ok <-
           match_field(:window_identity, descriptor.window_identity, expected.window_identity),
         :ok <-
           match_field(
             :window_identity_fingerprint,
             descriptor.window_identity_fingerprint,
             expected.window_identity_fingerprint
           ),
         :ok <- match_field(:coverage, descriptor.coverage, expected.coverage),
         :ok <-
           match_field(
             :manifest_schema_version,
             descriptor.manifest_schema_version,
             expected.manifest_schema_version
           ) do
      match_field(
        :runner_contract_version,
        descriptor.runner_contract_version,
        expected.runner_contract_version
      )
    end
  end

  defp persisted_table?(%{type: :sql, relation: %RelationRef{}, materialization: :table}),
    do: true

  defp persisted_table?(%{
         type: :sql,
         relation: %RelationRef{},
         materialization: {:incremental, _opts}
       }),
       do: true

  defp persisted_table?(_asset), do: false

  defp connection_definition!(nil, _opts),
    do: raise(ArgumentError, "persisted SQL targets require a named connection")

  defp connection_definition!(name, opts) do
    case opts |> Keyword.get(:connection_definitions, %{}) |> Map.fetch(name) do
      {:ok, %{adapter: adapter} = definition} when is_atom(adapter) ->
        definition

      _other ->
        raise ArgumentError,
              "missing connection definition for persisted SQL target #{inspect(name)}"
    end
  end

  defp target_id({module, name}), do: Atom.to_string(module) <> ":" <> Atom.to_string(name)

  defp relation_identity(%RelationRef{} = relation) do
    %{
      connection: identifier(relation.connection),
      catalog: relation.catalog,
      schema: relation.schema,
      name: relation.name
    }
  end

  defp materialization_identity(:table), do: %{kind: "table"}

  defp materialization_identity({:incremental, opts}) do
    %{
      kind: "incremental",
      strategy: identifier(Keyword.fetch!(opts, :strategy)),
      unique_key: identifiers(Keyword.get(opts, :unique_key)),
      window_column: identifier(Keyword.get(opts, :window_column))
    }
  end

  defp write_semantics(:table), do: %{mode: "replace"}

  defp write_semantics({:incremental, opts}) do
    %{
      mode: "incremental",
      strategy: identifier(Keyword.fetch!(opts, :strategy)),
      unique_key: identifiers(Keyword.get(opts, :unique_key)),
      window_column: identifier(Keyword.get(opts, :window_column))
    }
  end

  defp window_identity(%WindowSpec{kind: kind, timezone: timezone}),
    do: %{kind: identifier(kind), timezone: timezone}

  defp window_identity(_window), do: nil

  defp coverage_identity(nil), do: nil

  defp coverage_identity(%EffectiveCoverage{} = coverage) do
    %{
      declared_from: period_identity(coverage.declared_from),
      effective_from: period_identity(coverage.effective_from),
      through: coverage_through(coverage.through),
      availability_delay_seconds: coverage.availability_delay_seconds,
      kind: identifier(coverage.kind),
      timezone: coverage.timezone,
      scope_source: identifier(coverage.scope_source)
    }
  end

  defp period_identity(%TimePeriod{} = period) do
    %{
      kind: identifier(period.kind),
      start_at: DateTime.to_iso8601(period.start_at),
      end_at: DateTime.to_iso8601(period.end_at),
      timezone: period.timezone
    }
  end

  defp coverage_through(value) when value in [:latest_closed, :current], do: identifier(value)
  defp coverage_through(%TimePeriod{} = period), do: period_identity(period)

  defp contract(%{contract: contract}), do: contract
  defp contract(_assurance), do: nil

  defp contract_columns(%{columns: columns}) when is_list(columns) do
    Enum.map(columns, fn column ->
      %{
        name: Map.get(column, :name),
        type: Map.get(column, :type),
        nullable: Map.get(column, :nullable?)
      }
    end)
  end

  defp contract_columns(_contract), do: nil

  defp contract_grain_and_keys(%{} = contract) do
    grain = Map.get(contract, :grain)
    unique_keys = Map.get(contract, :unique_keys, [])

    if is_nil(grain) and unique_keys == [] do
      nil
    else
      %{
        grain: if(is_map(grain), do: Map.get(grain, :by, []), else: nil),
        unique_keys:
          Enum.map(unique_keys, fn
            %{columns: columns} -> columns
            columns when is_list(columns) -> columns
          end)
      }
    end
  end

  defp contract_grain_and_keys(_contract), do: nil

  defp canonical_relation(value) do
    value
    |> canonical_map([:connection, :catalog, :schema, :name])
    |> Map.update!(:connection, &identifier/1)
  end

  defp canonical_connection_identity(value) do
    value
    |> canonical_map([:name, :definition_module])
    |> Map.update!(:name, &identifier/1)
  end

  defp canonical_materialization(value) do
    value = canonical_map(value, [:kind, :strategy, :unique_key, :window_column])

    case identifier(value.kind) do
      "table" ->
        %{kind: "table"}

      "incremental" ->
        %{
          kind: "incremental",
          strategy: identifier(value.strategy),
          unique_key: identifiers(value.unique_key),
          window_column: identifier(value.window_column)
        }

      _other ->
        value
    end
  end

  defp canonical_write_semantics(value) do
    value = canonical_map(value, [:mode, :strategy, :unique_key, :window_column])

    case identifier(value.mode) do
      "replace" ->
        %{mode: "replace"}

      "incremental" ->
        %{
          mode: "incremental",
          strategy: identifier(value.strategy),
          unique_key: identifiers(value.unique_key),
          window_column: identifier(value.window_column)
        }

      _other ->
        value
    end
  end

  defp canonical_window_identity(nil), do: nil

  defp canonical_window_identity(value) do
    value = canonical_map(value, [:kind, :timezone])
    %{kind: identifier(value.kind), timezone: value.timezone}
  end

  defp canonical_coverage(nil), do: nil

  defp canonical_coverage(value) do
    value =
      canonical_map(value, [
        :declared_from,
        :effective_from,
        :through,
        :availability_delay_seconds,
        :kind,
        :timezone,
        :scope_source
      ])

    %{
      declared_from: canonical_period(value.declared_from),
      effective_from: canonical_period(value.effective_from),
      through: canonical_through(value.through),
      availability_delay_seconds: value.availability_delay_seconds,
      kind: identifier(value.kind),
      timezone: value.timezone,
      scope_source: identifier(value.scope_source)
    }
  end

  defp canonical_through(value) when value in [:latest_closed, :current], do: identifier(value)
  defp canonical_through(value) when value in ["latest_closed", "current"], do: value
  defp canonical_through(value), do: canonical_period(value)

  defp canonical_period(value) do
    value = canonical_map(value, [:kind, :start_at, :end_at, :timezone])

    %{
      kind: identifier(value.kind),
      start_at: value.start_at,
      end_at: value.end_at,
      timezone: value.timezone
    }
  end

  defp canonical_map(value, fields) when is_map(value) do
    Map.new(fields, &{&1, field(value, &1)})
  end

  defp canonical_map(_value, fields), do: Map.new(fields, &{&1, nil})

  defp valid_relation?(value),
    do:
      value == canonical_relation(value) and nonempty_string?(value.connection) and
        nonempty_string?(value.name)

  defp valid_connection_identity?(value),
    do:
      value == canonical_connection_identity(value) and nonempty_string?(value.name) and
        optional_string?(value.definition_module)

  defp valid_materialization?(%{kind: "table"}), do: true

  defp valid_materialization?(%{
         kind: "incremental",
         strategy: strategy,
         unique_key: unique_key,
         window_column: window_column
       }),
       do:
         strategy in @strategies and optional_string_list?(unique_key) and
           optional_string?(window_column)

  defp valid_materialization?(_value), do: false

  defp valid_write_semantics?(%{mode: "replace"}), do: true

  defp valid_write_semantics?(%{
         mode: "incremental",
         strategy: strategy,
         unique_key: unique_key,
         window_column: window_column
       }),
       do:
         strategy in @strategies and optional_string_list?(unique_key) and
           optional_string?(window_column)

  defp valid_write_semantics?(_value), do: false

  defp valid_window_identity?(nil), do: true

  defp valid_window_identity?(%{kind: kind, timezone: timezone}),
    do: kind in @period_kinds and Favn.Window.Validate.timezone(timezone) == :ok

  defp valid_window_identity?(_value), do: false

  defp valid_coverage?(nil), do: true

  defp valid_coverage?(%{} = coverage) do
    coverage == canonical_coverage(coverage) and coverage.kind in @period_kinds and
      coverage.scope_source in ["declared", "environment_floor"] and
      is_integer(coverage.availability_delay_seconds) and
      coverage.availability_delay_seconds >= 0 and
      Favn.Window.Validate.timezone(coverage.timezone) == :ok and
      valid_period?(coverage.declared_from, coverage.kind, coverage.timezone) and
      valid_period?(coverage.effective_from, coverage.kind, coverage.timezone) and
      valid_through?(coverage.through, coverage.kind, coverage.timezone)
  end

  defp valid_coverage?(_value), do: false

  defp valid_through?(value, _kind, _timezone) when value in ["latest_closed", "current"],
    do: true

  defp valid_through?(value, kind, timezone), do: valid_period?(value, kind, timezone)

  defp valid_period?(
         %{kind: kind, start_at: start_at, end_at: end_at, timezone: timezone},
         kind,
         timezone
       ) do
    match?({:ok, %DateTime{}, _offset}, DateTime.from_iso8601(start_at)) and
      match?({:ok, %DateTime{}, _offset}, DateTime.from_iso8601(end_at))
  end

  defp valid_period?(_value, _kind, _timezone), do: false

  defp hash_payload(descriptor),
    do: descriptor |> Map.from_struct() |> Map.delete(:descriptor_hash)

  defp fingerprint(nil), do: nil
  defp fingerprint(value), do: hash(value)

  defp hash(value) do
    value
    |> Serializer.encode_manifest!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp match_field(_field, value, value), do: :ok

  defp match_field(field, actual, expected),
    do: {:error, {:target_descriptor_asset_mismatch, field, actual, expected}}

  defp module_name(nil), do: nil
  defp module_name(module) when is_atom(module), do: Atom.to_string(module)

  defp identifier(nil), do: nil
  defp identifier(value) when is_atom(value), do: Atom.to_string(value)
  defp identifier(value) when is_binary(value), do: value
  defp identifier(value), do: value

  defp identifiers(nil), do: nil
  defp identifiers(values) when is_list(values), do: Enum.map(values, &identifier/1)
  defp identifiers(value), do: value

  defp nonempty_string?(value), do: is_binary(value) and value != ""
  defp optional_string?(nil), do: true
  defp optional_string?(value), do: nonempty_string?(value)
  defp optional_string_list?(nil), do: true

  defp optional_string_list?(values) when is_list(values),
    do: Enum.all?(values, &nonempty_string?/1)

  defp optional_string_list?(_value), do: false
  defp optional_hash?(nil), do: true
  defp optional_hash?(value), do: canonical_hash?(value)
  defp canonical_hash?(value) when is_binary(value), do: Regex.match?(@hash_pattern, value)
  defp canonical_hash?(_value), do: false
  defp positive_integer?(value), do: is_integer(value) and value > 0

  @persisted_fields [
    :schema_version,
    :target_id,
    :relation,
    :adapter,
    :connection_identity,
    :materialization,
    :write_semantics,
    :execution_package_hash,
    :contract_fingerprint,
    :grain_fingerprint,
    :window_identity,
    :window_identity_fingerprint,
    :coverage,
    :manifest_schema_version,
    :runner_contract_version,
    :descriptor_hash
  ]
  @persisted_field_names Enum.map(@persisted_fields, &Atom.to_string/1)

  defp reject_unknown_fields(value) do
    unknown =
      value
      |> Map.keys()
      |> Enum.reject(&(&1 in @persisted_fields or &1 in @persisted_field_names))
      |> Enum.sort_by(&inspect/1)

    if unknown == [],
      do: :ok,
      else: {:error, {:unknown_target_descriptor_fields, unknown}}
  end

  defp field(value, key, default \\ nil),
    do: Map.get(value, key, Map.get(value, Atom.to_string(key), default))
end
