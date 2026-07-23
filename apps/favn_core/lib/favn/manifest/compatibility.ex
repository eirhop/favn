defmodule Favn.Manifest.Compatibility do
  @moduledoc """
  Manifest schema and runner contract compatibility checks.
  """

  alias Favn.Manifest.ContractVersions
  alias Favn.Manifest.Asset
  alias Favn.Manifest.Pipeline
  alias Favn.Manifest.Schedule
  alias Favn.Manifest.TargetDescriptor
  alias Favn.RunnerRelease
  alias Favn.Window.Policy
  alias Favn.Window.Spec

  @current_schema_version ContractVersions.manifest_schema_version()
  @current_runner_contract_version ContractVersions.runner_contract_version()

  @type error ::
          {:invalid_manifest_input, term()}
          | {:missing_manifest_field,
             :schema_version | :runner_contract_version | :required_runner_release_id}
          | {:invalid_required_runner_release_id, term()}
          | {:invalid_execution_package_hash, Favn.Ref.t(), term()}
          | {:duplicate_execution_package_hash, String.t(), [Favn.Ref.t()]}
          | {:missing_execution_package_hash, Favn.Ref.t()}
          | {:unexpected_execution_package_hash, Favn.Ref.t()}
          | {:unsupported_schema_version, term(), pos_integer()}
          | {:unsupported_runner_contract_version, term(), pos_integer()}

  @spec current_schema_version() :: pos_integer()
  def current_schema_version, do: @current_schema_version

  @spec current_runner_contract_version() :: pos_integer()
  def current_runner_contract_version, do: @current_runner_contract_version

  @spec validate_manifest(term()) :: :ok | {:error, error()}
  def validate_manifest(manifest) when is_map(manifest) or is_struct(manifest) do
    with {:ok, schema_version} <- read_required_field(manifest, :schema_version),
         {:ok, runner_contract_version} <-
           read_required_field(manifest, :runner_contract_version),
         {:ok, required_runner_release_id} <-
           read_required_field(manifest, :required_runner_release_id),
         :ok <- validate_schema_version(schema_version),
         :ok <- validate_runner_contract_version(runner_contract_version),
         :ok <- validate_required_runner_release_id(required_runner_release_id),
         :ok <- validate_execution_package_refs(manifest) do
      validate_resolved_contracts(
        manifest,
        schema_version,
        runner_contract_version,
        required_runner_release_id
      )
    end
  end

  def validate_manifest(other), do: {:error, {:invalid_manifest_input, other}}

  @spec validate_schema_version(term()) :: :ok | {:error, error()}
  def validate_schema_version(@current_schema_version), do: :ok

  def validate_schema_version(other),
    do: {:error, {:unsupported_schema_version, other, @current_schema_version}}

  @spec validate_runner_contract_version(term()) :: :ok | {:error, error()}
  def validate_runner_contract_version(@current_runner_contract_version), do: :ok

  def validate_runner_contract_version(other),
    do: {:error, {:unsupported_runner_contract_version, other, @current_runner_contract_version}}

  @doc "Validates the exact runner release identity required by a current manifest."
  @spec validate_required_runner_release_id(term()) ::
          :ok | {:error, {:invalid_required_runner_release_id, term()}}
  def validate_required_runner_release_id(value) do
    case RunnerRelease.validate_id(value) do
      :ok -> :ok
      {:error, _reason} -> {:error, {:invalid_required_runner_release_id, value}}
    end
  end

  defp validate_execution_package_refs(manifest) do
    assets = Map.get(manifest, :assets, Map.get(manifest, "assets", []))

    with :ok <- validate_asset_package_refs(assets) do
      validate_unique_package_hashes(assets)
    end
  end

  defp validate_resolved_contracts(
         manifest,
         schema_version,
         runner_contract_version,
         runner_release_id
       ) do
    with :ok <-
           validate_assets(
             optional_field(manifest, :assets, []),
             schema_version,
             runner_contract_version,
             runner_release_id
           ),
         :ok <- validate_pipelines(optional_field(manifest, :pipelines, [])) do
      validate_schedules(optional_field(manifest, :schedules, []))
    end
  end

  defp validate_assets(assets, schema_version, runner_contract_version, runner_release_id)
       when is_list(assets) do
    Enum.reduce_while(assets, :ok, fn
      %Asset{} = asset, :ok ->
        case validate_asset(
               asset,
               schema_version,
               runner_contract_version,
               runner_release_id
             ) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:invalid_manifest_asset, asset.ref, reason}}}
        end

      _other, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_assets(_assets, _schema, _runner, _release),
    do: {:error, :invalid_manifest_assets}

  defp validate_asset(asset, schema_version, runner_contract_version, runner_release_id) do
    with :ok <- validate_asset_window(asset.window),
         :ok <- validate_asset_coverage(asset.coverage, asset.window),
         :ok <- validate_asset_freshness(asset.freshness) do
      validate_asset_generation(
        asset,
        schema_version,
        runner_contract_version,
        runner_release_id
      )
    end
  end

  defp validate_asset_window(nil), do: :ok

  defp validate_asset_window(%Spec{timezone: timezone, timezone_source: source} = window) do
    with {:ok, _window} <- Spec.from_value(window),
         true <- is_binary(timezone) and not is_nil(source) do
      :ok
    else
      false -> {:error, :unresolved_asset_timezone}
      {:error, _reason} = error -> error
    end
  end

  defp validate_asset_window(value), do: {:error, {:invalid_asset_window, value}}

  defp validate_asset_coverage(nil, _window), do: :ok

  defp validate_asset_coverage(%Favn.Coverage.Effective{} = coverage, %Spec{} = window) do
    with {:ok, _coverage} <- Favn.Coverage.Effective.validate(coverage),
         true <- coverage.kind == window.kind,
         true <- coverage.timezone == window.timezone,
         true <- coverage.timezone_source == window.timezone_source do
      :ok
    else
      false -> {:error, :coverage_window_identity_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp validate_asset_coverage(%Favn.Coverage.Effective{}, nil),
    do: {:error, :coverage_requires_window}

  defp validate_asset_coverage(value, _window), do: {:error, {:invalid_asset_coverage, value}}

  defp validate_asset_freshness(nil), do: :ok

  defp validate_asset_freshness(%Favn.Freshness.Policy{mode: :calendar_period} = freshness) do
    with {:ok, _freshness} <- Favn.Freshness.Policy.validate(freshness),
         true <- is_binary(freshness.timezone) and not is_nil(freshness.timezone_source) do
      :ok
    else
      false -> {:error, :unresolved_freshness_timezone}
      {:error, _reason} = error -> error
    end
  end

  defp validate_asset_freshness(%Favn.Freshness.Policy{} = freshness) do
    case Favn.Freshness.Policy.validate(freshness) do
      {:ok, _freshness} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_asset_freshness(value), do: {:error, {:invalid_asset_freshness, value}}

  defp validate_asset_generation(
         %Asset{type: :sql, materialization: materialization} = asset,
         schema_version,
         runner_contract_version,
         _runner_release_id
       )
       when materialization == :table or
              (is_tuple(materialization) and elem(materialization, 0) == :incremental) do
    with %TargetDescriptor{} = descriptor <- asset.target_descriptor,
         nil <- asset.semantic_generation_id do
      TargetDescriptor.validate_asset(
        descriptor,
        Map.from_struct(asset),
        schema_version,
        runner_contract_version
      )
    else
      _other -> {:error, :persisted_target_descriptor_required}
    end
  end

  defp validate_asset_generation(
         %Asset{window: %Spec{}, target_descriptor: nil} = asset,
         _schema_version,
         _runner_contract_version,
         runner_release_id
       ) do
    expected =
      TargetDescriptor.semantic_generation_id(Map.from_struct(asset), runner_release_id)

    if asset.semantic_generation_id == expected do
      :ok
    else
      {:error, {:semantic_generation_id_mismatch, asset.semantic_generation_id, expected}}
    end
  end

  defp validate_asset_generation(asset, _schema, _runner, _runner_release_id) do
    if is_nil(asset.target_descriptor), do: :ok, else: {:error, :unexpected_target_descriptor}
  end

  defp validate_pipelines(pipelines) when is_list(pipelines) do
    Enum.reduce_while(pipelines, :ok, fn
      %Pipeline{} = pipeline, :ok ->
        case validate_pipeline(pipeline) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, {:invalid_manifest_pipeline, pipeline.name, reason}}}
        end

      _other, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_pipelines(_pipelines), do: {:error, :invalid_manifest_pipelines}

  defp validate_pipeline(%Pipeline{window: nil, schedule: schedule}),
    do: validate_pipeline_schedule(schedule)

  defp validate_pipeline(%Pipeline{window: %Policy{} = window, schedule: schedule}) do
    with {:ok, _window} <- Policy.validate(window),
         true <- is_binary(window.timezone) and not is_nil(window.timezone_source),
         :ok <- validate_pipeline_schedule(schedule) do
      :ok
    else
      false -> {:error, :unresolved_pipeline_timezone}
      {:error, _reason} = error -> error
    end
  end

  defp validate_pipeline(%Pipeline{window: value}),
    do: {:error, {:invalid_pipeline_window, value}}

  defp validate_pipeline_schedule(nil), do: :ok
  defp validate_pipeline_schedule({:ref, _ref}), do: :ok

  defp validate_pipeline_schedule({:inline, %Schedule{} = schedule}),
    do: validate_schedule(schedule)

  defp validate_pipeline_schedule(value), do: {:error, {:invalid_pipeline_schedule, value}}

  defp validate_schedules(schedules) when is_list(schedules) do
    Enum.reduce_while(schedules, :ok, fn
      %Schedule{} = schedule, :ok ->
        case validate_schedule(schedule) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, {:invalid_manifest_schedule, schedule.name, reason}}}
        end

      _other, :ok ->
        {:cont, :ok}
    end)
  end

  defp validate_schedules(_schedules), do: {:error, :invalid_manifest_schedules}

  defp validate_schedule(%Schedule{timezone: timezone, timezone_source: source}) do
    with :ok <- Favn.Window.Validate.timezone(timezone),
         true <- source in [:local, :application_default, :utc_fallback] do
      :ok
    else
      false -> {:error, :invalid_schedule_timezone_source}
      {:error, _reason} = error -> error
    end
  end

  defp validate_asset_package_refs(assets) do
    Enum.reduce_while(assets, :ok, fn asset, :ok ->
      type = Map.get(asset, :type, Map.get(asset, "type"))
      ref = Map.get(asset, :ref, Map.get(asset, "ref"))
      hash = Map.get(asset, :execution_package_hash, Map.get(asset, "execution_package_hash"))

      case {type, hash} do
        {:sql, value} when is_binary(value) ->
          if canonical_hash?(value) do
            {:cont, :ok}
          else
            {:halt, {:error, {:invalid_execution_package_hash, ref, value}}}
          end

        {:sql, nil} ->
          {:halt, {:error, {:missing_execution_package_hash, ref}}}

        {:sql, value} ->
          {:halt, {:error, {:invalid_execution_package_hash, ref, value}}}

        {_type, nil} ->
          {:cont, :ok}

        {_type, _value} ->
          {:halt, {:error, {:unexpected_execution_package_hash, ref}}}
      end
    end)
  end

  defp validate_unique_package_hashes(assets) do
    assets
    |> Enum.flat_map(fn asset ->
      case {
        Map.get(asset, :execution_package_hash, Map.get(asset, "execution_package_hash")),
        Map.get(asset, :ref, Map.get(asset, "ref"))
      } do
        {hash, ref} when is_binary(hash) -> [{hash, ref}]
        _other -> []
      end
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.find(fn {_hash, refs} -> length(refs) > 1 end)
    |> case do
      nil -> :ok
      {hash, refs} -> {:error, {:duplicate_execution_package_hash, hash, Enum.sort(refs)}}
    end
  end

  defp canonical_hash?(hash), do: Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)

  defp read_required_field(value, field) do
    atom_key = field
    string_key = Atom.to_string(field)

    cond do
      Map.has_key?(value, atom_key) -> {:ok, Map.get(value, atom_key)}
      Map.has_key?(value, string_key) -> {:ok, Map.get(value, string_key)}
      true -> {:error, {:missing_manifest_field, field}}
    end
  end

  defp optional_field(value, field, default) do
    Map.get(value, field, Map.get(value, Atom.to_string(field), default))
  end
end
