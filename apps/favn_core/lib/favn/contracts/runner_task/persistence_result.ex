defmodule Favn.Contracts.RunnerTask.PersistenceResult do
  @moduledoc false

  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationMarkerInitializationResult
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask.OpenData
  alias Favn.SQL.Column
  alias Favn.SQL.Relation

  @sql_asset_meta_keys [
    :check_results,
    :command,
    :connection,
    :contract_validation,
    :group_replacement,
    :manifest_content_hash,
    :manifest_version_id,
    :materialized,
    :message,
    :metrics,
    :observed,
    :quality_status,
    :reason,
    :relation,
    :rows_affected,
    :runtime_inputs,
    :transaction_outcome,
    :write_outcome
  ]

  @sql_asset_required_keys [
    :check_results,
    :command,
    :connection,
    :group_replacement,
    :materialized,
    :quality_status,
    :reason,
    :rows_affected,
    :write_outcome
  ]

  @sql_failure_required_keys [
    :check_results,
    :connection,
    :quality_status,
    :transaction_outcome,
    :write_outcome
  ]

  @generation_results [
    GenerationActivationResult,
    GenerationDiscardResult,
    GenerationMarkerInitializationResult,
    GenerationReconciliationResult
  ]

  @spec normalize(atom(), term()) :: {:ok, term()} | {:error, term()}
  def normalize(:asset_attempt, %RunnerResult{} = result) do
    with {:ok, error} <- normalize_error(result.error, :runner_error_details),
         {:ok, asset_results} <- normalize_asset_results(result.asset_results) do
      {:ok, %{result | error: error, asset_results: asset_results}}
    end
  end

  def normalize(:relation_inspection, %RelationInspectionResult{} = result),
    do: normalize_inspection(result)

  def normalize(_kind, %module{error: error} = result) when module in @generation_results do
    with {:ok, error} <- normalize_error(error, :runner_error_details) do
      {:ok, %{result | error: error}}
    end
  end

  def normalize(_kind, result), do: {:ok, result}

  defp normalize_asset_results(results) when is_list(results) do
    map_values(results, &normalize_asset_result/1)
  end

  defp normalize_asset_results(_results), do: {:error, :invalid_runner_asset_results}

  defp normalize_asset_result(%RunnerAssetResult{} = result) do
    with {:ok, meta} <- normalize_asset_meta(result.meta),
         {:ok, error} <- normalize_error(result.error, :runner_error_details),
         {:ok, attempts} <- normalize_attempts(result.attempts) do
      {:ok, %{result | meta: meta, error: error, attempts: attempts}}
    end
  end

  defp normalize_asset_result(_result), do: {:error, :invalid_runner_asset_result}

  defp normalize_attempts(attempts) when is_list(attempts),
    do: map_values(attempts, &normalize_attempt/1)

  defp normalize_attempts(_attempts), do: {:error, :invalid_runner_asset_attempts}

  defp normalize_attempt(%{} = attempt) do
    with {:ok, meta} <- normalize_asset_meta(field(attempt, :meta, %{})),
         {:ok, error} <-
           normalize_error(field(attempt, :error), :runner_error_details) do
      {:ok,
       attempt
       |> put_field(:meta, meta)
       |> put_field(:error, error)}
    end
  end

  defp normalize_attempt(_attempt), do: {:error, :invalid_runner_asset_attempt}

  defp normalize_asset_meta(meta) when is_map(meta) do
    cond do
      source_asset_meta?(meta) -> {:ok, meta}
      sql_asset_meta?(meta) -> normalize_sql_asset_meta(meta)
      true -> normalize_open(meta, :asset_metadata)
    end
  end

  defp normalize_asset_meta(_meta), do: {:error, :invalid_runner_asset_metadata}

  defp normalize_sql_asset_meta(meta) do
    meta
    |> Enum.reduce_while({:ok, %{}, MapSet.new()}, fn {key, value}, {:ok, acc, seen} ->
      with {:ok, normalized_key, normalized_value} <- normalize_sql_asset_meta_entry(key, value),
           canonical_key <- canonical_key(normalized_key),
           false <- MapSet.member?(seen, canonical_key) do
        {:cont,
         {:ok, Map.put(acc, normalized_key, normalized_value), MapSet.put(seen, canonical_key)}}
      else
        true -> {:halt, open_error(:asset_metadata, :duplicate_key)}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> drop_seen_keys()
  end

  defp normalize_sql_asset_meta_entry(key, value) when key in @sql_asset_meta_keys do
    with {:ok, normalized} <- normalize_sql_asset_meta_value(key, value) do
      {:ok, key, normalized}
    end
  end

  defp normalize_sql_asset_meta_entry(key, value) do
    with {:ok, normalized} <- OpenData.normalize(%{key => value}) do
      [{normalized_key, normalized_value}] = Map.to_list(normalized)
      {:ok, normalized_key, normalized_value}
    else
      {:error, reason} -> open_error(:asset_metadata, reason)
    end
  end

  defp normalize_sql_asset_meta_value(:runtime_inputs, value), do: normalize_runtime_inputs(value)

  defp normalize_sql_asset_meta_value(:metrics, value) do
    case OpenData.normalize(value) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, reason} -> open_error(:sql_metrics, reason)
    end
  end

  defp normalize_sql_asset_meta_value(_key, value), do: {:ok, value}

  defp source_asset_meta?(%{observed: true, relation: %Favn.RelationRef{}} = meta),
    do: map_size(meta) == 2

  defp source_asset_meta?(_meta), do: false

  defp sql_asset_meta?(meta) do
    complete_sql_meta?(meta) or complete_sql_failure_meta?(meta) or typed_sql_evidence?(meta)
  end

  defp complete_sql_meta?(meta) do
    Enum.all?(@sql_asset_required_keys, &Map.has_key?(meta, &1)) and
      match?(%Favn.RelationRef{}, Map.get(meta, :materialized)) and
      is_atom(Map.get(meta, :connection)) and
      is_list(Map.get(meta, :check_results)) and
      Map.get(meta, :quality_status) in [:passed, :warning] and
      Map.get(meta, :write_outcome) in [:written, :no_op]
  end

  defp typed_sql_evidence?(meta) do
    match?(%Favn.SQL.ContractValidation{}, Map.get(meta, :contract_validation)) or
      match?(%Favn.SQL.GroupReplacementResult{}, Map.get(meta, :group_replacement)) or
      match?(%Favn.RelationRef{}, Map.get(meta, :materialized)) or
      typed_check_results?(Map.get(meta, :check_results))
  end

  defp complete_sql_failure_meta?(meta) do
    Enum.all?(@sql_failure_required_keys, &Map.has_key?(meta, &1)) and
      is_atom(Map.get(meta, :connection)) and
      is_list(Map.get(meta, :check_results)) and
      Map.get(meta, :quality_status) == :failed and
      Map.get(meta, :transaction_outcome) in [:rolled_back, :not_started, :unknown] and
      Map.get(meta, :write_outcome) in [:rolled_back, :not_started, :unknown]
  end

  defp typed_check_results?(values) when is_list(values),
    do: Enum.any?(values, &match?(%Favn.SQL.CheckResult{}, &1))

  defp typed_check_results?(_values), do: false

  defp canonical_key(key) when is_atom(key), do: Atom.to_string(key)
  defp canonical_key(key), do: key

  defp drop_seen_keys({:ok, normalized, _seen}), do: {:ok, normalized}
  defp drop_seen_keys({:error, _reason} = error), do: error

  defp normalize_runtime_inputs(%{} = value) do
    case Map.fetch(value, :input_metadata) do
      {:ok, metadata} ->
        case OpenData.normalize(metadata) do
          {:ok, normalized} -> {:ok, Map.put(value, :input_metadata, normalized)}
          {:error, reason} -> open_error(:runtime_input_metadata, reason)
        end

      :error ->
        {:ok, value}
    end
  end

  defp normalize_runtime_inputs(value), do: {:ok, value}

  defp normalize_error(nil, _path), do: {:ok, nil}

  defp normalize_error(%RunnerError{} = error, path) do
    case OpenData.normalize(error.details, tuples: :lists) do
      {:ok, details} -> {:ok, %{error | details: details}}
      {:error, reason} -> open_error(path, reason)
    end
  end

  defp normalize_error(error, _path), do: {:ok, error}

  defp normalize_inspection(%RelationInspectionResult{} = result) do
    with {:ok, relation} <- normalize_relation(result.relation),
         {:ok, columns} <- normalize_columns(result.columns),
         {:ok, sample} <- normalize_sample(result.sample),
         {:ok, table_metadata} <-
           normalize_open(result.table_metadata, :inspection_table_metadata),
         {:ok, error} <- normalize_open(result.error, :inspection_error) do
      {:ok,
       %{
         result
         | relation: relation,
           columns: columns,
           sample: sample,
           table_metadata: table_metadata,
           error: error
       }}
    end
  end

  defp normalize_relation(%Relation{} = relation) do
    with {:ok, metadata} <- normalize_open(relation.metadata, :inspection_relation_metadata) do
      {:ok, %{relation | metadata: metadata}}
    end
  end

  defp normalize_relation(relation), do: {:ok, relation}

  defp normalize_columns(columns) when is_list(columns),
    do: map_values(columns, &normalize_column/1)

  defp normalize_columns(columns), do: {:ok, columns}

  defp normalize_column(%Column{} = column) do
    with {:ok, metadata} <- normalize_column_metadata(column.metadata) do
      {:ok, %{column | metadata: metadata}}
    end
  end

  defp normalize_column(column), do: {:ok, column}

  defp normalize_column_metadata(metadata) when is_map(metadata) do
    atom_control? = Map.has_key?(metadata, :contract_nullability)
    string_control? = Map.has_key?(metadata, "contract_nullability")
    atom_control = Map.get(metadata, :contract_nullability)
    string_control = Map.get(metadata, "contract_nullability")

    cond do
      atom_control? and string_control? ->
        open_error(:inspection_column_metadata, :duplicate_key)

      true ->
        control = atom_control || string_control

        with true <-
               is_nil(control) or control in [:reliable, :unreliable, "reliable", "unreliable"],
             {:ok, normalized} <-
               metadata
               |> Map.drop([:contract_nullability, "contract_nullability"])
               |> normalize_open(:inspection_column_metadata) do
          normalized =
            if is_nil(control) do
              normalized
            else
              Map.put(normalized, :contract_nullability, normalize_nullability(control))
            end

          {:ok, normalized}
        else
          false -> {:error, :invalid_contract_nullability}
          {:error, _reason} = error -> error
        end
    end
  end

  defp normalize_column_metadata(metadata), do: {:ok, metadata}

  defp normalize_nullability("reliable"), do: :reliable
  defp normalize_nullability("unreliable"), do: :unreliable
  defp normalize_nullability(value), do: value

  defp normalize_sample(nil), do: {:ok, nil}

  defp normalize_sample(%{} = sample) do
    allowed = [:limit, :columns, :rows]

    if Enum.all?(Map.keys(sample), &(&1 in allowed)) do
      with {:ok, columns} <- normalize_open(Map.get(sample, :columns, []), :inspection_sample),
           {:ok, rows} <- normalize_open(Map.get(sample, :rows, []), :inspection_sample) do
        {:ok, %{limit: Map.get(sample, :limit), columns: columns, rows: rows}}
      end
    else
      normalize_open(sample, :inspection_sample)
    end
  end

  defp normalize_sample(_sample), do: {:error, :invalid_inspection_sample}

  defp normalize_open(nil, _path), do: {:ok, nil}

  defp normalize_open(value, path) do
    case OpenData.normalize(value) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, reason} -> open_error(path, reason)
    end
  end

  defp open_error(path, reason), do: {:error, {:invalid_runner_task_open_data, path, reason}}

  defp map_values(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_values()
  end

  defp reverse_values({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_values({:error, _reason} = error), do: error

  defp field(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp put_field(map, key, value) do
    cond do
      Map.has_key?(map, key) -> Map.put(map, key, value)
      Map.has_key?(map, Atom.to_string(key)) -> Map.put(map, Atom.to_string(key), value)
      true -> Map.put(map, key, value)
    end
  end
end
