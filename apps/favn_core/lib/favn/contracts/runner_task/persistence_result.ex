defmodule Favn.Contracts.RunnerTask.PersistenceResult do
  @moduledoc false

  alias Favn.Contracts.GenerationActivationResult
  alias Favn.Contracts.GenerationDiscardResult
  alias Favn.Contracts.GenerationReconciliationResult
  alias Favn.Contracts.RelationInspectionResult
  alias Favn.Contracts.RunnerAssetEvidence
  alias Favn.Contracts.RunnerAssetResult
  alias Favn.Contracts.RunnerError
  alias Favn.Contracts.RunnerResult
  alias Favn.Contracts.RunnerTask.OpenData
  alias Favn.SQL.Column
  alias Favn.SQL.Relation

  @generation_results [
    GenerationActivationResult,
    GenerationDiscardResult,
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
         {:ok, evidence} <- normalize_evidence(result.evidence),
         {:ok, error} <- normalize_error(result.error, :runner_error_details),
         {:ok, attempts} <- normalize_attempts(result.attempts) do
      {:ok, %{result | meta: meta, evidence: evidence, error: error, attempts: attempts}}
    end
  end

  defp normalize_asset_result(_result), do: {:error, :invalid_runner_asset_result}

  defp normalize_attempts(attempts) when is_list(attempts),
    do: map_values(attempts, &normalize_attempt/1)

  defp normalize_attempts(_attempts), do: {:error, :invalid_runner_asset_attempts}

  defp normalize_attempt(%{} = attempt) do
    with {:ok, meta} <- normalize_asset_meta(field(attempt, :meta, %{})),
         {:ok, evidence} <- normalize_evidence(field(attempt, :evidence)),
         {:ok, error} <-
           normalize_error(field(attempt, :error), :runner_error_details) do
      {:ok,
       attempt
       |> put_field(:meta, meta)
       |> put_field(:evidence, evidence)
       |> put_field(:error, error)}
    end
  end

  defp normalize_attempt(_attempt), do: {:error, :invalid_runner_asset_attempt}

  defp normalize_asset_meta(meta) when is_map(meta), do: normalize_open(meta, :asset_metadata)
  defp normalize_asset_meta(_meta), do: {:error, :invalid_runner_asset_metadata}

  defp normalize_evidence(nil), do: {:ok, nil}

  defp normalize_evidence(%RunnerAssetEvidence{kind: kind} = evidence)
       when kind in [:sql, :source] do
    with {:ok, metrics} <- normalize_open(evidence.metrics, :sql_metrics),
         {:ok, runtime_inputs} <- normalize_runtime_inputs(evidence.runtime_inputs) do
      {:ok, %{evidence | metrics: metrics, runtime_inputs: runtime_inputs}}
    end
  end

  defp normalize_evidence(_evidence), do: {:error, :invalid_runner_asset_evidence}

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
