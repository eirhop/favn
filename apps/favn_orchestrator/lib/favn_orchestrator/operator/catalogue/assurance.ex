defmodule FavnOrchestrator.Operator.Catalogue.Assurance do
  @moduledoc """
  Projects manifest-pinned SQL assurance into the internal operator read model.

  `FavnOrchestrator.Operator.Catalogue` composes this projection into the asset
  detail DTO exposed through the public orchestrator facade.
  """

  alias Favn.SQL.Contract
  alias Favn.SQL.Contract.Param

  @doc "Projects one asset's assurance definition and latest check evidence."
  @spec detail(map(), map() | nil) :: map() | nil
  def detail(%{assurance: nil}, _latest_run), do: nil

  def detail(%{assurance: assurance, ref: asset_ref}, latest_run) do
    contract = Map.get(assurance, :contract)
    checks = List.wrap(Map.get(assurance, :checks))

    if is_nil(contract) and checks == [] do
      nil
    else
      meta = latest_asset_meta(latest_run, asset_ref)
      results = meta |> field(:check_results, []) |> List.wrap()
      results_by_name = Map.new(results, &{to_string(field(&1, :name)), &1})

      check_details =
        Enum.map(checks, &check_detail(&1, Map.get(results_by_name, Atom.to_string(&1.name))))

      checks_by_claim_id =
        check_details
        |> Enum.filter(&is_binary(&1.claim_id))
        |> Map.new(&{&1.claim_id, &1})

      %{
        contract: contract_detail(contract, checks_by_claim_id),
        checks: check_details,
        quality_status:
          normalize_enum(field(meta, :quality_status), [:passed, :warning, :failed]),
        write_outcome:
          normalize_enum(field(meta, :write_outcome), [
            :written,
            :no_op,
            :rolled_back,
            :not_started,
            :unknown
          ]),
        contract_validation: contract_validation_detail(field(meta, :contract_validation)),
        latest_run_id: latest_run && Map.get(latest_run, :id)
      }
    end
  end

  defp contract_detail(nil, _checks_by_claim_id), do: nil

  defp contract_detail(contract, checks_by_claim_id) do
    row_count_specs =
      contract
      |> Contract.generated_check_specs()
      |> Enum.take(length(contract.row_counts))

    %{
      grain:
        case contract.grain do
          nil -> nil
          grain -> %{by: grain.by, description: grain.description}
        end,
      columns: Enum.map(contract.columns, &contract_column_detail/1),
      unique_keys: Enum.map(contract.unique_keys, & &1.columns),
      row_counts:
        contract.row_counts
        |> Enum.zip(row_count_specs)
        |> Enum.map(fn {row_count, spec} ->
          row_count_detail(row_count, spec, Map.get(checks_by_claim_id, spec.claim_id))
        end)
    }
  end

  defp row_count_detail(row_count, spec, check) do
    %{
      claim_id: spec.claim_id,
      equals: row_count_equals_detail(row_count.equals),
      min: row_count.min,
      max: row_count.max,
      when: row_count.when,
      on_violation: row_count.on_violation,
      latest_result: check && check.latest_result
    }
  end

  defp row_count_equals_detail(nil), do: nil
  defp row_count_equals_detail(%Param{name: name}), do: %{source: :param, name: name}
  defp row_count_equals_detail(value), do: %{source: :literal, value: value}

  defp contract_column_detail(column) do
    %{
      name: column.name,
      type: column.type,
      nullable?: column.nullable?,
      description: column.description,
      tags: column.tags,
      renamed_from: column.renamed_from,
      via: column.via,
      sources: Enum.map(column.sources, &lineage_detail/1)
    }
  end

  defp lineage_detail(%{kind: :asset} = lineage) do
    %{kind: :asset, asset_ref: lineage.asset_ref, column: lineage.column}
  end

  defp lineage_detail(%{kind: :external} = lineage) do
    %{kind: :external, dataset: lineage.dataset, column: lineage.column}
  end

  defp check_detail(check, latest_result) do
    %{
      name: field(check, :name),
      origin: field(check, :origin),
      claim_id: field(check, :claim_id),
      phase: field(check, :at),
      when: field(check, :when),
      on_violation: field(check, :on_violation),
      message: field(check, :message),
      latest_result: check_result_detail(latest_result)
    }
  end

  defp check_result_detail(nil), do: nil

  defp check_result_detail(result) do
    %{
      outcome:
        normalize_enum(field(result, :outcome), [
          :passed,
          :warned,
          :failed,
          :materialization_skipped,
          :condition_skipped,
          :not_run,
          :errored
        ]),
      metrics: field(result, :metrics, %{}),
      duration_ms: field(result, :duration_ms),
      reason: field(result, :reason),
      message: field(result, :message)
    }
  end

  defp contract_validation_detail(nil), do: nil

  defp contract_validation_detail(validation) do
    %{
      status: normalize_enum(field(validation, :status), [:passed, :failed]),
      expected_columns: field(validation, :expected_columns, []),
      observed_columns: field(validation, :observed_columns, []),
      differences: field(validation, :differences, []),
      observed_column_count: field(validation, :observed_column_count),
      observed_truncated?: field(validation, :observed_truncated?, false)
    }
  end

  defp latest_asset_meta(nil, _asset_ref), do: %{}

  defp latest_asset_meta(latest_run, asset_ref) do
    result =
      case Map.get(latest_run, :asset_results) do
        results when is_map(results) ->
          Map.get(results, asset_ref)

        _other ->
          latest_run
          |> field(:result, %{})
          |> field(:asset_results, [])
          |> find_asset_result(asset_ref)
      end

    field(result, :meta, %{})
  end

  defp find_asset_result(results, asset_ref) when is_map(results), do: Map.get(results, asset_ref)

  defp find_asset_result(results, asset_ref) when is_list(results),
    do: Enum.find(results, &(field(&1, :ref) == asset_ref))

  defp find_asset_result(_results, _asset_ref), do: nil

  defp field(value, key, default \\ nil)
  defp field(nil, _key, default), do: default

  defp field(value, key, default) when is_map(value) do
    Map.get(value, key, Map.get(value, Atom.to_string(key), default))
  end

  defp field(_value, _key, default), do: default

  defp normalize_enum(value, allowed) when is_atom(value),
    do: if(value in allowed, do: value, else: value)

  defp normalize_enum(value, allowed) when is_binary(value) do
    Enum.find(allowed, value, &(Atom.to_string(&1) == value))
  end

  defp normalize_enum(value, _allowed), do: value
end
