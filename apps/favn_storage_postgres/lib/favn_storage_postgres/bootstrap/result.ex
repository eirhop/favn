defmodule FavnStoragePostgres.Bootstrap.Result do
  @moduledoc false

  alias Favn.RunnerRelease
  alias FavnStoragePostgres.StorageV2.Migrations

  @contract_version 1

  @type operation :: :status | :bootstrap | :upgrade
  @type state ::
          :ready
          | :invalid_configuration
          | :server_unreachable
          | :authentication_unavailable
          | :authentication_rejected
          | :database_missing
          | :identity_mapping_missing
          | :identity_mapping_conflict
          | :unsafe_authority
          | :role_hardening_required
          | :schema_upgrade_required
          | :runtime_grants_missing
          | :workspace_missing
          | :workspace_conflict
          | :operation_in_progress
          | :bootstrap_required
          | :unknown_outcome
          | :operation_failed

  @spec ready(operation(), [atom()]) :: {:ok, map()}
  def ready(operation, completed_stages) do
    {:ok,
     base(operation, :ready, :ready, true, completed_stages, [])
     |> Map.put(:runtime_verified, true)}
  end

  @spec status(operation(), state(), atom(), [atom()], map()) :: {:ok, map()}
  def status(operation, state, stage, completed_stages \\ [], details \\ %{}) do
    finding = finding(state, stage, details)

    {:ok,
     base(operation, :changes_required, state, true, completed_stages, [finding])
     |> Map.put(:runtime_verified, false)}
  end

  @spec status_findings(operation(), state(), [map()]) :: {:ok, map()}
  def status_findings(operation, state, findings) when is_list(findings) and findings != [] do
    bounded_findings =
      findings
      |> Enum.take(32)
      |> Enum.map(fn finding ->
        %{
          code: Map.fetch!(finding, :code),
          stage: Map.fetch!(finding, :stage),
          details: bounded_details(Map.get(finding, :details, %{}))
        }
      end)

    {:ok,
     base(operation, :changes_required, state, true, [], bounded_findings)
     |> Map.put(:runtime_verified, false)}
  end

  @spec error(operation(), state(), atom(), atom(), [atom()], map()) :: {:error, map()}
  def error(operation, state, code, stage, completed_stages \\ [], details \\ %{}) do
    unknown? = state == :unknown_outcome

    {:error,
     base(
       operation,
       if(unknown?, do: :unknown, else: :failed),
       state,
       not unknown?,
       completed_stages,
       [finding(code, stage, details)]
     )
     |> Map.put(:code, code)
     |> Map.put(:runtime_verified, false)}
  end

  @doc false
  @spec error_findings(operation(), state(), atom(), atom(), [map()], map()) :: {:error, map()}
  def error_findings(operation, state, code, stage, findings, details \\ %{})
      when is_list(findings) and is_map(details) do
    {:error, base_result} = error(operation, state, code, stage, [], details)

    bounded_findings =
      findings
      |> Kernel.++([finding(code, stage, details)])
      |> Enum.uniq_by(&{&1.code, &1.stage, &1.details})
      |> Enum.take(32)
      |> Enum.map(fn item ->
        %{
          code: Map.fetch!(item, :code),
          stage: Map.fetch!(item, :stage),
          details: bounded_details(Map.get(item, :details, %{}))
        }
      end)

    {:error, %{base_result | findings: bounded_findings}}
  end

  @spec exit_code(map()) :: non_neg_integer()
  def exit_code(%{state: :ready}), do: 0
  def exit_code(%{operation: :status, outcome: :changes_required}), do: 2
  def exit_code(%{state: :invalid_configuration}), do: 64

  def exit_code(%{state: state})
      when state in [:server_unreachable, :authentication_unavailable],
      do: 69

  def exit_code(%{state: :operation_in_progress}), do: 75
  def exit_code(%{state: :unknown_outcome}), do: 76
  def exit_code(_result), do: 70

  defp base(operation, outcome, state, safe_to_retry, completed_stages, findings) do
    %{
      contract_version: @contract_version,
      operation: operation,
      status: if(state == :ready, do: :ok, else: :error),
      outcome: outcome,
      state: state,
      safe_to_retry: safe_to_retry,
      release: release_identity(),
      completed_stages: completed_stages,
      findings: findings
    }
  end

  defp release_identity do
    %{
      favn_version: RunnerRelease.current_favn_version(),
      latest_migration_version: Migrations.expected_versions() |> List.last()
    }
  end

  defp finding(code, stage, details) do
    %{code: code, stage: stage, details: bounded_details(details)}
  end

  defp bounded_details(details) when is_map(details) do
    details
    |> Map.take([
      :required_action,
      :expected_role,
      :database,
      :workspace_id,
      :subject,
      :category,
      :expected,
      :actual,
      :parent_roles,
      :diagnostic_id,
      :failure_kind,
      :failure_class,
      :failure_location
    ])
    |> Enum.take(8)
    |> Map.new()
  end

  defp bounded_details(_details), do: %{}
end
