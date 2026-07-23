defmodule FavnOrchestrator.RunnerDiagnostics do
  @moduledoc false

  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  @type validation_error ::
          :runner_not_ready
          | :runner_release_info_unavailable
          | :runner_runtime_info_unavailable
          | :runner_node_identity_mismatch

  @spec validate_ready(map(), keyword()) ::
          {:ok, String.t()} | {:error, validation_error()}
  def validate_ready(diagnostics, opts \\ []) when is_map(diagnostics) and is_list(opts) do
    with :ok <- validate_readiness(diagnostics),
         {:ok, release_id} <- validate_release_id(diagnostics),
         :ok <- validate_identity_source(diagnostics),
         :ok <- validate_runtime_contract(diagnostics),
         :ok <- validate_node_identity(diagnostics, opts) do
      {:ok, release_id}
    end
  end

  defp validate_readiness(diagnostics) do
    if field(diagnostics, :available?) == true and
         field(diagnostics, :ready?) == true and
         field(diagnostics, :status) in [:ready, "ready"] do
      :ok
    else
      {:error, :runner_not_ready}
    end
  end

  defp validate_release_id(diagnostics) do
    release_id = field(diagnostics, :runner_release_id)

    case RunnerRelease.validate_id(release_id) do
      :ok -> {:ok, release_id}
      {:error, _reason} -> {:error, :runner_release_info_unavailable}
    end
  end

  defp validate_identity_source(diagnostics) do
    if field(diagnostics, :identity_source) in [:operator, "operator"],
      do: :ok,
      else: {:error, :runner_release_info_unavailable}
  end

  defp validate_runtime_contract(diagnostics) do
    expected_favn_version = RunnerRelease.current_favn_version()
    expected_runner_contract = Compatibility.current_runner_contract_version()

    if field(diagnostics, :favn_version) == expected_favn_version and
         field(diagnostics, :runner_contract_version) == expected_runner_contract do
      :ok
    else
      {:error, :runner_runtime_info_unavailable}
    end
  end

  defp validate_node_identity(diagnostics, opts) do
    actual = field(diagnostics, :node_name)
    expected = opts |> Keyword.get(:runner_node) |> normalize_node_name()

    cond do
      not valid_node_name?(actual) -> {:error, :runner_node_identity_mismatch}
      is_nil(expected) -> :ok
      actual == expected -> :ok
      true -> {:error, :runner_node_identity_mismatch}
    end
  end

  defp valid_node_name?(node_name), do: is_binary(node_name) and node_name != ""

  defp normalize_node_name(nil), do: nil
  defp normalize_node_name(node_name) when is_atom(node_name), do: Atom.to_string(node_name)
  defp normalize_node_name(node_name) when is_binary(node_name), do: node_name
  defp normalize_node_name(_node_name), do: :invalid

  defp field(map, key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key)))
  end
end
