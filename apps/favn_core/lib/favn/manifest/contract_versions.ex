defmodule Favn.Manifest.ContractVersions do
  @moduledoc """
  Compile-time compatibility versions shared by manifests and release artifacts.
  """

  @manifest_schema_version 10
  @runner_contract_version 10

  @doc "Returns the only manifest schema version accepted by this release."
  @spec manifest_schema_version() :: pos_integer()
  def manifest_schema_version, do: @manifest_schema_version

  @doc "Returns the runner protocol version accepted by this release."
  @spec runner_contract_version() :: pos_integer()
  def runner_contract_version, do: @runner_contract_version
end
