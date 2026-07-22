defmodule Favn.RunnerRelease.ModuleFingerprint do
  @moduledoc """
  Canonical executable fingerprint for one module in a runner release.

  Module names are serialized as strings. Decoding never creates atoms from
  artifact data.
  """

  alias Favn.RunnerRelease.Validation

  @enforce_keys [:module, :digest]
  defstruct [:module, :digest]

  @type t :: %__MODULE__{module: String.t(), digest: String.t()}
  @type error ::
          {:missing_runner_release_field, atom()}
          | {:invalid_runner_release_field, atom(), atom()}

  @doc "Builds and validates a module fingerprint."
  @spec new(map() | t()) :: {:ok, t()} | {:error, error()}
  def new(value) when is_map(value) do
    with {:ok, module} <- value |> Validation.fetch(:module) |> then_fetch_module(),
         {:ok, digest} <- value |> Validation.fetch(:digest) |> then_fetch_digest() do
      {:ok, %__MODULE__{module: module, digest: digest}}
    end
  end

  def new(_value), do: {:error, {:invalid_runner_release_field, :runtime_modules, :expected_map}}

  @doc "Returns the canonical identity payload for this module."
  @spec identity_payload(t()) :: map()
  def identity_payload(%__MODULE__{} = fingerprint) do
    %{"module" => fingerprint.module, "digest" => fingerprint.digest}
  end

  defp then_fetch_module({:ok, value}), do: Validation.module_name(value)
  defp then_fetch_module({:error, _reason} = error), do: error

  defp then_fetch_digest({:ok, value}), do: Validation.digest(value, :digest)
  defp then_fetch_digest({:error, _reason} = error), do: error
end
