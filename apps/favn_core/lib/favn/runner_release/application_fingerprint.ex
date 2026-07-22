defmodule Favn.RunnerRelease.ApplicationFingerprint do
  @moduledoc """
  Canonical runtime dependency identity for one OTP application.

  `lock_fingerprint` represents the normalized production lock/source input
  selected by the runner builder; it is not a filesystem or registry locator.
  """

  alias Favn.RunnerRelease.Validation

  @enforce_keys [:application, :version, :lock_fingerprint]
  defstruct [:application, :version, :lock_fingerprint]

  @type t :: %__MODULE__{
          application: String.t(),
          version: String.t(),
          lock_fingerprint: String.t()
        }

  @type error ::
          {:missing_runner_release_field, atom()}
          | {:invalid_runner_release_field, atom(), atom()}

  @doc "Builds and validates an application fingerprint."
  @spec new(map() | t()) :: {:ok, t()} | {:error, error()}
  def new(value) when is_map(value) do
    with {:ok, application} <- required_identifier(value, :application, 128),
         {:ok, version} <- required_string(value, :version, 128),
         {:ok, lock_fingerprint} <- required_digest(value, :lock_fingerprint) do
      {:ok,
       %__MODULE__{
         application: application,
         version: version,
         lock_fingerprint: lock_fingerprint
       }}
    end
  end

  def new(_value),
    do: {:error, {:invalid_runner_release_field, :runtime_applications, :expected_map}}

  @doc "Returns the canonical identity payload for this application."
  @spec identity_payload(t()) :: map()
  def identity_payload(%__MODULE__{} = fingerprint) do
    %{
      "application" => fingerprint.application,
      "version" => fingerprint.version,
      "lock_fingerprint" => fingerprint.lock_fingerprint
    }
  end

  defp required_identifier(value, field, max_bytes) do
    case Validation.fetch(value, field) do
      {:ok, field_value} -> Validation.identifier(field_value, field, max_bytes)
      {:error, _reason} = error -> error
    end
  end

  defp required_string(value, field, max_bytes) do
    case Validation.fetch(value, field) do
      {:ok, field_value} -> Validation.string(field_value, field, max_bytes)
      {:error, _reason} = error -> error
    end
  end

  defp required_digest(value, field) do
    case Validation.fetch(value, field) do
      {:ok, field_value} -> Validation.digest(field_value, field)
      {:error, _reason} = error -> error
    end
  end
end
