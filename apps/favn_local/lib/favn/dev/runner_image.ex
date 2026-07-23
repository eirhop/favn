defmodule Favn.Dev.RunnerImage do
  @moduledoc """
  Validates a customer-built runner image selected by the user.

  This boundary never builds, tags, pulls, or mutates the image. Local
  development requires the image to exist before `mix favn.dev` starts.
  """

  alias Favn.Dev.{Docker, State}
  alias Favn.Manifest.Compatibility
  alias Favn.RunnerRelease

  @type result :: %{
          runner_release_id: String.t(),
          selected_reference: String.t(),
          image_reference: String.t(),
          image_id: String.t(),
          favn_version: String.t(),
          runner_contract_version: pos_integer(),
          target: String.t(),
          status: :selected
        }

  @doc "Inspects and validates the user-selected local runner image."
  @spec ensure(map(), keyword()) :: {:ok, result()} | {:error, term()}
  def ensure(project, opts \\ []) when is_map(project) and is_list(opts) do
    with {:ok, reference} <- image_reference(project),
         {:ok, image} <- Docker.inspect_image(reference, opts),
         {:ok, metadata} <- validate_image(image),
         result = %{
           runner_release_id: metadata.runner_release_id,
           selected_reference: reference,
           image_reference: image.id,
           image_id: image.id,
           favn_version: metadata.favn_version,
           runner_contract_version: metadata.runner_contract_version,
           target: metadata.target,
           status: :selected
         },
         :ok <- persist(result, opts) do
      {:ok, result}
    end
  end

  defp image_reference(%{"runner_image" => reference})
       when is_binary(reference) and reference != "",
       do: {:ok, reference}

  defp image_reference(_project), do: {:error, :runner_image_required}

  defp validate_image(image) do
    release_id = image.labels["io.favn.runner-release-id"]
    favn_version = image.labels["io.favn.version"]
    contract = image.labels["io.favn.runner-contract-version"]
    target = image.labels["io.favn.target"]
    expected_contract = Compatibility.current_runner_contract_version()

    with true <-
           Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, image.id) ||
             {:error, {:runner_image_id_invalid, image.id}},
         true <-
           (image.os == "linux" and image.architecture == "amd64") ||
             {:error, {:unsupported_runner_image_target, image.os, image.architecture}},
         :ok <- RunnerRelease.validate_id(release_id),
         true <-
           favn_version == RunnerRelease.current_favn_version() ||
             {:error,
              {:runner_image_favn_version_mismatch,
               %{expected: RunnerRelease.current_favn_version(), actual: favn_version}}},
         {parsed_contract, ""} <- Integer.parse(contract || ""),
         true <-
           parsed_contract == expected_contract ||
             {:error,
              {:runner_image_contract_mismatch,
               %{expected: expected_contract, actual: parsed_contract}}},
         true <-
           target == RunnerRelease.current_target() ||
             {:error,
              {:runner_image_target_mismatch,
               %{expected: RunnerRelease.current_target(), actual: target}}} do
      {:ok,
       %{
         runner_release_id: release_id,
         favn_version: favn_version,
         runner_contract_version: parsed_contract,
         target: target
       }}
    else
      {:error, {:invalid_runner_release_field, :runner_release_id, :invalid_id}} ->
        {:error, {:runner_image_release_id_invalid, release_id}}

      :error ->
        {:error, {:runner_image_contract_invalid, contract}}

      {:error, _reason} = error ->
        error
    end
  end

  defp persist(result, opts) do
    State.write_runner_latest(
      %{
        "schema_version" => 1,
        "runner_release_id" => result.runner_release_id,
        "selected_reference" => result.selected_reference,
        "image_reference" => result.image_reference,
        "image_id" => result.image_id,
        "favn_version" => result.favn_version,
        "runner_contract_version" => result.runner_contract_version,
        "target" => result.target
      },
      opts
    )
  end
end
