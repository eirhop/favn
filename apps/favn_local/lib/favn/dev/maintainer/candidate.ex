defmodule Favn.Dev.Maintainer.Candidate do
  @moduledoc """
  Exact locally loaded control-plane candidate selected for maintainer use.

  The mutable candidate tag is retained only for diagnostics. Local Compose
  always receives the immutable Docker image ID.
  """

  alias Favn.Dev.Maintainer.Source

  @build_id ~r/\A[0-9a-f]{64}\z/
  @image_id ~r/\Asha256:[0-9a-f]{64}\z/
  @revision ~r/\A(?:[0-9a-f]{40,64}|unknown)\z/

  @enforce_keys [
    :control_plane_build_id,
    :image_tag,
    :image_id,
    :candidate_path,
    :image_source_revision,
    :image_source_dirty,
    :checkout,
    :checkout_revision,
    :checkout_dirty,
    :checkout_fingerprint
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          control_plane_build_id: String.t(),
          image_tag: String.t(),
          image_id: String.t(),
          candidate_path: Path.t(),
          image_source_revision: String.t(),
          image_source_dirty: boolean(),
          checkout: Path.t(),
          checkout_revision: String.t(),
          checkout_dirty: boolean(),
          checkout_fingerprint: String.t()
        }

  @doc "Validates one builder result and its persisted candidate descriptor."
  @spec from_build(map(), Source.t()) :: {:ok, t()} | {:error, term()}
  def from_build(
        %{
          control_plane_build_id: build_id,
          image_status: image_status,
          image_tag: image_tag,
          image_id: image_id,
          candidate_path: candidate_path
        },
        %Source{} = source
      )
      when image_status in [:loaded, :reused] and is_binary(build_id) and
             is_binary(image_tag) and is_binary(image_id) and is_binary(candidate_path) do
    with true <- Regex.match?(@build_id, build_id),
         true <- Regex.match?(@image_id, image_id),
         {:ok, %{type: :regular}} <- File.lstat(candidate_path),
         {:ok, encoded} <- File.read(candidate_path),
         {:ok, descriptor} <- JSON.decode(encoded),
         {:ok, image_revision, image_dirty} <-
           validate_descriptor(descriptor, build_id, image_tag, image_id) do
      {:ok,
       %__MODULE__{
         control_plane_build_id: build_id,
         image_tag: image_tag,
         image_id: image_id,
         candidate_path: Path.expand(candidate_path),
         image_source_revision: image_revision,
         image_source_dirty: image_dirty,
         checkout: source.checkout,
         checkout_revision: source.revision,
         checkout_dirty: source.dirty,
         checkout_fingerprint: source.fingerprint
       }}
    else
      _invalid -> {:error, :invalid_maintainer_candidate}
    end
  end

  def from_build(_invalid, %Source{}), do: {:error, :invalid_maintainer_candidate}

  defp validate_descriptor(
         %{
           "schema_version" => 1,
           "control_plane_build_id" => build_id,
           "candidate_tag" => image_tag,
           "image_id" => image_id,
           "source_revision" => revision,
           "source_dirty" => dirty,
           "target" => "linux/amd64"
         },
         build_id,
         image_tag,
         image_id
       )
       when is_binary(revision) and is_boolean(dirty) do
    if Regex.match?(@revision, revision),
      do: {:ok, revision, dirty},
      else: {:error, :invalid_maintainer_candidate}
  end

  defp validate_descriptor(_descriptor, _build_id, _image_tag, _image_id),
    do: {:error, :invalid_maintainer_candidate}
end
