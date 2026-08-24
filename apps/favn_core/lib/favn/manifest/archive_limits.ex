defmodule Favn.Manifest.ArchiveLimits do
  @moduledoc """
  Shared build and upload limits for the first-party manifest archive.

  The builder rejects an archive that the Orchestrator would reject, so users
  never need to choose package or transport chunk sizes themselves.
  """

  @compressed_bytes 256 * 1_024 * 1_024
  @expanded_bytes 1_024 * 1_024 * 1_024
  @bundle_bytes 4 * 1_024 * 1_024
  @manifest_index_bytes 64 * 1_024 * 1_024
  @execution_package_bytes 4 * 1_024 * 1_024
  @execution_packages 10_000
  @tar_entries 10_002
  @package_batch_count 100
  @package_batch_bytes 32 * 1_024 * 1_024
  @read_chunk_bytes 1 * 1_024 * 1_024
  @upload_timeout_ms 15 * 60 * 1_000

  @type t :: %{
          compressed_bytes: pos_integer(),
          expanded_bytes: pos_integer(),
          bundle_bytes: pos_integer(),
          manifest_index_bytes: pos_integer(),
          execution_package_bytes: pos_integer(),
          execution_packages: pos_integer(),
          tar_entries: pos_integer(),
          package_batch_count: pos_integer(),
          package_batch_bytes: pos_integer(),
          read_chunk_bytes: pos_integer(),
          upload_timeout_ms: pos_integer()
        }

  @doc "Returns the stable manifest archive protocol limits."
  @spec current() :: t()
  def current do
    %{
      compressed_bytes: @compressed_bytes,
      expanded_bytes: @expanded_bytes,
      bundle_bytes: @bundle_bytes,
      manifest_index_bytes: @manifest_index_bytes,
      execution_package_bytes: @execution_package_bytes,
      execution_packages: @execution_packages,
      tar_entries: @tar_entries,
      package_batch_count: @package_batch_count,
      package_batch_bytes: @package_batch_bytes,
      read_chunk_bytes: @read_chunk_bytes,
      upload_timeout_ms: @upload_timeout_ms
    }
  end
end
