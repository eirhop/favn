defmodule FavnOrchestrator.MemoryCapacity.Budget do
  @moduledoc """
  Fixed upper bounds for memory-heavy manifest stages.

  More available RAM permits concurrent non-exclusive work; it never increases
  package or batch sizes without new measured evidence.
  """

  alias Favn.Manifest.ArchiveLimits

  @mib 1_024 * 1_024
  @manifest_base 256 * @mib
  @index_fixed 128 * @mib
  @index_max 512 * @mib
  @index_raw_max ArchiveLimits.current().manifest_index_bytes
  @retained_term_multiplier 4
  @run_decode_fixed 64 * @mib
  @run_decode_multiplier 16

  @doc "Base upload, bundle, package, and low-level package reservation."
  @spec manifest_base() :: pos_integer()
  def manifest_base, do: @manifest_base

  @doc "Reservation for the bounded missing-hash operation."
  @spec missing_hashes() :: pos_integer()
  def missing_hashes, do: 32 * @mib

  @doc "Reservation for a declared manifest-index byte size."
  @spec index(non_neg_integer()) :: pos_integer()
  def index(declared_bytes) when is_integer(declared_bytes) and declared_bytes >= 0 do
    declared_bytes
    |> min(@index_raw_max)
    |> then(&max(@manifest_base, @index_fixed + 6 * &1))
    |> min(@index_max)
  end

  @doc "Reservation for building or verifying an already-decoded manifest term."
  @spec live_index() :: pos_integer()
  def live_index, do: @index_max

  @doc "Returns the index reservation only when the uncompressed bytes are supported."
  @spec persisted_index(non_neg_integer()) ::
          {:ok, pos_integer()} | {:error, :manifest_memory_budget_exceeded}
  def persisted_index(bytes) when is_integer(bytes) and bytes in 0..@index_raw_max,
    do: {:ok, index(bytes)}

  def persisted_index(bytes) when is_integer(bytes) and bytes >= 0,
    do: {:error, :manifest_memory_budget_exceeded}

  @doc "Maximum supported index-stage reservation."
  @spec index_max() :: pos_integer()
  def index_max, do: @index_max

  @doc "Conservative retained-memory estimate for one live BEAM term."
  @spec retained_term_bytes(term()) :: non_neg_integer()
  def retained_term_bytes(term),
    do: @retained_term_multiplier * :erlang.external_size(term)

  @doc "Reservation covering a retained term and its encoded worker handoff."
  @spec worker_handoff(pos_integer()) :: pos_integer()
  def worker_handoff(retained_bytes) when is_integer(retained_bytes) and retained_bytes > 0,
    do: retained_bytes + div(retained_bytes, @retained_term_multiplier)

  @doc "Largest retained result whose term plus encoded handoff fits a working budget."
  @spec serialized_result_limit(pos_integer()) :: pos_integer()
  def serialized_result_limit(working_bytes)
      when is_integer(working_bytes) and working_bytes > 0,
      do: div(@retained_term_multiplier * working_bytes, @retained_term_multiplier + 1)

  @doc "Returns a size-derived persisted-run decode reservation within the run-plan ceiling."
  @spec run_decode(non_neg_integer(), pos_integer()) ::
          {:ok, %{working_bytes: pos_integer(), result_bytes: pos_integer()}}
          | {:error, :manifest_memory_budget_exceeded}
  def run_decode(persisted_bytes, retained_limit)
      when is_integer(persisted_bytes) and persisted_bytes >= 0 and is_integer(retained_limit) and
             retained_limit > 0 do
    result_bytes = max(@run_decode_fixed, @run_decode_multiplier * persisted_bytes)
    working_bytes = worker_handoff(result_bytes)
    maximum = worker_handoff(retained_limit)

    if working_bytes <= maximum,
      do: {:ok, %{working_bytes: working_bytes, result_bytes: result_bytes}},
      else: {:error, :manifest_memory_budget_exceeded}
  end
end
