defmodule Favn.RunnerPool do
  @moduledoc """
  Opaque logical runner-pool identity shared by authoring and runtime contracts.

  Pool names select an interchangeable runner environment. They do not imply
  size, price, capability, or infrastructure provider.
  """

  alias Favn.RunnerRelease

  @default :default
  @max_name_bytes 63
  @max_pools 64
  @name ~r/\A[a-zA-Z0-9][a-zA-Z0-9._-]*\z/

  @type source_name :: atom()
  @type runtime_name :: String.t()
  @type releases :: %{optional(runtime_name()) => String.t()}

  @doc "Returns the source-level pool used when authoring omits a pool."
  @spec default() :: :default
  def default, do: @default

  @doc "Validates a non-nil source atom without converting runtime input to atoms."
  @spec validate_source(term()) :: :ok | {:error, {:invalid_runner_pool, term()}}
  def validate_source(value) when is_atom(value) and not is_nil(value) do
    value
    |> Atom.to_string()
    |> validate_runtime()
  end

  def validate_source(value), do: {:error, {:invalid_runner_pool, value}}

  @doc "Converts a validated source atom to its canonical manifest string."
  @spec encode(source_name()) :: {:ok, runtime_name()} | {:error, term()}
  def encode(value) do
    with :ok <- validate_source(value), do: {:ok, Atom.to_string(value)}
  end

  @doc "Validates a bounded runtime name without creating an atom."
  @spec validate_runtime(term()) :: :ok | {:error, {:invalid_runner_pool, term()}}
  def validate_runtime(value) when is_binary(value) do
    if byte_size(value) in 1..@max_name_bytes and Regex.match?(@name, value),
      do: :ok,
      else: {:error, {:invalid_runner_pool, value}}
  end

  def validate_runtime(value), do: {:error, {:invalid_runner_pool, value}}

  @doc "Validates a complete canonical pool-to-release manifest map."
  @spec validate_releases(term()) :: :ok | {:error, term()}
  def validate_releases(releases) when is_map(releases) and map_size(releases) in 0..@max_pools do
    Enum.reduce_while(releases, :ok, fn {pool, release_id}, :ok ->
      with :ok <- validate_runtime(pool),
           :ok <- RunnerRelease.validate_id(release_id) do
        {:cont, :ok}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def validate_releases(value), do: {:error, {:invalid_runner_releases, value}}

  @doc "Returns the exact release for a runtime pool name."
  @spec fetch_release(releases(), runtime_name()) :: {:ok, String.t()} | {:error, term()}
  def fetch_release(releases, pool) when is_map(releases) do
    with :ok <- validate_runtime(pool),
         {:ok, release_id} <- Map.fetch(releases, pool) do
      {:ok, release_id}
    else
      :error -> {:error, {:runner_pool_release_not_found, pool}}
      {:error, _reason} = error -> error
    end
  end

  def fetch_release(releases, pool),
    do: {:error, {:invalid_runner_releases, releases, pool}}
end
