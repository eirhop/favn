defmodule Favn.SQL.Catalog.Request do
  @moduledoc """
  Validated one-target catalog publication and its deterministic operation identity.

  Expected version/revision pairs prevent stale CI writes and ABA during rollback.
  An identity binds physical scope, exact artifacts and expectations, not credentials.
  """
  alias Favn.Catalog.Projection
  alias Favn.Semantic.{Artifact, Snapshot}

  @enforce_keys [
    :target,
    :connection,
    :catalog,
    :schema,
    :projections,
    :expectations,
    :operation_id
  ]
  defstruct [
    :target,
    :connection,
    :catalog,
    :schema,
    :projections,
    :expectations,
    :operation_id,
    :semantic
  ]

  @type t :: %__MODULE__{
          target: String.t(),
          connection: atom(),
          catalog: String.t(),
          schema: String.t(),
          projections: [Projection.t()],
          expectations: map(),
          operation_id: String.t(),
          semantic: Artifact.t() | nil
        }

  @doc "Validates target, artifacts and explicitly supplied prior selections."
  @spec new(String.t(), keyword(), [struct()], map()) :: {:ok, t()} | {:error, atom()}
  def new(target, config, artifacts, expectations) do
    with true <- is_binary(target) and Keyword.keyword?(config),
         true <- Enum.sort(Keyword.keys(config)) == [:catalog, :connection, :schema],
         true <- is_atom(config[:connection]) and not is_nil(config[:connection]),
         true <- identifier?(config[:catalog]) and identifier?(config[:schema]),
         true <- is_list(artifacts) and length(artifacts) in 1..2,
         {:ok, projections} <- project(artifacts),
         kinds = Enum.map(projections, & &1.kind),
         true <-
           Enum.uniq(kinds) == kinds and Enum.sort(kinds) == Enum.sort(Map.keys(expectations)),
         true <- Enum.all?(expectations, fn {_, value} -> expected?(value) end) do
      identity = %{
        "connection" => to_string(config[:connection]),
        "catalog" => config[:catalog],
        "schema" => config[:schema],
        "artifacts" => Enum.map(projections, &Map.take(&1, [:kind, :version, :identity])),
        "expectations" => expectations,
        "schema_version" => 1
      }

      {:ok,
       %__MODULE__{
         target: target,
         connection: config[:connection],
         catalog: config[:catalog],
         schema: config[:schema],
         projections: projections,
         expectations: expectations,
         operation_id: Snapshot.digest("cp_", identity),
         semantic: Enum.find(artifacts, &is_struct(&1, Artifact))
       }}
    else
      _ -> {:error, :invalid_publication_request}
    end
  end

  @doc "Parses `version:revision` or the explicit initial selection `none:0`."
  @spec expectation(String.t()) :: {:ok, map()} | {:error, atom()}
  def expectation(value) when is_binary(value) do
    case String.split(value, ":") do
      ["none", "0"] ->
        {:ok, %{"version" => nil, "revision" => 0}}

      [version, revision] ->
        case Integer.parse(revision) do
          {number, ""} when number > 0 ->
            item = %{"version" => version, "revision" => number}
            if expected?(item), do: {:ok, item}, else: {:error, :invalid_expectation}

          _ ->
            {:error, :invalid_expectation}
        end

      _ ->
        {:error, :invalid_expectation}
    end
  end

  def expectation(_), do: {:error, :invalid_expectation}

  defp project(artifacts) do
    Enum.reduce_while(artifacts, {:ok, []}, fn artifact, {:ok, acc} ->
      case Projection.build(artifact) do
        {:ok, projection} -> {:cont, {:ok, [projection | acc]}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, projections} -> {:ok, Enum.sort_by(projections, & &1.kind)}
      error -> error
    end
  end

  defp expected?(%{"version" => nil, "revision" => 0} = value), do: map_size(value) == 2

  defp expected?(%{"version" => version, "revision" => revision} = value),
    do:
      map_size(value) == 2 and identifier?(version) and is_integer(revision) and
        revision in 1..9_223_372_036_854_775_806

  defp expected?(_), do: false

  defp identifier?(value),
    do:
      is_binary(value) and byte_size(value) in 1..255 and String.valid?(value) and
        not String.contains?(value, <<0>>)
end
