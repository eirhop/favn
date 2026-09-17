defmodule Favn.Semantic.Validator do
  @moduledoc """
  Build-time boundary for closed native SQL expression validation.

  Implementations parse the expression before binding its quoted input columns
  to an isolated, empty relation. They own native sessions, deadlines, cleanup,
  type profiles and dialect qualification. Core never opens a data connection.
  Core supplies `locations`: zero-based UTF-8 byte offsets of generated quoted
  column tokens in the expression. Validators must reject column references at
  any other location, including otherwise valid bare references to those inputs.
  `allowed_aggregate_locations` is nil for a leaf expression, or the exact sorted
  locations of its inlined metric aggregates for composition. The native parser
  must reject any added or missing aggregate before binding. Its returned
  `aggregate_locations` are ephemeral compilation evidence, never artifact data.
  A validation result describes the synthetic binding, not the exact physical
  type of a later consumer query.
  """

  @type input :: %{
          required(:name) => String.t(),
          required(:type) => atom(),
          required(:nullable) => boolean(),
          optional(:locations) => [non_neg_integer()]
        }
  @type result :: %{
          required(:native_type) => String.t(),
          required(:nullable) => :unknown | boolean(),
          required(:runtime_version) => String.t(),
          required(:compiler_version) => String.t(),
          required(:validation_profile) => %{String.t() => String.t()},
          required(:aggregate_locations) => [non_neg_integer()]
        }
  @type options :: [allowed_aggregate_locations: [non_neg_integer()] | nil]

  @callback validate(String.t(), [input()], options()) :: {:ok, result()} | {:error, term()}
end
