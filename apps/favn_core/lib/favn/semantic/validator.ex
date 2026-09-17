defmodule Favn.Semantic.Validator do
  @moduledoc """
  Build-time boundary for closed native SQL expression validation.

  Implementations parse the expression before binding its quoted input columns
  to an isolated, empty relation. They own native sessions, deadlines, cleanup,
  type profiles and dialect qualification. Core never opens a data connection.
  A validation result describes the synthetic binding, not the exact physical
  type of a later consumer query.
  """

  @type input :: %{name: String.t(), type: atom(), nullable: boolean()}
  @type result :: %{
          required(:native_type) => String.t(),
          required(:nullable) => :unknown | boolean(),
          optional(:runtime_version) => String.t(),
          optional(:compiler_version) => String.t(),
          optional(:validation_profile) => map()
        }

  @callback validate(String.t(), [input()]) :: {:ok, result()} | {:error, term()}
end
