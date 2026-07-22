defmodule Favn.SQL.GenerationInspection do
  @moduledoc """
  Bounded physical inspection of a target-generation relation.
  """

  alias Favn.RelationRef
  alias Favn.SQL.{Column, Relation}
  alias Favn.TargetCompatibility.PhysicalFingerprint

  @enforce_keys [:relation_ref, :relation, :columns, :physical_fingerprint]
  defstruct [:relation_ref, :relation, :columns, :physical_fingerprint]

  @type t :: %__MODULE__{
          relation_ref: RelationRef.t(),
          relation: Relation.t(),
          columns: [Column.t()],
          physical_fingerprint: PhysicalFingerprint.t()
        }
end
