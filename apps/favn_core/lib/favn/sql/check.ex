defmodule Favn.SQL.Check do
  @moduledoc """
  Compiled declarative SQL check carried by a manifest SQL asset.

  Checks contain only runtime-required SQL IR and metadata. They can therefore
  render and execute without loading the authoring module that declared them.

  This is the typed compile-time/runtime contract, not the user authoring API.
  Asset authors declare checks with `Favn.SQLAsset.check/3`. Read
  `Favn.SQLAsset` first for transaction semantics and `Favn.SQL.CheckResult` for
  the durable runtime outcome.

  A compiled check records:

  - a unique `name`
  - the `:before_materialize` or `:after_materialize` phase in `at`
  - the `:fail`, `:warn`, or `:skip_materialization` violation policy
  - whether it was authored directly or generated from a contract claim
  - the optional `:target_exists` condition and static message
  - authored SQL plus compiled `Favn.SQL.Template` IR
  - whether nested SQL uses the runtime `query()` or `target()` relation

  Validation also enforces the cross-field rules needed by the runner:
  `:skip_materialization` is before-only and target-existence guarded, and any
  before check using `target()` has the same guard. One asset may carry at most
  50 authored checks plus the three grouped contract claims; messages are
  limited to 1,024 bytes.
  """

  alias Favn.SQL.Template

  @phases [:before_materialize, :after_materialize]
  @violation_policies [:fail, :warn, :skip_materialization]
  @origins [:authored, :contract]
  @conditions [nil, :target_exists]
  @max_message_bytes 1_024
  @max_per_asset 50
  @max_contract_per_asset 3

  @enforce_keys [:name, :at, :on_violation, :sql, :template, :uses_query?, :uses_target?]
  defstruct [
    :name,
    :at,
    :on_violation,
    :when,
    :message,
    :sql,
    :template,
    :file,
    :line,
    origin: :authored,
    claim_id: nil,
    uses_query?: false,
    uses_target?: false
  ]

  @type phase :: :before_materialize | :after_materialize
  @type violation_policy :: :fail | :warn | :skip_materialization
  @type condition :: nil | :target_exists

  @type t :: %__MODULE__{
          name: atom(),
          at: phase(),
          on_violation: violation_policy(),
          when: condition(),
          message: String.t() | nil,
          sql: String.t(),
          template: Template.t(),
          file: String.t() | nil,
          line: pos_integer() | nil,
          origin: :authored | :contract,
          claim_id: String.t() | nil,
          uses_query?: boolean(),
          uses_target?: boolean()
        }

  @doc "Builds a compiled check and rejects invalid persisted contract values."
  @spec new!(map() | keyword()) :: t()
  def new!(fields) when is_map(fields) or is_list(fields) do
    fields = Map.new(fields)
    check = struct!(__MODULE__, fields)
    validate!(check)
  end

  @doc "Validates a compiled check contract."
  @spec validate!(t()) :: t()
  def validate!(%__MODULE__{} = check) do
    unless is_atom(check.name) and not is_nil(check.name),
      do: raise(ArgumentError, "SQL check name must be a non-nil atom")

    unless check.at in @phases,
      do: raise(ArgumentError, "invalid SQL check phase #{inspect(check.at)}")

    unless check.on_violation in @violation_policies,
      do:
        raise(
          ArgumentError,
          "invalid SQL check on_violation value #{inspect(check.on_violation)}"
        )

    unless check.origin in @origins,
      do: raise(ArgumentError, "invalid SQL check origin #{inspect(check.origin)}")

    if check.origin == :contract and
         (not is_binary(check.claim_id) or String.trim(check.claim_id) == ""),
       do: raise(ArgumentError, "contract-generated SQL checks require a claim_id")

    if check.origin == :authored and not is_nil(check.claim_id),
      do: raise(ArgumentError, "authored SQL checks cannot set a contract claim_id")

    unless check.when in @conditions,
      do: raise(ArgumentError, "invalid SQL check condition #{inspect(check.when)}")

    if check.on_violation == :skip_materialization and check.at != :before_materialize,
      do: raise(ArgumentError, ":skip_materialization is valid only before materialization")

    if check.on_violation == :skip_materialization and check.when != :target_exists,
      do: raise(ArgumentError, ":skip_materialization requires when: :target_exists")

    if check.at == :before_materialize and check.uses_target? and check.when != :target_exists,
      do:
        raise(
          ArgumentError,
          "before-materialize checks using target() require when: :target_exists"
        )

    if check.message != nil and not is_binary(check.message),
      do: raise(ArgumentError, "SQL check message must be a string")

    if is_binary(check.message) and byte_size(check.message) > @max_message_bytes,
      do: raise(ArgumentError, "SQL check message exceeds #{@max_message_bytes} bytes")

    unless is_binary(check.sql), do: raise(ArgumentError, "SQL check sql must be a string")

    unless match?(%Template{}, check.template),
      do: raise(ArgumentError, "invalid SQL check template")

    unless is_boolean(check.uses_query?), do: raise(ArgumentError, "uses_query? must be boolean")

    unless is_boolean(check.uses_target?),
      do: raise(ArgumentError, "uses_target? must be boolean")

    check
  end

  @doc "Validates an asset's complete ordered check list."
  @spec validate_list!([t()]) :: [t()]
  def validate_list!(checks) when is_list(checks) do
    checks = Enum.map(checks, &validate!/1)
    authored_count = Enum.count(checks, &(&1.origin == :authored))
    contract_count = Enum.count(checks, &(&1.origin == :contract))

    if authored_count > @max_per_asset do
      raise ArgumentError, "SQL assets support at most #{@max_per_asset} authored checks"
    end

    if contract_count > @max_contract_per_asset do
      raise ArgumentError,
            "SQL contracts support at most #{@max_contract_per_asset} grouped generated checks"
    end

    checks
    |> Enum.group_by(& &1.name)
    |> Enum.find(fn {_name, named_checks} -> length(named_checks) > 1 end)
    |> case do
      nil -> checks
      {name, _named_checks} -> raise ArgumentError, "duplicate SQL check #{inspect(name)}"
    end
  end

  @doc "Returns the maximum supported static check message size in bytes."
  @spec max_message_bytes() :: pos_integer()
  def max_message_bytes, do: @max_message_bytes

  @doc "Returns the maximum number of authored checks supported by one SQL asset."
  @spec max_per_asset() :: pos_integer()
  def max_per_asset, do: @max_per_asset

  @doc "Returns the maximum grouped contract checks supported by one SQL asset."
  @spec max_contract_per_asset() :: pos_integer()
  def max_contract_per_asset, do: @max_contract_per_asset
end
