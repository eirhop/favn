defmodule Favn.SQLAsset.RuntimeInputs do
  @moduledoc """
  Behaviour for resolving bounded SQL parameters immediately before execution.

  A resolver receives the final `Favn.Run.Context` for an executed SQL asset and
  returns either a typed `Favn.SQLAsset.RuntimeInputs.Result` or a typed
  `Favn.SQLAsset.RuntimeInputs.Error`. Resolver modules may perform I/O, but they
  must configure bounded client timeouts below the runner-owned resolution
  deadline.

  Resolved values are ordinary bound SQL parameters. They cannot add SQL source,
  relation names, or lifecycle callbacks. Favn keeps the parameter payload local
  to the runner and exposes only the result identity and explicitly safe metadata.

  The orchestrator atomically pins a successful resolution before execution.
  Later attempts and safe restart recovery reuse that run/node pin without
  invoking the resolver again. New-run behavior is explicit: normal runs resolve
  fresh, exact replay requires source pins, and resume/retry-remaining inherit
  existing pins while resolving nodes the source run never reached. Resolvers
  are selection-only code and must not claim, consume, delete, or write external
  state.

  ## Authoring breadcrumb

  Start with `Favn.AI`, then read `Favn.SQLAsset` for SQL asset placement and
  execution semantics. Declare the resolver in the asset with the sole public
  form:

      @runtime_inputs MyApp.Orders.Inputs

  The attribute must appear at most once before `query`. Anonymous functions,
  captures, MFA tuples, and inline resolver blocks are unsupported. Implement
  `resolve/1` in the named module and return only
  `Favn.SQLAsset.RuntimeInputs.Result` or
  `Favn.SQLAsset.RuntimeInputs.Error`.

  The HexDocs guide
  [Runtime Inputs For SQL Assets](sql-runtime-inputs.html) covers the complete
  workflow, supported bind values, budgets, protected sensitive-value handling,
  and pin/replay boundary. The companion
  [Retries, Replay, And Runtime-Input Pins](retries-and-replay.html) guide covers
  retry precedence and failure safety.
  """

  alias Favn.Run.Context
  alias Favn.SQLAsset.RuntimeInputs.Error
  alias Favn.SQLAsset.RuntimeInputs.Result

  @doc """
  Resolves the SQL parameters for one execution using its final runtime context.

  The resolver may perform bounded I/O. It must not return SQL source,
  credentials in metadata, arbitrary terms, or an untyped error.
  """
  @callback resolve(Context.t()) :: {:ok, Result.t()} | {:error, Error.t()}
end
