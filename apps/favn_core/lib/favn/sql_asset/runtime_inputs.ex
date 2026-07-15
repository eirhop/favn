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

  Runtime inputs are resolved again for a later attempt or runner restart. Until
  an explicit pinning contract is added, replay-sensitive resolvers should not be
  used for work that requires the exact same selected inputs across retries.
  """

  alias Favn.Run.Context
  alias Favn.SQLAsset.RuntimeInputs.Error
  alias Favn.SQLAsset.RuntimeInputs.Result

  @doc """
  Resolves the SQL parameters for one execution using its final runtime context.
  """
  @callback resolve(Context.t()) :: {:ok, Result.t()} | {:error, Error.t()}
end
