defmodule Favn.RuntimeValue.Provider do
  @moduledoc """
  Provider contract for a deferred `Favn.RuntimeValue`.

  Implementations must return bounded, redacted errors. The request is trusted
  deployment configuration, but it must not contain an already-resolved secret.
  Favn invokes the callback in an owned process with a finite 15-second bound.
  """

  alias Favn.RuntimeValue.Error

  @callback fetch_runtime_value(request :: term()) ::
              {:ok, term()} | {:error, Error.t() | term()}
end
