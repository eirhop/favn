defmodule Favn.Runtime.Executor do
  @moduledoc """
  Behaviour boundary for single-step asset invocation.
  """

  alias Favn.Asset
  alias Favn.Run.Context

  @type error_details :: %{
          required(:kind) => :error | :throw | :exit,
          required(:reason) => term(),
          required(:stacktrace) => [term()],
          optional(:message) => String.t()
        }

  @type execution_result ::
          {:ok, %{output: term(), meta: map()}}
          | {:error, error_details()}

  @callback execute_step(Asset.t(), Context.t(), map()) :: execution_result()
end
