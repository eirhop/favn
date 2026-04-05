defmodule Favn.Runtime.Executor do
  @moduledoc """
  Behaviour boundary for asynchronous single-step asset invocation.
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
          {:ok, %{meta: map()}}
          | {:error, error_details()}

  @type execution_handle :: %{
          required(:exec_ref) => reference(),
          required(:monitor_ref) => reference(),
          required(:pid) => pid()
        }

  @callback start_step(Asset.t(), Context.t(), pid(), Favn.asset_ref()) ::
              {:ok, execution_handle()} | {:error, term()}
  @callback cancel_step(execution_handle(), map()) :: :ok | {:error, term()}
end
