defmodule FavnOrchestrator.RunnerClientValidatorTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.RunnerClientValidator

  defmodule CompleteClient do
    def register_manifest(_, _), do: :ok
    def submit_work(_, _), do: {:ok, "exec_1"}
    def await_result(_, _, _), do: {:error, :not_started}
    def cancel_work(_, _, _), do: :ok
    def inspect_relation(_, _), do: {:error, :not_supported}
  end

  defmodule PartialClient do
    def register_manifest(_, _), do: :ok
  end

  test "requires every execution callback" do
    assert :ok = RunnerClientValidator.validate(CompleteClient)
    assert {:error, :runner_client_not_available} = RunnerClientValidator.validate(PartialClient)
    assert {:error, :runner_client_not_available} = RunnerClientValidator.validate(:not_loaded)
    assert {:error, :runner_client_not_available} = RunnerClientValidator.validate(nil)
  end
end
