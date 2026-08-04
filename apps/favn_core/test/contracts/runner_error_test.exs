defmodule Favn.Contracts.RunnerErrorTest do
  use ExUnit.Case, async: true

  alias Favn.Contracts.RunnerError

  describe "text truncation" do
    test "an oversize message is truncated to the validation limit" do
      error = RunnerError.new(message: String.duplicate("x", 10_000))

      assert byte_size(error.message) == 4_096
      assert :ok = RunnerError.validate(error)
    end

    test "a normalized exception with an oversize message stays valid" do
      huge = String.duplicate("y", 8_192)
      error = RunnerError.normalize(%RuntimeError{message: huge}, phase: :runtime_inputs)

      assert byte_size(error.message) <= 4_096
      assert byte_size(error.reason) <= 4_096
      assert :ok = RunnerError.validate(error)
    end

    test "an oversize reason term is truncated to the validation limit" do
      error = RunnerError.normalize(String.duplicate("z", 20_000), phase: :runtime_inputs)

      assert byte_size(error.reason) <= 4_096
      assert :ok = RunnerError.validate(error)
    end

    test "truncation does not split a multibyte codepoint" do
      message = String.duplicate("a", 4_095) <> String.duplicate("ø", 10)
      error = RunnerError.new(message: message)

      assert String.valid?(error.message)
      assert byte_size(error.message) <= 4_096
      assert :ok = RunnerError.validate(error)
    end

    test "text at the limit passes through untouched" do
      message = String.duplicate("m", 4_096)

      assert RunnerError.new(message: message).message == message
    end
  end
end
