defmodule Favn.Runner.SupervisedChildrenTest do
  use ExUnit.Case, async: true

  alias Favn.Runner.SupervisedChildren

  test "returns configured OTP child specifications" do
    children = [Agent, {Task, fn -> :ok end}]
    assert {:ok, ^children} = SupervisedChildren.child_specs(children: children)
  end

  test "rejects unknown and invalid options" do
    assert {:error, {:unknown_options, [:unknown]}} =
             SupervisedChildren.child_specs(unknown: true)

    assert {:error, {:invalid_option, :children, :expected_list}} =
             SupervisedChildren.child_specs(children: Agent)
  end
end
