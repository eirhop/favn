defmodule FavnView.ApplicationTest do
  use ExUnit.Case, async: true

  test "prep_stop drains the control plane before the View supervisor stops" do
    parent = self()

    state = %{
      runtime?: true,
      drain: fn ->
        send(parent, :drained)
        {:ok, %{}}
      end
    }

    assert ^state = FavnView.Application.prep_stop(state)
    assert_receive :drained
  end
end
