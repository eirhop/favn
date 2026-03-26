defmodule Flux.RefTest do
  use ExUnit.Case, async: true

  alias Flux.Ref

  test "builds a canonical ref" do
    ref = Ref.new(Example.Assets, :normalize_orders)

    assert ref == {Example.Assets, :normalize_orders}
  end
end
