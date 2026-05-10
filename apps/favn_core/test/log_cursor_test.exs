defmodule Favn.Log.CursorTest do
  use ExUnit.Case, async: true

  alias Favn.Log.Cursor

  test "parses and formats global cursor" do
    assert {:ok, cursor} = Cursor.parse("global:42")
    assert cursor == %Cursor{scope: :global, global_sequence: 42}
    assert Cursor.format(cursor) == "global:42"
  end

  test "parses and formats run cursor with global sequence" do
    assert {:ok, cursor} = Cursor.parse("run:run_123:43")
    assert cursor == %Cursor{scope: :run, run_id: "run_123", global_sequence: 43}
    assert Cursor.format(cursor) == "run:run_123:43"
  end

  test "parses and formats asset cursor with global sequence" do
    assert {:ok, cursor} = Cursor.parse("asset:run_123:asset_step_1:44")

    assert cursor == %Cursor{
             scope: :asset,
             run_id: "run_123",
             asset_step_id: "asset_step_1",
             global_sequence: 44
           }

    assert Cursor.format(cursor) == "asset:run_123:asset_step_1:44"
  end

  test "rejects invalid cursors" do
    assert Cursor.parse("run:run_123:not-int") == {:error, :invalid_cursor}
    assert Cursor.parse("asset:run_123:asset_step_1:-1") == {:error, :invalid_cursor}
    assert Cursor.parse("unknown:1") == {:error, :invalid_cursor}
  end
end
