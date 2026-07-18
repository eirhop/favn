defmodule FavnOrchestrator.ApplicationTest do
  use ExUnit.Case, async: true

  test "OTP application metadata is stable while test runtime children stay disabled" do
    assert Application.spec(:favn_orchestrator, :mod) == {FavnOrchestrator.Application, []}

    supervisor = Process.whereis(FavnOrchestrator.Supervisor)
    assert is_pid(supervisor)
    assert Supervisor.which_children(supervisor) == []

    refute Process.whereis(FavnOrchestrator.Persistence.Runtime)
  end
end
