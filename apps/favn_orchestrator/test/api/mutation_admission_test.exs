defmodule FavnOrchestrator.API.MutationAdmissionTest do
  use ExUnit.Case, async: true

  import Plug.Test

  alias FavnOrchestrator.API.MutationAdmission
  alias FavnOrchestrator.Lifecycle

  test "read-only requests remain available while mutations fail with a retryable 503" do
    name = unique_name()
    start_supervised!({Lifecycle, name: name, shutdown_drain_timeout_ms: 1_000})
    :ok = Lifecycle.mark_accepting(name)

    admitted = MutationAdmission.call(conn(:post, "/runs"), lifecycle: name)
    assert %{halted: false} = admitted
    assert Lifecycle.diagnostics(name).active_admissions == 1

    :ok = Lifecycle.drain(name)

    _sent = Plug.Conn.send_resp(admitted, 204, "")
    assert Lifecycle.diagnostics(name).active_admissions == 0

    assert %{halted: false} = MutationAdmission.call(conn(:get, "/runs"), lifecycle: name)

    conn = MutationAdmission.call(conn(:post, "/runs"), lifecycle: name)
    assert conn.halted
    assert conn.status == 503

    assert %{
             "error" => %{
               "code" => "runtime_draining",
               "retryable" => true,
               "status" => 503
             }
           } = Jason.decode!(conn.resp_body)
  end

  defp unique_name,
    do: :"mutation_lifecycle_#{System.unique_integer([:positive, :monotonic])}"
end
