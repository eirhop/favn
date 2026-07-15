defmodule Favn.SQLAsset.RuntimeInputsTest do
  use ExUnit.Case, async: true

  alias Favn.SQLAsset.RuntimeInputs.Result

  test "result inspection redacts sensitive values everywhere they are repeated" do
    result = %Result{
      params: %{snapshot_id: "public", signed_url: "secret-url"},
      identity: "snapshot:secret-url",
      metadata: %{nested: [%{echo: "secret-url"}]},
      sensitive_params: [:signed_url]
    }

    inspected = inspect(result)

    refute inspected =~ "secret-url"
    assert inspected =~ "snapshot:[REDACTED]"
    assert inspected =~ "echo: :redacted"
    assert inspected =~ "snapshot_id: \"public\""
  end
end
