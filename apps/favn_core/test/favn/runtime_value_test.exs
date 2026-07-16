defmodule Favn.RuntimeValueTest do
  use ExUnit.Case, async: true

  alias Favn.RuntimeValue

  defmodule Provider do
    @behaviour Favn.RuntimeValue.Provider

    @impl true
    def fetch_runtime_value(:token), do: {:ok, "resolved-secret"}
    def fetch_runtime_value(:error), do: {:error, :unavailable}
  end

  test "resolves a provider-owned value" do
    ref = RuntimeValue.new(Provider, :token, secret?: true)
    assert {:ok, "resolved-secret"} = RuntimeValue.resolve(ref)
  end

  test "inspect output never includes the provider request" do
    ref = RuntimeValue.new(Provider, "request-secret", secret?: true)
    inspected = inspect(ref)

    assert inspected =~ "provider: Favn.RuntimeValueTest.Provider"
    assert inspected =~ "secret?: true"
    refute inspected =~ "request-secret"
  end

  test "normalizes provider failures without exposing their reason" do
    ref = RuntimeValue.new(Provider, :error)

    assert {:error, %Favn.RuntimeValue.Error{reason: :provider_error, provider: Provider}} =
             RuntimeValue.resolve(ref)
  end
end
