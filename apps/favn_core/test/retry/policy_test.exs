defmodule Favn.Retry.PolicyTest do
  use ExUnit.Case, async: true

  alias Favn.Retry.Backoff
  alias Favn.Retry.Policy

  test "normalizes fixed and exponential policies" do
    assert {:ok, %Policy{max_attempts: 3, backoff: %Backoff{strategy: :fixed} = fixed}} =
             Policy.new(max_attempts: 3, backoff: 250)

    assert fixed.initial_ms == 250
    assert fixed.max_ms == 250

    assert {:ok,
            %Policy{
              max_attempts: 4,
              backoff: %Backoff{strategy: :exponential, initial_ms: 100, max_ms: 1_000}
            }} =
             Policy.new(
               max_attempts: 4,
               backoff: {:exponential, initial: 100, max: 1_000, jitter: 0.2}
             )
  end

  test "bounds exponential delay, jitter, and retry-after" do
    policy =
      Policy.new!(
        max_attempts: 5,
        backoff: {:exponential, initial: 100, max: 500, jitter: 0.2}
      )

    assert Policy.delay_ms(policy, 1, nil, 0.0) == 80
    assert Policy.delay_ms(policy, 3, nil, 0.5) == 400
    assert Policy.delay_ms(policy, 5, 700, 1.0) == 700
    assert Policy.delay_ms(policy, 5, 90_000_000, 1.0) == Backoff.max_delay_ms()
  end

  test "defaults to one attempt and rejects invalid bounds" do
    assert Policy.default().max_attempts == 1
    assert {:error, {:invalid_retry_max_attempts, 0}} = Policy.new(max_attempts: 0)

    assert {:error, {:invalid_backoff_bounds, 1_000, 100}} =
             Policy.new(backoff: %{strategy: :exponential, initial_ms: 1_000, max_ms: 100})
  end
end
