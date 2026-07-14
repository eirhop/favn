defmodule FavnOrchestrator.Auth.LoginLimiterTest do
  use ExUnit.Case, async: true

  alias FavnOrchestrator.Auth.LoginLimiter

  test "counts in-flight attempts before password work completes" do
    now = DateTime.utc_now()
    keys = [{:remote, "127.0.0.1"}]

    assert {:allowed, attempts} = LoginLimiter.begin_attempt(%{}, keys, now, 2, 60)
    assert {:allowed, attempts} = LoginLimiter.begin_attempt(attempts, keys, now, 2, 60)
    assert {:blocked, _attempts} = LoginLimiter.begin_attempt(attempts, keys, now, 2, 60)
  end

  test "expires inactive limiter keys after the retention window" do
    now = DateTime.utc_now()
    keys = [{:credential, "user", nil}]

    assert {:allowed, attempts} = LoginLimiter.begin_attempt(%{}, keys, now, 1, 60)
    attempts = LoginLimiter.finish_attempt(attempts, keys, :error, now, 1, 60)
    assert {:blocked, _attempts} = LoginLimiter.begin_attempt(attempts, keys, now, 1, 60)

    later = DateTime.add(now, 301, :second)
    assert {:allowed, _attempts} = LoginLimiter.begin_attempt(attempts, keys, later, 1, 60)
  end
end
