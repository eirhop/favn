defmodule FavnView.ApplicationConfigTest do
  use ExUnit.Case, async: false

  alias FavnView.ApplicationConfig

  setup do
    previous_endpoint = Application.get_env(:favn_view, FavnView.Endpoint)
    previous_session = Application.get_env(:favn_view, :session_cookie_options)
    previous_requirement = Application.get_env(:favn_view, :require_secure_cookies)
    previous_runtime = Application.get_env(:favn_view, :production_runtime_config)

    on_exit(fn ->
      restore_env(FavnView.Endpoint, previous_endpoint)
      restore_env(:session_cookie_options, previous_session)
      restore_env(:require_secure_cookies, previous_requirement)
      restore_env(:production_runtime_config, previous_runtime)
    end)

    :ok
  end

  test "application-owned defaults are production-safe without consumer configuration" do
    Application.delete_env(:favn_view, FavnView.Endpoint)
    Application.delete_env(:favn_view, :session_cookie_options)
    Application.delete_env(:favn_view, :require_secure_cookies)
    Application.put_env(:favn_view, :production_runtime_config, true)

    assert :ok = ApplicationConfig.configure()

    assert Application.fetch_env!(:favn_view, FavnView.Endpoint)[:adapter] ==
             Bandit.PhoenixAdapter

    assert Application.fetch_env!(:favn_view, :session_cookie_options)[:secure]
    assert Application.fetch_env!(:favn_view, :require_secure_cookies)
  end

  test "source development can explicitly opt into a loopback HTTP cookie" do
    Application.delete_env(:favn_view, :session_cookie_options)

    assert :ok = ApplicationConfig.configure([], secure: false)

    refute Application.fetch_env!(:favn_view, :session_cookie_options)[:secure]
  end

  test "Phoenix logging filters browser credentials and session material" do
    filtered =
      Phoenix.Logger.filter_values(%{
        "current_password" => "current",
        "new_password_confirmation" => "replacement",
        "operator_session_token" => "opaque",
        "credential" => "credential-value"
      })

    assert Enum.all?(Map.values(filtered), &(&1 == "[FILTERED]"))
  end

  defp restore_env(key, nil), do: Application.delete_env(:favn_view, key)
  defp restore_env(key, value), do: Application.put_env(:favn_view, key, value)
end
