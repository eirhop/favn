defmodule FavnView.ApplicationConfig do
  @moduledoc """
  Owns the baseline runtime configuration required to start `FavnView`.

  Favn can run from its umbrella or as a dependency of another Mix project.
  Dependency configuration files are not loaded by consumer projects, so boot
  callers use this module before starting the View application.
  """

  @endpoint_defaults [
    url: [host: "localhost"],
    adapter: Bandit.PhoenixAdapter,
    render_errors: [
      formats: [html: FavnView.ErrorHTML, json: FavnView.ErrorJSON],
      layout: false
    ],
    pubsub_server: FavnView.PubSub,
    live_view: [signing_salt: "Pqi8zx5Q"]
  ]

  @session_cookie_defaults [
    store: :cookie,
    key: "_favn_view_key",
    signing_salt: "zqy+dPTK",
    encryption_salt: "favn-view-session-v1",
    same_site: "Lax",
    http_only: true,
    secure: true
  ]

  @doc """
  Installs View-owned defaults and applies the supplied endpoint overrides.

  Existing application configuration overrides the defaults. Explicit endpoint
  overrides take final precedence, allowing a boot owner to select listener,
  URL, reload, and secret settings without duplicating View internals.
  """
  @spec configure(keyword(), keyword()) :: :ok
  def configure(endpoint_overrides \\ [], session_overrides \\ [])
      when is_list(endpoint_overrides) and is_list(session_overrides) do
    endpoint =
      @endpoint_defaults
      |> Keyword.merge(Application.get_env(:favn_view, FavnView.Endpoint, []))
      |> Keyword.merge(endpoint_overrides)

    session_cookie_options =
      Keyword.merge(
        @session_cookie_defaults,
        Application.get_env(:favn_view, :session_cookie_options, [])
      )
      |> Keyword.merge(session_overrides)

    Application.put_env(:favn_view, FavnView.Endpoint, endpoint)
    Application.put_env(:favn_view, :session_cookie_options, session_cookie_options)
    configure_production_cookie_requirement()

    :ok
  end

  defp configure_production_cookie_requirement do
    if Application.get_env(:favn_view, :production_runtime_config, false) do
      Application.put_env(
        :favn_view,
        :require_secure_cookies,
        Application.get_env(:favn_view, :require_secure_cookies, true)
      )
    end
  end
end
