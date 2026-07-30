defmodule FavnView.Dev.DesignSystem.AuditScript do
  @moduledoc """
  Serves `audit.js` to the design-system page.

  The script is read at compile time and served from a route rather than inlined
  into the page, so the router's content security policy stays intact:
  `script-src 'self'` allows it, and no development surface needs a weaker
  policy than production has.

  It is also not part of `assets/js`, so it is never bundled into `app.js` and
  cannot reach a release.
  """

  @script_path Path.join(__DIR__, "audit.js")
  @external_resource @script_path
  @script File.read!(@script_path)

  @doc """
  The audit script source.
  """
  @spec js() :: String.t()
  def js, do: @script
end
