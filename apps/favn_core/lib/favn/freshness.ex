defmodule Favn.Freshness do
  @moduledoc """
  Documentation namespace for asset freshness concepts.

  Freshness answers whether a previously successful asset result is good enough
  for the current run request. Authors declare freshness on assets with
  `freshness`; the compiler stores the normalized policy in the manifest; the
  orchestrator records successful freshness state and decides whether planned
  execution nodes should run, skip, or block.

  ## Authoring

  In `Favn.Asset`, `Favn.SQLAsset`, and `Favn.MultiAsset`, attach
  at most one `freshness` directly above the asset declaration it belongs to.

      freshness :daily
      freshness {:daily, timezone: "Europe/Oslo"}
      freshness [max_age: {:hours, 6}]
      freshness [window_success: true]
      freshness :always

  Windowed assets default to exact window-success freshness when no explicit
  `freshness` is declared. Non-windowed assets have no implicit freshness policy.

  ## Policy Input

  Read `Favn.Freshness.Policy` for accepted V1 policy values and normalization.
  The supported policy families are:

  - calendar day freshness in a timezone
  - rolling max-age freshness
  - exact window-success freshness
  - always-run freshness

  ## Keys And State

  Read `Favn.Freshness.Key` for stable freshness keys such as `"latest"`,
  `"calendar:day:Etc/UTC:2026-05-09"`, and `"window:<encoded-window-key>"`.

  Freshness state is maintained by the orchestrator as a control-plane read
  model. Internal operator code can inspect it through `FavnOrchestrator`, but it
  is not exposed through `favn_view` yet.
  """
end
