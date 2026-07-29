defmodule CrmDemo.Landing.Crm do
  @moduledoc """
  Shared configuration for every asset that lands CRM data.

  The credentials live here rather than on each asset because every CRM landing
  asset talks to the same system. `env!/2` and `secret_env!/2` record *where* a
  value comes from; the manifest stores the variable name and the secret flag,
  never the value. All three are optional so the tutorial runs without setup.

  `CRM_API_LATENCY_MS` makes the stand-in client wait that long per page. It
  exists because the stand-in answers instantly and a real CRM API does not, so
  every landing asset finished in under a tenth of a second — which makes the
  operator UI's run timeline show a row of identical slivers instead of work
  taking the time work takes. Set it to a few hundred milliseconds to watch a run
  behave the way a real one does:

      CRM_API_LATENCY_MS=800 mix favn.run CrmDemo.Pipelines.CrmDaily --window day:2026-07-23
  """

  use Favn.Namespace

  runtime_config(:crm_api,
    base_url: env!("CRM_API_BASE_URL", required?: false),
    token: secret_env!("CRM_API_TOKEN", required?: false),
    latency_ms: env!("CRM_API_LATENCY_MS", required?: false)
  )

  meta(owner: "crm-demo", tags: [:landing])
end
