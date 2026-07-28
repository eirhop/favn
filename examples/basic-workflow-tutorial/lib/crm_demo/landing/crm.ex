defmodule CrmDemo.Landing.Crm do
  @moduledoc """
  Shared configuration for every asset that lands CRM data.

  The credentials live here rather than on each asset because every CRM landing
  asset talks to the same system. `env!/2` and `secret_env!/2` record *where* a
  value comes from; the manifest stores the variable name and the secret flag,
  never the value. Both are optional so the tutorial runs without setup.
  """

  use Favn.Namespace

  runtime_config(:crm_api,
    base_url: env!("CRM_API_BASE_URL", required?: false),
    token: secret_env!("CRM_API_TOKEN", required?: false)
  )

  meta(owner: "crm-demo", tags: [:landing])
end
