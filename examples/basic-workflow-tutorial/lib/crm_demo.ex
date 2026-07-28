defmodule CrmDemo do
  @moduledoc """
  A small, complete Favn project you can copy as a starting point.

  The example lands data from a stand-in CRM API and publishes it through three
  warehouse layers. Folder paths mirror module namespaces, so the tree is the
  documentation:

      lib/crm_demo/
        connections/        data-engine connections
        integrations/       source-system transport only
        landing/            imperative Elixir extraction assets
        support/landing/    landing manifest and file storage
        warehouse/source/   typed, source-aligned SQL relations
        warehouse/core/     reusable, source-independent SQL models
        warehouse/mart/     analytics-facing SQL models
        contracts/          reusable output-contract column fragments
        sql/                reusable SQL projection helpers
        lifecycle/          tiny assets for exercising run lifecycle
        pipelines/          runnable entrypoints

  Read `README.md` for the commands and `CrmDemo.Landing.Crm.Extractor` for the
  landing contract that everything downstream depends on.
  """
end
