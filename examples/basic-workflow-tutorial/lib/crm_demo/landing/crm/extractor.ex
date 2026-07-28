defmodule CrmDemo.Landing.Crm.Extractor do
  @moduledoc """
  Shared extraction mechanics for every CRM landing asset.

  Each asset declaration owns its dataset settings - endpoint, mode, page size,
  date field. This module owns the sequence, which is the same for all of them:

  1. build a client from resolved runtime configuration;
  2. start a new landing run;
  3. fetch page by page and write each page as an immutable data part;
  4. write the completion manifest last; and
  5. return the manifest location and an extraction summary.

  Nothing here knows which dataset it is extracting. Adding a dataset means
  adding an `asset` block, not editing this module.
  """

  alias CrmDemo.Integrations.Crm.Client
  alias CrmDemo.Support.Landing.{Manifest, Storage}

  @doc "Runs one extraction described by the asset's settings and window."
  @spec extract(Favn.Run.Context.t()) :: {:ok, map()}
  def extract(ctx) do
    settings = ctx.asset.settings
    config = ctx.runtime_config.crm_api
    client = Client.new(config.base_url, config.token)

    landing_run_id = "#{ctx.run_id}-attempt-#{ctx.attempt}"
    extracted_at = DateTime.utc_now()

    {files, row_count} = write_parts(client, settings, ctx.window, landing_run_id, 1, nil)

    manifest_path =
      Storage.write_manifest!(%Manifest{
        dataset: settings.dataset,
        landing_run_id: landing_run_id,
        mode: settings.mode,
        window_start: ctx.window && ctx.window.start_at,
        window_end: ctx.window && ctx.window.end_at,
        extracted_at: extracted_at,
        row_count: row_count,
        files: files
      })

    {:ok,
     %{
       dataset: settings.dataset,
       mode: settings.mode,
       landing_run_id: landing_run_id,
       part_count: length(files),
       rows_landed: row_count,
       manifest_path: manifest_path
     }}
  end

  # Every page becomes one part file, including an empty first page, so a window
  # with no source activity still lands an explicit "nothing happened" result.
  defp write_parts(client, settings, window, landing_run_id, index, cursor) do
    {:ok, page} =
      Client.fetch_page(client, settings.endpoint, page_opts(settings, window, cursor))

    path = Storage.write_part!(settings.dataset, landing_run_id, index, page.rows)

    case page.next_cursor do
      nil ->
        {[path], length(page.rows)}

      next_cursor ->
        {files, rows} =
          write_parts(client, settings, window, landing_run_id, index + 1, next_cursor)

        {[path | files], length(page.rows) + rows}
    end
  end

  defp page_opts(settings, nil, cursor) do
    [page_size: settings.page_size, cursor: cursor]
  end

  defp page_opts(settings, window, cursor) do
    [
      page_size: settings.page_size,
      cursor: cursor,
      date_field: settings.date_field,
      changed_since: window.start_at,
      changed_until: window.end_at
    ]
  end
end
