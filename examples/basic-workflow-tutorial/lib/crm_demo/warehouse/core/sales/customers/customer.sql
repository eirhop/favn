select
  account.account_id as customer_id,
  account.name as customer_name,
  account.segment,
  account.industry,
  count(contact.contact_id) as contact_count,

  core_metadata(@favn_run_started_at)

from source.account as account
left join source.contact as contact
  on contact.account_id = account.account_id
group by
  account.account_id,
  account.name,
  account.segment,
  account.industry
