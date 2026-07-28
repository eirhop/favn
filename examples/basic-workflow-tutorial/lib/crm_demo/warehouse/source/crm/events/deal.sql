with raw as (
  select *
  from read_json(
    from_json(@files_json, '["VARCHAR"]'),
    format = 'newline_delimited',
    columns = {
      'DealId': 'VARCHAR',
      'AccountId': 'VARCHAR',
      'Stage': 'VARCHAR',
      'AmountCents': 'BIGINT',
      'OccurredAt': 'TIMESTAMP'
    }
  )
)

select
  raw."DealId" as deal_id,
  raw."AccountId" as account_id,
  raw."Stage" as stage,
  raw."AmountCents" as amount_cents,
  raw."OccurredAt" as occurred_at,

  source_metadata(@landing_run_id, @extracted_at, md5(to_json(raw)))

from raw
