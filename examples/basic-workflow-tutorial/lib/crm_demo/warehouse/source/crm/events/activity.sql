with raw as (
  select *
  from read_json(
    from_json(@files_json, '["VARCHAR"]'),
    format = 'newline_delimited',
    columns = {
      'ActivityId': 'VARCHAR',
      'AccountId': 'VARCHAR',
      'ActivityType': 'VARCHAR',
      'OccurredAt': 'TIMESTAMP'
    }
  )
)

select
  raw."ActivityId" as activity_id,
  raw."AccountId" as account_id,
  raw."ActivityType" as activity_type,
  raw."OccurredAt" as occurred_at,

  source_metadata(@landing_run_id, @extracted_at, md5(to_json(raw)))

from raw
