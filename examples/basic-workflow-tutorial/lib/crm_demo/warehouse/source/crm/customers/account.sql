-- Reads only the files the selected landing manifest listed. The paths arrive
-- as a bound JSON array, never interpolated into this file.
with raw as (
  select *
  from read_json(
    from_json(@files_json, '["VARCHAR"]'),
    format = 'newline_delimited',
    columns = {
      'AccountId': 'VARCHAR',
      'Name': 'VARCHAR',
      'Segment': 'VARCHAR',
      'Industry': 'VARCHAR'
    }
  )
)

select
  raw."AccountId" as account_id,
  raw."Name" as name,
  raw."Segment" as segment,
  raw."Industry" as industry,

  source_metadata(@landing_run_id, @extracted_at, md5(to_json(raw)))

from raw
