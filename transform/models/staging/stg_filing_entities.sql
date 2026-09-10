-- One row per legal entity named on a filing.
-- Display names look like:
--   NEXTERA ENERGY INC  (NEE, NEE-PN)  (CIK 0000753308), FLORIDA POWER & LIGHT CO  (CIK 0000037634)
-- Split after each (CIK ...) so parent + subsidiary become separate rows.

with split_entities as (
    select
        file_id,
        trim(
            unnest(
                string_split(
                    regexp_replace(company, '(\(CIK\s+[0-9]+\))\s*,\s*', '\1|||', 'g'),
                    '|||'
                )
            )
        ) as entity
    from {{ ref('stg_sec_filings') }}
)

select
    file_id,
    lpad(regexp_extract(entity, '\(CIK\s+([0-9]+)\)', 1), 10, '0') as cik,
    trim(regexp_extract(entity, '^([^(]+)', 1)) as name,
    case
        when regexp_extract(entity, '\(([^)]+)\)', 1) like 'CIK %'
            then null
        else regexp_extract(entity, '\(([^)]+)\)', 1)
    end as tickers_raw
from split_entities
where entity is not null
    and entity <> ''
    and regexp_extract(entity, '\(CIK\s+([0-9]+)\)', 1) is not null
    and regexp_extract(entity, '\(CIK\s+([0-9]+)\)', 1) <> ''
