select
    file_id,
    form,
    file_date,
    period_ending,
    location,
    accession
from {{ ref('stg_sec_filings') }}
where file_id is not null
    and file_id <> ''
    and form is not null
    and form <> ''
qualify row_number() over (partition by file_id order by file_date desc nulls last) = 1
