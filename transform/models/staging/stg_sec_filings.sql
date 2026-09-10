select
    file_id,
    company,
    cik as cik_raw,
    form,
    try_cast(file_date as date) as file_date,
    try_cast(period_ending as date) as period_ending,
    location,
    accession
from {{ source('landing', 'sec_filings') }}
