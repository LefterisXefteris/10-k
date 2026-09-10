select distinct
    entities.file_id,
    entities.cik
from {{ ref('stg_filing_entities') }} as entities
inner join {{ ref('filings') }} as filings
    on entities.file_id = filings.file_id
inner join {{ ref('companies') }} as companies
    on entities.cik = companies.cik
