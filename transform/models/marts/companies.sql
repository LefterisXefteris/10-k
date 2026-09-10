with filing_companies as (
    select
        cik,
        name
    from {{ ref('stg_filing_entities') }}
),

model_companies as (
    select
        cik,
        trim(regexp_extract(company, '^([^(]+)', 1)) as name
    from {{ ref('stg_hf_models') }}
    where cik is not null
        and cik <> ''
)

select
    cik,
    max_by(name, length(name)) as name
from (
    select * from filing_companies
    union all
    select * from model_companies
)
where cik is not null
    and cik <> ''
    and name is not null
    and name <> ''
group by cik
