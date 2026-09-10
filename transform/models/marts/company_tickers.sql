with exploded as (
    select
        cik,
        trim(unnest(string_split(tickers_raw, ','))) as ticker
    from {{ ref('stg_filing_entities') }}
    where tickers_raw is not null
        and tickers_raw <> ''
        and cik is not null
        and cik <> ''
)

select distinct
    exploded.cik,
    exploded.ticker
from exploded
inner join {{ ref('companies') }} as companies
    on exploded.cik = companies.cik
where exploded.ticker <> ''
