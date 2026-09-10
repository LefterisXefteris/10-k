{{ config(alias='models') }}

select
    hf.model_id,
    companies.cik,
    hf.org,
    hf.downloads,
    hf.likes,
    hf.pipeline_tag,
    hf.library_name,
    hf.created_at
from {{ ref('stg_hf_models') }} as hf
left join {{ ref('companies') }} as companies
    on hf.cik = companies.cik
where hf.model_id is not null
    and hf.model_id <> ''
    and hf.org is not null
    and hf.org <> ''
qualify row_number() over (
    partition by hf.model_id
    order by hf.downloads desc nulls last
) = 1
