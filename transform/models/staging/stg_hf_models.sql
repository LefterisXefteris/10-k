select
    model_id,
    company,
    org,
    downloads,
    likes,
    pipeline_tag,
    library_name,
    try_cast(created_at as timestamp) as created_at,
    tags,
    lpad(regexp_extract(company, '\(CIK\s+([0-9]+)\)', 1), 10, '0') as cik
from {{ source('landing', 'hf_models') }}
where model_id is not null
    and model_id <> ''
