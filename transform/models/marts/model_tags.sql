with exploded as (
    select
        model_id,
        trim(unnest(string_split(tags, ';'))) as tag
    from {{ ref('stg_hf_models') }}
    where tags is not null
        and tags <> ''
)

select distinct
    exploded.model_id,
    exploded.tag
from exploded
inner join {{ ref('hf_models') }} as models
    on exploded.model_id = models.model_id
where exploded.tag <> ''
