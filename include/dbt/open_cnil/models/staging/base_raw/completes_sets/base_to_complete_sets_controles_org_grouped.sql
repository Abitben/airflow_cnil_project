with source as (
      select * from {{ source('to_complete_sets', 'controles_org_grouped') }}
),
renamed as (
    select
        {{ adapter.quote("organismes") }},
        {{ adapter.quote("grouped_company") }},
        {{ adapter.quote("score") }}

    from source
)
select * from renamed
  