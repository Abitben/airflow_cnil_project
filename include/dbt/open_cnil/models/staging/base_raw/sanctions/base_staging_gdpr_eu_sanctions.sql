with source as (
      select * from {{ source('raw_data', 'gdpr_eu_sanctions') }}
),
renamed as (
    select
        {{ adapter.quote("etid") }},
        {{ adapter.quote("country") }},
        {{ adapter.quote("authority") }},
        {{ adapter.quote("date_of_decision") }},
        {{ adapter.quote("fine") }},
        {{ adapter.quote("controller_processor") }},
        {{ adapter.quote("sector") }},
        {{ adapter.quote("quoted_art") }},
        {{ adapter.quote("type") }},
        {{ adapter.quote("summary") }},
        {{ adapter.quote("source") }}

    from source
)
select * from renamed
  