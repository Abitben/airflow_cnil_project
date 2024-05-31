with source as (
      select * from {{ source('raw_data', 'gdpr_fr_sanctions') }}
),
renamed as (
    select
        PARSE_DATE('%d/%m/%Y', TRIM({{ adapter.quote("date") }})) as date_date,
        TRIM({{ adapter.quote("organisme_type") }}) as organisme_type,
        {{ adapter.quote("manquements") }},
        {{ adapter.quote("decision") }},
        {{ adapter.quote("theme") }},
        {{ adapter.quote("link")}}

    from source
)
select * from renamed