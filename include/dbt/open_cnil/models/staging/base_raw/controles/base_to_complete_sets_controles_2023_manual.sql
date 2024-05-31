with source as (
      select * from {{ source('to_complete_sets', 'controles_2023_manual') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        {{ adapter.quote("controles_realises") }},
        {{ adapter.quote("dont_controles_videoprotection") }},
        {{ adapter.quote("dont_controles_en_ligne") }},
        {{ adapter.quote("dont_autres_controles_loi_iandl_rgpd") }}

    from source
)
select * from renamed
  