with source as (
      select * from {{ source('to_complete_sets', 'med_2023_manual') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        {{ adapter.quote("mises_en_demeure_publiques") }},
        {{ adapter.quote("mises_en_demeure_non_publiques") }},
        {{ adapter.quote("total") }}

    from source
)
select * from renamed
  