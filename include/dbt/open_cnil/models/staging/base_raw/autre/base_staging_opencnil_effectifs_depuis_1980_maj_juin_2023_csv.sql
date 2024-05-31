with source as (
      select * from {{ source('raw_data', 'opencnil_effectifs_depuis_1980_maj_juin_2023_csv') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01 , 01) as annee,
        {{ adapter.quote("postes_budgetaires") }},
        {{ adapter.quote("etpt") }}

    from source
)
select * from renamed
  