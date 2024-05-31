with source as (
      select * from {{ source('raw_data', 'opencnil_budget_depuis_2000_maj_mars_2024_csv') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        CAST(REPLACE(({{ adapter.quote("budget_total") }}), " ","") AS INT64) as budget_total,
        CAST(REPLACE({{ adapter.quote("dont_personnel_titre_2") }}," ","") AS INT64) as dont_personnel,
        CAST(REPLACE({{ adapter.quote("dont_fonctionnement") }}," ","") AS INT64) as dont_fonctionnement

    from source
)
select * from renamed