with source as (
      select * from {{ source('raw_data', 'opencnil_volumes_plaintes_depuis_1981_maj_juin_2023_csv') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        {{ adapter.quote("nombre_de_plaintes_recues") }}

    from source
)
select * from renamed
  