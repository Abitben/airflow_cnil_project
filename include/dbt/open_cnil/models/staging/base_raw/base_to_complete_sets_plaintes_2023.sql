with source as (
      select * from {{ source('to_complete_sets', 'plaintes_2023') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        {{ adapter.quote("nombre_de_plaintes_recues") }}

    from source
)
select * from renamed
  