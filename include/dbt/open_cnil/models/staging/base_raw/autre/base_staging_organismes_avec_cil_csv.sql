with source as (
      select * from {{ source('raw_data', 'organismes_avec_cil_csv') }}
),
renamed as (
    select
        {{ adapter.quote("siren") }},
        {{ adapter.quote("responsable") }},
        {{ adapter.quote("adresse") }},
        {{ adapter.quote("code_postal") }},
        {{ adapter.quote("ville") }},
        {{ adapter.quote("naf") }},
        {{ adapter.quote("typecil") }},
        {{ adapter.quote("portee") }}

    from source
)
select * from renamed
  