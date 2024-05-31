with source as (
      select * from {{ source('raw_data', 'opencnil_liste_controles_2018_csv') }}
),
renamed as (
    select
        DATE(2018, 01, 01) as annee,
        {{ clean_types_controles(adapter.quote("type_de_controle"))}} as type_de_controle,
        {{ adapter.quote("modalite_de_controle") }},
        {{ clean_organismes(adapter.quote("organismes")) }} as organismes,
        CAST({{ adapter.quote("dep") }} AS INT64) as departement,
        {{ adapter.quote("lieu") }} as lieu_de_controle,
        {{ adapter.quote("secteur_dactivite") }},
        'France' as pays
    from source
)
select * from renamed
LIMIT 1000
OFFSET 2