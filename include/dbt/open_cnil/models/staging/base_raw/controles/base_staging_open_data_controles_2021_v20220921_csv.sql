with source as (
      select * from {{ source('raw_data', 'open_data_controles_2021_v20220921_csv') }}
),
renamed as (
    select
        DATE(2021, 01, 01) as annee,
        {{ clean_types_controles(adapter.quote("categorie_de_controle"))}} as type_de_controle,
        {{ adapter.quote("modalite_de_controle") }},
        {{ clean_organismes(adapter.quote("organismes_controles")) }} as organismes,
        CAST({{ adapter.quote("dept") }} as INT64) as departement,
        {{ adapter.quote("ville") }} as lieu_de_controle,
        {{ adapter.quote("activite_de_lorganisme") }} as secteur_dactivite,
        INITCAP({{ adapter.quote("pays") }}) as pays,
    from source
)
select * from renamed
  