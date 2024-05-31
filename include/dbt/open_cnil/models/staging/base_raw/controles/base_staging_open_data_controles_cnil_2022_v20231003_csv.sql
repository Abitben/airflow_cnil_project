with source as (
      select * from {{ source('raw_data', 'open_data_controles_cnil_2022_v20231003_csv') }}
),
renamed as (
    select
        DATE(2022, 01, 01) as annee,
        {{ clean_types_controles(adapter.quote("categorie__de_controle"))}} as type_de_controle,
        {{ clean_organismes(adapter.quote("organismes_controles")) }} as organismes,
        {{ adapter.quote("modalites_de_controle") }} as modalite_de_controle,
        CAST({{ adapter.quote("dept") }} as INT64) as departement,
        {{ adapter.quote("ville") }} as lieu_de_controle,
        {{ adapter.quote("activite__de_lorganisme") }} as secteur_dactivite,
        INITCAP({{ adapter.quote("pays") }}) as pays
    from source
)
select * from renamed