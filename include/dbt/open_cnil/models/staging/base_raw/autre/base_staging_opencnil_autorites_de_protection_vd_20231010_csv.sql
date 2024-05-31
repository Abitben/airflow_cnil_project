with source as (
      select * from {{ source('raw_data', 'opencnil_autorites_de_protection_vd_20231010_csv') }}
),
renamed as (
    select
        {{ adapter.quote("zone") }},
        {{ adapter.quote("code_pays_iso") }},
        {{ adapter.quote("nom_du_pays") }},
        {{ adapter.quote("niveau_de_protection") }},
        {{ adapter.quote("membre_de_ledpb_") }},
        {{ adapter.quote("membre_de_lafapdp_") }},
        {{ adapter.quote("site_internet_de_lautorite_independante") }},
        {{ adapter.quote("adresse_postale") }},
        {{ adapter.quote("adresse_corrigee_pour_les_coordonnees_gps_longitude__latitude") }},
        {{ adapter.quote("latitude") }},
        {{ adapter.quote("longitude") }}

    from source
)
select * from renamed
  