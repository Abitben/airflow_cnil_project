with source as (
      select * from {{ source('raw_data', 'opencnil_violationsdcpnotifiees_20240331_csv') }}
),
renamed as (
    select
        {{ adapter.quote("date_de_reception_de_la_notification") }},
        {{ adapter.quote("secteur_dactivite_de_lorganisme_concerne") }},
        {{ adapter.quote("natures_de_la_violation") }},
        {{ adapter.quote("nombre_de_personnes_impactees") }},
        {{ adapter.quote("typologie_des_donnees_impactees") }},
        {{ adapter.quote("donnees_sensibles") }},
        {{ adapter.quote("origines_de_lincident") }},
        {{ adapter.quote("causes_de_lincident") }},
        {{ adapter.quote("information_des_personnes") }}

    from source
)
select * from renamed
  