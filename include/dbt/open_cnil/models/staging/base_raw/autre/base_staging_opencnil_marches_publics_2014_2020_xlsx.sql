with source as (
      select * from {{ source('raw_data', 'opencnil_marches_publics_2014_2020_xlsx') }}
),
renamed as (
    select
        {{ adapter.quote("nature_du_marche") }},
        {{ adapter.quote("tranche_de_montant_minimum_ht") }},
        {{ adapter.quote("tranche_de_montant_maximum_ht") }},
        {{ adapter.quote("objet_du_marche") }},
        {{ adapter.quote("date_de_notification") }},
        {{ adapter.quote("nom_de_lattributaire") }},
        {{ adapter.quote("code_postal_de_lattributaire") }},
        {{ adapter.quote("montant_du_marche_forfait_annuel_ht") }},
        {{ adapter.quote("montant_minimum_du_marche__montant_minimum_pour_la_partie_unitaire_ht") }},
        {{ adapter.quote("montant_maximum_du_marche__montant_maximum_pour_la_partie_unitaire_ht") }}

    from source
)
select * from renamed
  