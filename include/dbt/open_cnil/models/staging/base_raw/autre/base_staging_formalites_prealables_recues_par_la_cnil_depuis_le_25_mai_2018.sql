with source as (
      select * from {{ source('raw_data', 'formalites_prealables_recues_par_la_cnil_depuis_le_25_mai_2018') }}
),
renamed as (
    select
        {{ adapter.quote("organisme_raison_sociale") }},
        {{ adapter.quote("organisme_nom_du_service") }},
        {{ adapter.quote("organisme_adresse") }},
        {{ adapter.quote("organisme_code_postal") }},
        {{ adapter.quote("organisme_ville") }},
        {{ adapter.quote("organisme_siren") }},
        {{ adapter.quote("service_charge_du_droit_dacces") }},
        CAST({{ adapter.quote("date_denregistrement") }} AS DATE) as date_denregistrement,
        {{ adapter.quote("finalite_du_traitement") }},
        {{ adapter.quote("categories_et_destinataires_des_donnees") }},
        {{ adapter.quote("numero_denregistrement") }},
        {{ adapter.quote("type_de_formalite_prealable") }}

    from source
)
select * from renamed
  