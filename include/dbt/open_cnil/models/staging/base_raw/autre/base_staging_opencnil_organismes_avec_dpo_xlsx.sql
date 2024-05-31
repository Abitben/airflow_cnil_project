with source as (
      select * from {{ source('raw_data', 'opencnil_organismes_avec_dpo_xlsx') }}
),
renamed as (
    select
        {{ adapter.quote("siren_organisme_designant") }},
        {{ adapter.quote("nom_organisme_designant") }},
        {{ adapter.quote("secteur_activite_organisme_designant") }},
        {{ adapter.quote("code_naf_organisme_designant") }},
        {{ adapter.quote("adresse_postale_organisme_designant") }},
        SAFE_CAST(LEFT({{ adapter.quote("code_postal_organisme_designant")}}, 2) AS INT64) as num_dep,
        CONCAT('FR', '-', LEFT({{ adapter.quote("code_postal_organisme_designant") }}, 2)) as code_iso_dep_designant,
        CONCAT('FR', '-', {{ adapter.quote("code_postal_organisme_designant") }}) as zipcode_designant,
        {{ adapter.quote("ville_organisme_designant") }},
        {{ adapter.quote("pays_organisme_designant") }},
        {{ adapter.quote("type_de_dpo") }},
        {{ adapter.quote("date_de_la_designation") }},
        {{ adapter.quote("siren_organisme_designe") }},
        {{ adapter.quote("nom_organisme_designe") }},
        {{ adapter.quote("secteur_activite_organisme_designe") }},
        {{ adapter.quote("code_naf_organisme_designe") }},
        {{ adapter.quote("adresse_postale_organisme_designe") }},
        {{ adapter.quote("code_postal_organisme_designe") }},
        SAFE_CAST(LEFT({{ adapter.quote("code_postal_organisme_designe")}}, 2) AS INT64) as num_dep_designe,
        CONCAT('FR', '-', LEFT({{ adapter.quote("code_postal_organisme_designe") }}, 2)) as code_iso_dep_designe,
        CONCAT('FR', '-', {{ adapter.quote("code_postal_organisme_designe") }}) as zipcode_designant_designe,
        {{ adapter.quote("ville_organisme_designe") }},
        {{ adapter.quote("pays_organisme_designe") }},
        {{ adapter.quote("moyen_contact_dpo_email") }},
        {{ adapter.quote("moyen_contact_dpo_url") }},
        {{ adapter.quote("moyen_contact_dpo_telephone") }},
        {{ adapter.quote("moyen_contact_dpo_adresse_postale") }},
        {{ adapter.quote("moyen_contact_dpo_code_postal") }},
        {{ adapter.quote("moyen_contact_dpo_ville") }},
        {{ adapter.quote("moyen_contact_dpo_pays") }},
        {{ adapter.quote("moyen_contact_dpo_autre") }}

    from source
)
select *,
 {{ code_iso_region('num_dep') }} as code_iso_region_designant,
 {{ code_iso_region('num_dep_designe') }} as code_iso_region_designe
from renamed
  