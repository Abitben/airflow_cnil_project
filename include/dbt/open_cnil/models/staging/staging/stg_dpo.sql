select
    
        if (siren_organisme_designant = 'nan', NULL, siren_organisme_designant) as siren_organisme_designant, 
    
        if (nom_organisme_designant = 'nan', NULL, nom_organisme_designant) as nom_organisme_designant, 
    
        if (secteur_activite_organisme_designant = 'nan', NULL, secteur_activite_organisme_designant) as secteur_activite_organisme_designant, 
    
        if (code_naf_organisme_designant = 'nan', NULL, code_naf_organisme_designant) as code_naf_organisme_designant, 
    
        if (adresse_postale_organisme_designant = 'nan', NULL, adresse_postale_organisme_designant) as adresse_postale_organisme_designant, 
    
        if (code_iso_dep_designant = 'nan', NULL, code_iso_dep_designant) as code_iso_dep_designant, 

        num_dep,
    
        if (zipcode_designant = 'nan', NULL, zipcode_designant) as zipcode_designant, 
    
        if (ville_organisme_designant = 'nan', NULL, ville_organisme_designant) as ville_organisme_designant, 
    
        if (pays_organisme_designant = 'nan', NULL, pays_organisme_designant) as pays_organisme_designant, 
    
        if (type_de_dpo = 'nan', NULL, type_de_dpo) as type_de_dpo, 
    
        if (date_de_la_designation = 'nan', NULL, date_de_la_designation) as date_de_la_designation, 
    
        if (siren_organisme_designe = 'nan', NULL, siren_organisme_designe) as siren_organisme_designe, 
    
        if (nom_organisme_designe = 'nan', NULL, nom_organisme_designe) as nom_organisme_designe, 
    
        if (secteur_activite_organisme_designe = 'nan', NULL, secteur_activite_organisme_designe) as secteur_activite_organisme_designe, 
    
        if (code_naf_organisme_designe = 'nan', NULL, code_naf_organisme_designe) as code_naf_organisme_designe, 
    
        if (adresse_postale_organisme_designe = 'nan', NULL, adresse_postale_organisme_designe) as adresse_postale_organisme_designe, 
    
        if (code_postal_organisme_designe = 'nan', NULL, code_postal_organisme_designe) as code_postal_organisme_designe, 
    
        num_dep_designe, 
    
        if (code_iso_dep_designe = 'nan', NULL, code_iso_dep_designe) as code_iso_dep_designe, 
    
        if (zipcode_designant_designe = 'nan', NULL, zipcode_designant_designe) as zipcode_designant_designe, 
    
        if (ville_organisme_designe = 'nan', NULL, ville_organisme_designe) as ville_organisme_designe, 
    
        if (pays_organisme_designe = 'nan', NULL, pays_organisme_designe) as pays_organisme_designe, 
    
        if (moyen_contact_dpo_email = 'nan', NULL, moyen_contact_dpo_email) as moyen_contact_dpo_email, 
    
        if (moyen_contact_dpo_url = 'nan', NULL, moyen_contact_dpo_url) as moyen_contact_dpo_url, 
    
        if (moyen_contact_dpo_telephone = 'nan', NULL, moyen_contact_dpo_telephone) as moyen_contact_dpo_telephone, 
    
        if (moyen_contact_dpo_adresse_postale = 'nan', NULL, moyen_contact_dpo_adresse_postale) as moyen_contact_dpo_adresse_postale, 
    
        if (moyen_contact_dpo_code_postal = 'nan', NULL, moyen_contact_dpo_code_postal) as moyen_contact_dpo_code_postal, 
    
        if (moyen_contact_dpo_ville = 'nan', NULL, moyen_contact_dpo_ville) as moyen_contact_dpo_ville, 
    
        if (moyen_contact_dpo_pays = 'nan', NULL, moyen_contact_dpo_pays) as moyen_contact_dpo_pays, 
    
        if (moyen_contact_dpo_autre = 'nan', NULL, moyen_contact_dpo_autre) as moyen_contact_dpo_autre, 
    
        if (code_iso_region_designant = 'nan', NULL, code_iso_region_designant) as code_iso_region_designant, 
    
        if (code_iso_region_designe = 'nan', NULL, code_iso_region_designe) as code_iso_region_designe, 
    
from {{ref('base_staging_opencnil_organismes_avec_dpo_xlsx')}}