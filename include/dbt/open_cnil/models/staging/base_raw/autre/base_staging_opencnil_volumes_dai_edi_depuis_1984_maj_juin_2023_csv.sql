with source as (
      select * from {{ source('raw_data', 'opencnil_volumes_dai_edi_depuis_1984_maj_juin_2023_csv') }}
),
renamed as (
    select
        {{ adapter.quote("annee") }} as annee,
        {{ adapter.quote("nombre_de_dossiers_recus_dans_lannee") }},
        {{ adapter.quote("nombre_de_verifications_de_fichiers") }}

    from source
    LIMIT 1000
    OFFSET 1
)
select 
DATE(CAST({{ adapter.quote("annee") }} as INT64), 01, 01) as annee,
nombre_de_dossiers_recus_dans_lannee,
nombre_de_verifications_de_fichiers,
from renamed
ORDER BY 
annee