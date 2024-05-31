with source as (
      select * from {{ source('raw_data', 'open_cnil_volumes_med_depuis_2014_maj_aout_2023_csv') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee")}}, 01, 01) as annee,
        {{ adapter.quote("mises_en_demeure_publiques") }},
        {{ adapter.quote("mises_en_demeure_non_publiques") }},
        {{ adapter.quote("total") }}

    from source
)
select * from renamed
  