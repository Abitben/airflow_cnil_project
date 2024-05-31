with source as (
      select * from {{ source('raw_data', 'opencnil_dai_stic_judex_taj_maj_janvier_2019_xlsx') }}
),
renamed as (
    select
        {{ adapter.quote("annee") }},
        {{ adapter.quote("2012") }},
        {{ adapter.quote("2013") }},
        {{ adapter.quote("2014") }},
        {{ adapter.quote("2015") }},
        {{ adapter.quote("2016") }},
        {{ adapter.quote("2017") }},
        {{ adapter.quote("2018") }}

    from source
)
select * from renamed
  