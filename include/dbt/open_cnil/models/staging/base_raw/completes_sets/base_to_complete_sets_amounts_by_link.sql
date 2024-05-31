with source as (
      select * from {{ source('to_complete_sets', 'amounts_by_link') }}
),
renamed as (
    select
        {{ adapter.quote("DATE") }} as date_date,
        {{ adapter.quote("NOM_OU_TYPE_D_ORGANISME") }} as organisme_type,
        {{ adapter.quote("Amount") }} as amount
    from source
)
select * from renamed
  