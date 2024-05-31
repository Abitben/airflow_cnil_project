with source as (
      select * from {{ source('to_complete_sets', 'amounts_sanctions') }}
),
renamed as (
    select
        {{ adapter.quote("Decision") }} as decision,
        {{ adapter.quote("Amount") }} as amount

    from source
)
select * from renamed
  