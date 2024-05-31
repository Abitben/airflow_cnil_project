with source as (
      select * from {{ source('to_complete_sets', 'amount_by_date') }}
),
renamed as (
    select
        {{ adapter.quote("date_date") }},
        {{ adapter.quote("amount") }}

    from source
)
select * from renamed
  