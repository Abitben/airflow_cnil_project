with source as (
      select * from {{ source('to_complete_sets', 'organismes_secteur') }}
),
renamed as (
    select
        TRIM({{ adapter.quote("string_field_0") }}) as organisme_type,
        {{ adapter.quote("string_field_1") }} as secteur

    from source
)
select * from renamed
  