with source as (
      select * from {{ source('to_complete_sets', 'gdpr_list_art_eng') }}
),
renamed as (
    select
        {{ adapter.quote("string_field_0") }} as article_nb,
        {{ adapter.quote("string_field_1") }} as article_gdpr

    from source
)
select 
*
from renamed
  