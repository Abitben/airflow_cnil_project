with source as (
      select * from {{ source('to_complete_sets', 'manquements_gdpr') }}
),
renamed as (
    select
        TRIM(LOWER({{ adapter.quote("string_field_0") }})) as manquement,
        {{ adapter.quote("string_field_1") }} as article_principe, 
        {{ adapter.quote("string_field_2") }} as article_nb,
        {{ adapter.quote("string_field_3") }} as article_title,
        {{ adapter.quote("string_field_4") }} as article_gdpr

    from source
)
select * from renamed
  