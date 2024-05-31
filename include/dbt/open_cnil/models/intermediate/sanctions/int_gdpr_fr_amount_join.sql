WITH amounts_by_decision AS (
  SELECT
    DISTINCT decision,
    amount
  FROM
    {{ref('base_to_complete_sets_amounts_sanctions')}}
  WHERE
    amount IS NOT NULL
),

amounts_by_link AS (
  SELECT
    CAST(date_date as DATE) as date_date,
    organisme_type,
    SAFE_CAST(amount AS INT64) as amount
  FROM
    {{ref('base_to_complete_sets_amounts_by_link')}}
)

SELECT
  listes.id,
  COALESCE(amounts.amount, amounts_link.amount) AS amount
FROM
  {{ref('stg_sanc_gdpr_fr')}} AS listes
LEFT JOIN amounts_by_decision AS amounts
  ON listes.decision = amounts.Decision
LEFT JOIN amounts_by_link AS amounts_link
  ON amounts_link.date_date = listes.date_date
  AND amounts_link.organisme_type = listes.organisme_type
ORDER BY
  listes.date_date DESC