{{ config(
    unique_key='purchase_order_key',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

WITH source_data AS (
    SELECT
        h.purchase_order_hky AS purchase_order_key,
        h.purchase_order_bk AS purchase_order_business_key,
        s.DELETE_FLAG as delete_flag,
        s.SUBTOTAL as subtotal,
        s.TOTALDUE as totaldue,
        s.LDTS AS valid_from
    FROM
        {{ ref('hub_purchase_order') }} h
    JOIN
        {{ ref('sat_purchase_order') }} s
    ON
        h.purchase_order_hky = s.purchase_order_hky
),

ranked_data AS (
    SELECT
        *,
        -- Assign a row number to each record partitioned by the customer's business key.
        ROW_NUMBER() OVER (PARTITION BY purchase_order_key ORDER BY valid_from) as rn,
        -- Calculate the next valid_from date for the valid_to date calculation.
        -- We'll use this to set the end date of the previous record.
        LEAD(valid_from, 1, '9999-12-31') OVER (PARTITION BY purchase_order_key ORDER BY valid_from) AS next_valid_from
    FROM
        source_data
),

final AS (
    SELECT
        purchase_order_key,
        purchase_order_business_key,
        delete_flag,
        subtotal,
        totaldue,
        valid_from,
        -- The valid_to date is the next record's start date, minus 1 second.
        CASE
            WHEN next_valid_from = '9999-12-31' THEN CAST('9999-12-31' AS TIMESTAMP_NTZ)
            ELSE next_valid_from - INTERVAL '1 second'
        END AS valid_to,
        -- Create a flag to easily identify the most current record.
        CASE WHEN next_valid_from = '9999-12-31' THEN TRUE ELSE FALSE END AS is_current
    FROM
        ranked_data
    WHERE   
        delete_flag = 'N'
    AND
        is_current = 'TRUE'
)

SELECT * FROM final 

{% if is_incremental() %}
  -- This is the incremental update logic. We'll only select records that have changed
  -- since the last run to ensure we only append new versions.
  WHERE valid_from > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}