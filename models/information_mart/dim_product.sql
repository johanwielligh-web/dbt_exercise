{{ config(
    unique_key='product_key',
    incremental_strategy='merge',
    on_schema_change='fail'
) }}

WITH source_data AS (
    SELECT
        h.Product_hky AS product_key,
        h.Product_bk AS product_business_key,
        s.DELETE_FLAG as delete_flag,
        s.LDTS AS valid_from,
        s.CATEGORY_NAME as category_name,
        s.SUBCATEGORY_NAME as subcategory_name,
        s.LISTPRICE as listprice,
        s.PRODUCT_NAME as product_name       
    FROM
        {{ ref('hub_product') }} h
    JOIN
        {{ ref('sat_product') }} s
    ON
        h.Product_hky = s.Product_hky
),

ranked_data AS (
    SELECT
        *,
        -- Assign a row number to each record partitioned by the customer's business key.
        ROW_NUMBER() OVER (PARTITION BY product_key ORDER BY valid_from) as rn,
        -- Calculate the next valid_from date for the valid_to date calculation.
        -- We'll use this to set the end date of the previous record.
        LEAD(valid_from, 1, '9999-12-31') OVER (PARTITION BY product_key ORDER BY valid_from) AS next_valid_from
    FROM
        source_data
),

final AS (
    SELECT
        product_key,
        product_business_key,
        delete_flag,
        category_name,
        subcategory_name,
        listprice,
        valid_from,
        product_name,
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