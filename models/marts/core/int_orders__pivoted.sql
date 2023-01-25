{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

with payments AS (
    SELECT
        *
    FROM {{ ref('stg_payments') }}
),

final AS (
    SELECT
        order_id,
        {% for payment_method in payment_methods -%}

        SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN amount ELSE 0 END)
            AS {{ payment_method }}_amount

        {%- if not loop.last -%}
            ,
        {% endif -%}

        {%- endfor %}
    FROM {{ ref('stg_payments') }}
    GROUP BY 1
)

SELECT  
    * 
FROM final
ORDER BY order_id