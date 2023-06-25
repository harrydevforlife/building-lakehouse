{{
    config(
        unique_key="order_id"
    )
}}

SELECT
    order_id
    ,payment_sequential
    ,payment_type
    ,payment_installments
    ,payment_value
FROM de_mysql.brazillian_ecommerce.olist_order_payments_dataset    