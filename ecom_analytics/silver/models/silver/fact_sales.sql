SELECT
	ro.order_id
	, ro.customer_id
	, ro.order_purchase_timestamp
	, roi.product_id
	, rop.payment_value
	, ro.order_status
FROM {{ source("silver", "olist_orders") }} ro
JOIN {{ source("silver", "olist_order_items")}} roi
ON ro.order_id = roi.order_id
JOIN {{ source("silver", "olist_order_payments")}} rop
ON ro.order_id = rop.order_id