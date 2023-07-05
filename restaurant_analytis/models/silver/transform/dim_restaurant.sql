WITH restaurant_raw AS (
    SELECT
        sr.business_id as restaurant_id,
        sr.name as restaurant_name,
        sr.categories as categories,
        sr.city as city,
        sr.address as address,
        sr.is_open as is_open,
        sr.latitude as latitude,
        sr.longitude as longitude,
        sr.postal_code as postal_code,
        sr.review_count as review_count,
        sr.stars as stars,
        sr.state as state,
        sr.hours as hours
    FROM {{ source('bronze', 'restaurant')}} sr
)
SELECT * FROM restaurant_raw