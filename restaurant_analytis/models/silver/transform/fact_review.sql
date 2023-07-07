WITH review_raw AS (
    SELECT
        rr.review_id as review_id,
        rr.business_id as restaurant_id,
        rr.user_id as user_id,
        rr.stars as stars,
        rr.cool as cool,
        rr.funny as funny,
        rr.useful as useful,
        date_format(to_timestamp(rr.date, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as review_date,
        rr.text as review_description
    FROM {{ source('bronze', 'review')}} rr
)
SELECT * FROM review_raw