WITH user_raw AS (
    SELECT 
        us.user_id as user_id,
        us.name as user_name,
        us.average_stars as average_stars,
        us.review_count as review_count,
        us.funny as funny,
        us.cool as cool,
        us.fans as fans,
        us.friends as friends,
        us.compliment_cool as compliment_cool,
        us.compliment_cute as compliment_cute,
        us.compliment_funny as compliment_funny,
        us.compliment_hot as compliment_hot,
        us.compliment_list as compliment_list,
        us.compliment_more as compliment_more,
        us.compliment_note as compliment_note,
        us.compliment_photos as compliment_photos,
        us.compliment_plain as compliment_plain, 
        us.compliment_profile as compliment_profile,
        us.compliment_writer as compliment_writer
    FROM bronze.user us
)

SELECT * FROM user_raw