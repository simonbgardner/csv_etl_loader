
SELECT

    _key
    street,
    city,
    zip,
    state,
    beds,
    baths,
    sq__ft,
    type,
    sale_date,
    price,
    latitude,
    longitude,

FROM house_sales_revisions

JOIN
(
    SELECT
        _key,
        MAX(import_time)
    FROM house_sales_raw
    GROUP BY _key
) deduplicate

ON deduplicate._key = house_sales_raw._key;