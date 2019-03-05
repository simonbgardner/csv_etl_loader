DROP TABLE IF EXISTS house_sales;


CREATE TABLE house_sales(
 
    _key        TEXT NOT NULL,
    street      TEXT,
    city        TEXT,
    zip         TEXT,
    state       TEXT,   
    beds        INT,
    baths       INT,
    sq__ft      INT,
    type        TEXT,
    sale_date   TEXT,
    price       REAL,
    latitude    REAL,
    longitude   REAL
);



SELECT

    _key,
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