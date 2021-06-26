SELECT
    id                AS album_id,
    name              AS album_name,
    vendor_id         AS album_vendor_id,
    requires_shipping AS album_requires_shipping,
    sku               AS album_sku,
    taxable           AS album_is_taxable,
    status            AS album_status,
    price             AS album_price
FROM stg_albums