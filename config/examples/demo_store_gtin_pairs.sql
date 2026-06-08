SELECT store_id, gtin
FROM demo.retail.inventory
WHERE store_id IS NOT NULL
  AND gtin IS NOT NULL
