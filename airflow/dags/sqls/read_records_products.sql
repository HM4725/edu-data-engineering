SELECT *
FROM products
WHERE created_at >= '{{ data_interval_start.strftime("%Y-%m-%d") }}'
  AND created_at < '{{ data_interval_end.strftime("%Y-%m-%d") }}';