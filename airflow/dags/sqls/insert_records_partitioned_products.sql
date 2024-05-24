INSERT INTO products_{{ data_interval_start.strftime("%Y%m%d") }} (id, name, price, created_at, updated_at)
VALUES {{ ti.xcom_pull(task_ids='transform') }};
