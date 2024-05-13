INSERT INTO products (name, price)
VALUES {{ ti.xcom_pull(task_ids='transform', key='records_str') }};
