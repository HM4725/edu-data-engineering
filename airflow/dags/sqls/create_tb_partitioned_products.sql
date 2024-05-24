CREATE TABLE IF NOT EXISTS products_{{ data_interval_start.strftime("%Y%m%d") }} (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255),
    price INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id),
    INDEX (created_at),
    INDEX (updated_at)
);
