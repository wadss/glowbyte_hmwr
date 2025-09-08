-- запросы к Greenplume

CREATE TABLE IF NOT EXISTS public.orders (
	order_id TEXT NOT NULL,
	customer_id TEXT NOT NULL,
	product_id TEXT,
	price NUMERIC(23, 5),
	quantity INT,
	order_dt TIMESTAMP,
	update_dt TIMESTAMP
);

INSERT INTO public.orders (order_id, customer_id, product_id, price, quantity, order_dt, update_dt) VALUES 
('1', '1', 'первый заказ', 3, 5, '2025-09-06 00:12:53', NULL),
('2', '1', 'второй заказ', 24.5, 5, '2025-09-06 00:15:53', NULL),
('3', '2', 'третий заказ', 1, 5, '2025-09-01 00:15:53', '2025-09-06 02:12:53'),
('4', '3', 'четвертый заказ', 11, 3, '2025-09-06 01:11:51', NULL),
('5', '4', 'пятый заказ', 12, 3, '2025-09-06 05:11:51', NULL),
('6', '5', 'шестой заказ', 9.5, 5, '2025-09-02 05:11:51', '2025-09-06 05:11:51'),
('1', '1', 'первый заказ', 4, 5, '2025-09-06 00:12:53', '2025-09-06 05:11:51'),
('2', '1', 'второй заказ', 25.5, 5, '2025-09-06 00:15:53', '2025-09-06 06:11:51');

-- Запросы к Clickhouse

CREATE TABLE IF NOT EXISTS default.orders_story (
	`order_id` String NOT NULL,
	`customer_id` String,
	`product_id` String,
	`price` Decimal(23, 5),
	`quantity` UInt32,
	`order_dt` DateTime,
	`update_dt` DateTime NULL,
	`version` UInt32 NOT NULL
)

Engine = ReplacingMergeTree()
ORDER BY (order_id, version)
TTL order_dt + INTERVAL 1 YEAR DELETE;

CREATE TABLE IF NOT EXISTS default.orders_mart
(
    `order_id` String,
    `customer_id` String,
    `product_id` String,
    `price` Decimal(23, 5),
    `quantity` UInt64,
    `order_dt` DateTime,
    `update_dt` Nullable(DateTime),
    `version` UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY order_id
TTL order_dt + INTERVAL 1 YEAR DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.orders_mart_rmv
TO default.orders_mart
AS
SELECT *
FROM default.orders_story;


