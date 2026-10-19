-- Full rebuild: har run par warehouse schema dobara banta hai (idempotent)
DROP SCHEMA IF EXISTS warehouse CASCADE;
CREATE SCHEMA warehouse;

-- ---------- Dimensions ----------
CREATE TABLE warehouse.dim_date AS
SELECT
    to_char(d, 'YYYYMMDD')::int          AS date_key,
    d::date                              AS full_date,
    EXTRACT(YEAR FROM d)::int            AS year,
    EXTRACT(QUARTER FROM d)::int         AS quarter,
    EXTRACT(MONTH FROM d)::int           AS month,
    to_char(d, 'Mon')                    AS month_name,
    EXTRACT(WEEK FROM d)::int            AS week_of_year,
    EXTRACT(ISODOW FROM d)::int          AS day_of_week,
    to_char(d, 'Dy')                     AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7)     AS is_weekend
FROM generate_series(
        (SELECT date_trunc('year', MIN(order_date)) FROM public.orders),
        (SELECT date_trunc('year', MAX(order_date)) + interval '1 year' - interval '1 day' FROM public.orders),
        interval '1 day') AS d;
ALTER TABLE warehouse.dim_date ADD PRIMARY KEY (date_key);

CREATE TABLE warehouse.dim_customer AS
SELECT user_id AS customer_key, name, email, country, created_at AS signup_date
FROM public.users;
ALTER TABLE warehouse.dim_customer ADD PRIMARY KEY (customer_key);

CREATE TABLE warehouse.dim_seller AS
SELECT seller_id AS seller_key, seller_name, rating, created_at
FROM public.sellers;
ALTER TABLE warehouse.dim_seller ADD PRIMARY KEY (seller_key);

CREATE TABLE warehouse.dim_product AS
SELECT p.product_id AS product_key, p.name AS product_name,
       c.category_name, p.price AS list_price
FROM public.products p
JOIN public.categories c USING (category_id);
ALTER TABLE warehouse.dim_product ADD PRIMARY KEY (product_key);

-- ---------- Fact (grain: ek order item = ek row) ----------
CREATE TABLE warehouse.fact_orders AS
SELECT
    oi.order_item_id,
    o.order_id,
    to_char(o.order_date, 'YYYYMMDD')::int AS date_key,
    o.user_id      AS customer_key,
    oi.product_id  AS product_key,
    p.seller_id    AS seller_key,
    o.status       AS order_status,
    pay.method     AS payment_method,
    pay.status     AS payment_status,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS line_amount
FROM public.order_items oi
JOIN public.orders   o   USING (order_id)
JOIN public.products p   ON p.product_id = oi.product_id
LEFT JOIN public.payments pay ON pay.order_id = o.order_id;

ALTER TABLE warehouse.fact_orders ADD PRIMARY KEY (order_item_id);
CREATE INDEX ON warehouse.fact_orders (date_key);
CREATE INDEX ON warehouse.fact_orders (customer_key);
CREATE INDEX ON warehouse.fact_orders (product_key);
CREATE INDEX ON warehouse.fact_orders (seller_key);

ANALYZE;