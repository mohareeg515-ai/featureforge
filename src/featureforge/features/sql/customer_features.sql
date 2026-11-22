-- Customer features at a snapshot cutoff T = $cutoff (single source of truth).
--
-- Point-in-time rules (see docs/point_in_time.md):
--   * event time  : invoice_ts / event_ts  <  T   (strictly before the cutoff)
--   * availability: available_at           <= T   (records not yet known at T are invisible)
--   * versions    : if a record was corrected, the latest version available at T wins
--   * windows     : [T - N days, T)   (lower bound inclusive, upper bound exclusive)
WITH params AS (SELECT CAST($cutoff AS TIMESTAMP) AS t),
tx AS (
    SELECT x.*
    FROM transactions x, params p
    WHERE x.invoice_ts < p.t AND x.available_at <= p.t
    QUALIFY row_number() OVER (
        PARTITION BY x.record_id ORDER BY x.available_at DESC, x.ingestion_date DESC) = 1
),
orders AS (
    SELECT customer_id, invoice_no,
           min(invoice_ts) AS order_ts,
           sum(quantity * unit_price) AS order_value
    FROM tx GROUP BY customer_id, invoice_no
),
order_agg AS (
    SELECT o.customer_id,
           count(*) FILTER (WHERE o.order_ts >= p.t - INTERVAL 7 DAY)  AS purchases_7d,
           count(*) FILTER (WHERE o.order_ts >= p.t - INTERVAL 30 DAY) AS purchases_30d,
           count(*) FILTER (WHERE o.order_ts >= p.t - INTERVAL 90 DAY) AS purchases_90d,
           sum(o.order_value) FILTER (WHERE o.order_ts >= p.t - INTERVAL 30 DAY) AS total_spend_30d,
           sum(o.order_value) FILTER (WHERE o.order_ts >= p.t - INTERVAL 90 DAY) AS total_spend_90d,
           sum(o.order_value) / count(*) AS avg_order_value,
           CAST(floor(date_diff('second', max(o.order_ts), p.t) / 86400.0) AS INTEGER)
               AS days_since_last_purchase,
           count(*) / greatest(date_diff('second', min(o.order_ts), p.t) / 86400.0 / 30.0, 1.0)
               AS purchase_frequency
    FROM orders o, params p
    GROUP BY o.customer_id, p.t
),
prod_agg AS (
    SELECT x.customer_id,
           count(DISTINCT x.stock_code) FILTER (WHERE x.invoice_ts >= p.t - INTERVAL 30 DAY)
               AS unique_products_30d
    FROM tx x, params p
    GROUP BY x.customer_id, p.t
),
ev AS (
    SELECT e.*
    FROM customer_events e, params p
    WHERE e.event_ts < p.t AND e.available_at <= p.t
    QUALIFY row_number() OVER (
        PARTITION BY e.event_id ORDER BY e.available_at DESC, e.ingestion_date DESC) = 1
),
ev_agg AS (
    SELECT e.customer_id,
           count(*) FILTER (WHERE e.event_type = 'session_start'
                              AND e.event_ts >= p.t - INTERVAL 7 DAY) AS sessions_7d,
           count(*) FILTER (WHERE e.event_type = 'add_to_cart'
                              AND e.event_ts >= p.t - INTERVAL 7 DAY) AS cart_events_7d
    FROM ev e, params p
    GROUP BY e.customer_id, p.t
),
entities AS (
    SELECT customer_id FROM order_agg
    UNION
    SELECT customer_id FROM ev_agg
)
SELECT
    e.customer_id                                              AS customer_id,
    (SELECT t FROM params)                                     AS snapshot_ts,
    CAST(coalesce(oa.purchases_7d, 0) AS BIGINT)               AS purchases_7d,
    CAST(coalesce(oa.purchases_30d, 0) AS BIGINT)              AS purchases_30d,
    CAST(coalesce(oa.purchases_90d, 0) AS BIGINT)              AS purchases_90d,
    CAST(coalesce(oa.total_spend_30d, 0.0) AS DOUBLE)          AS total_spend_30d,
    CAST(coalesce(oa.total_spend_90d, 0.0) AS DOUBLE)          AS total_spend_90d,
    CAST(oa.avg_order_value AS DOUBLE)                         AS avg_order_value,
    CAST(oa.days_since_last_purchase AS INTEGER)               AS days_since_last_purchase,
    CAST(coalesce(pa.unique_products_30d, 0) AS BIGINT)        AS unique_products_30d,
    CAST(oa.purchase_frequency AS DOUBLE)                      AS purchase_frequency,
    CAST(oa.avg_order_value * oa.purchase_frequency * 12 AS DOUBLE)
                                                               AS customer_lifetime_value_proxy,
    CAST(coalesce(ea.sessions_7d, 0) AS BIGINT)                AS sessions_7d,
    CAST(coalesce(ea.cart_events_7d, 0) AS BIGINT)             AS cart_events_7d
FROM entities e
LEFT JOIN order_agg oa USING (customer_id)
LEFT JOIN prod_agg pa USING (customer_id)
LEFT JOIN ev_agg ea USING (customer_id)
ORDER BY e.customer_id
