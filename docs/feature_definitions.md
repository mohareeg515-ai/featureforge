# Feature definitions

_Generated from `src/featureforge/features/definitions.py` by `scripts/generate_feature_docs.py`. Do not edit by hand; a test fails if this file drifts from the registry._

* Transformation version: `1.0.0`
* Entity key: `customer_id`; snapshot timestamp column: `snapshot_ts` (= snapshot date at 00:00:00 UTC, the *cutoff* T).
* All features are computed by one SQL file, `src/featureforge/features/sql/customer_features.sql`.
* Windows are `[T - N days, T)`: lower bound inclusive, upper bound exclusive. Only records with `available_at <= T` are visible (see `point_in_time.md`).
* Features marked **synthetic** depend on generated events: the real Online Retail dataset has no clickstream.

| Feature | Type | Null policy | Source | Calculation | Description |
|---|---|---|---|---|---|
| `purchases_7d` | Int64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `count(distinct invoice_no) where invoice_ts in [T-7d, T)` | Distinct orders in the 7 days before the snapshot. |
| `purchases_30d` | Int64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `count(distinct invoice_no) where invoice_ts in [T-30d, T)` | Distinct orders in the 30 days before the snapshot. |
| `purchases_90d` | Int64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `count(distinct invoice_no) where invoice_ts in [T-90d, T)` | Distinct orders in the 90 days before the snapshot. |
| `total_spend_30d` | Float64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `sum(quantity*unit_price) where invoice_ts in [T-30d, T)` | Sum of quantity*unit_price over the last 30 days. |
| `total_spend_90d` | Float64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `sum(quantity*unit_price) where invoice_ts in [T-90d, T)` | Sum of quantity*unit_price over the last 90 days. |
| `avg_order_value` | Float64 | NULL when it cannot be computed (no purchase history) | normalized.transactions (UCI Online Retail schema) | `sum(order_value)/count(orders) for invoice_ts < T` | Mean order value over the customer's whole eligible history. |
| `days_since_last_purchase` | Int32 | NULL when it cannot be computed (no purchase history) | normalized.transactions (UCI Online Retail schema) | `floor((T - max(invoice_ts)) / 1 day)` | Whole days between the last order and the snapshot. |
| `unique_products_30d` | Int64 | 0 when the customer has no eligible activity | normalized.transactions (UCI Online Retail schema) | `count(distinct stock_code) where invoice_ts in [T-30d, T)` | Distinct stock codes bought in the last 30 days. |
| `purchase_frequency` | Float64 | NULL when it cannot be computed (no purchase history) | normalized.transactions (UCI Online Retail schema) | `n_orders / greatest((T - first_order_ts)/30d, 1)` | Orders per 30 days since first purchase (tenure floor: 30 days). |
| `customer_lifetime_value_proxy` | Float64 | NULL when it cannot be computed (no purchase history) | normalized.transactions (UCI Online Retail schema) | `avg_order_value * purchase_frequency * 12` | PROXY (not a statistical CLV model): avg_order_value * purchase_frequency * 12. |
| `sessions_7d` **(synthetic)** | Int64 | 0 when the customer has no eligible activity | normalized.customer_events (SYNTHETIC) | `count(*) where event_type='session_start' and event_ts in [T-7d, T)` | Synthetic session_start events in the last 7 days. |
| `cart_events_7d` **(synthetic)** | Int64 | 0 when the customer has no eligible activity | normalized.customer_events (SYNTHETIC) | `count(*) where event_type='add_to_cart' and event_ts in [T-7d, T)` | Synthetic add_to_cart events in the last 7 days. |

## Tests per feature

Every feature is covered by `tests/unit/test_features.py` (hand-computed values, window boundaries, null policies, and a cross-check against an independent Polars implementation), `tests/unit/test_point_in_time.py` and `tests/unit/test_feature_store.py` (training/inference value consistency).
