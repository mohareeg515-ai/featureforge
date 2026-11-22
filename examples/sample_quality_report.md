# Quality report: latest

Summary: `{"checks": 41, "passed": 41, "warnings": 0, "critical": 0}`

| Dataset | Check | Status | Severity | Affected | Total |
|---|---|---|---|---:|---:|
| raw.transactions | schema_validation | PASS | info | 0 | 8316 |
| raw.customer_events | schema_validation | PASS | info | 0 | 12790 |
| raw.transactions | non_empty | PASS | info | 0 | 8316 |
| raw.transactions | missing_values[CustomerID] | PASS | info | 517 | 8316 |
| raw.transactions | duplicate_records | PASS | info | 70 | 8316 |
| raw.transactions | duplicate_record_versions | PASS | info | 0 | 8316 |
| raw.customer_events | missing_values[customer_id] | PASS | info | 0 | 12790 |
| raw.customer_events | duplicate_events | PASS | info | 0 | 12790 |
| raw.customer_events | allowed_values[event_type] | PASS | info | 0 | 12790 |
| raw.transactions | volume_anomaly[latest_delta] | PASS | info | 0 | 338 |
| raw.transactions | partition_completeness[coverage_chain] | PASS | info | 0 | 22 |
| raw.customer_events | partition_completeness[coverage_chain] | PASS | info | 0 | 22 |
| normalized.transactions | non_empty | PASS | info | 0 | 7524 |
| normalized.transactions | missing_values[customer_id] | PASS | info | 0 | 7524 |
| normalized.transactions | range[quantity] | PASS | info | 0 | 7524 |
| normalized.transactions | range[unit_price] | PASS | info | 0 | 7524 |
| normalized.transactions | duplicate_record_versions | PASS | info | 0 | 7524 |
| normalized.transactions | rejected_fraction | PASS | info | 792 | 8316 |
| normalized.customer_events | rejected_fraction | PASS | info | 0 | 12790 |
| normalized.customer_events | referential_integrity[customer_id->customer_id] | PASS | info | 8 | 12790 |
| normalized.transactions | freshness[latest_available_transaction] | PASS | info | 0 | 1 |
| features.customer_features | schema_and_null_policy | PASS | info | 0 | 14 |
| features.customer_features | type_validation | PASS | info | 0 | 14 |
| features.customer_features | non_empty | PASS | info | 0 | 281 |
| features.customer_features | duplicate_entity_keys | PASS | info | 0 | 281 |
| features.customer_features | snapshot_ts_equals_cutoff | PASS | critical | 0 | 281 |
| features.customer_features | range[purchases_7d] | PASS | info | 0 | 281 |
| features.customer_features | range[purchases_30d] | PASS | info | 0 | 281 |
| features.customer_features | range[purchases_90d] | PASS | info | 0 | 281 |
| features.customer_features | range[total_spend_30d] | PASS | info | 0 | 281 |
| features.customer_features | range[total_spend_90d] | PASS | info | 0 | 281 |
| features.customer_features | range[unique_products_30d] | PASS | info | 0 | 281 |
| features.customer_features | range[sessions_7d] | PASS | info | 0 | 281 |
| features.customer_features | range[cart_events_7d] | PASS | info | 0 | 281 |
| features.customer_features | range[avg_order_value] | PASS | info | 0 | 281 |
| features.customer_features | range[days_since_last_purchase] | PASS | info | 0 | 281 |
| features.customer_features | range[purchase_frequency] | PASS | info | 0 | 281 |
| features.customer_features | missing_values[avg_order_value] | PASS | info | 1 | 281 |
| features.customer_features | missing_values[purchase_frequency] | PASS | info | 1 | 281 |
| features.customer_features | feature_consistency | PASS | info | 0 | 281 |
| features.customer_features | volume_anomaly[entity_count] | PASS | info | 0 | 281 |
