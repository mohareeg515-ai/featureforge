# Point-in-time correctness

**Goal:** a training example observed at time `T` may only use information that was
*known* at `T`. Anything else is label/feature leakage and produces models that look great
offline and fail in production.

## Two clocks per record

| Column | Meaning |
|---|---|
| `invoice_ts` / `event_ts` | **Event time**: when the purchase/event happened. |
| `available_at` | **Availability time**: when the record became visible to the platform. |

Snapshot for date `D` is computed at cutoff `T = D 00:00:00` (UTC). A record is visible iff

```
event_time < T   AND   available_at <= T
```

* Late-arriving data (event before `T`, available after `T`) is **invisible at `T`** and
  becomes visible in later snapshots, where it counts by its *event* time.
* Corrections are new *versions* of the same `record_id` with a later `available_at`.
  The SQL keeps the latest version available at `T` (`QUALIFY row_number() ... ORDER BY
  available_at DESC`), so an old snapshot never sees a correction published later.
* Windows are `[T - N days, T)`.

For the real Online Retail file, which has no availability timestamps, we *assume*
`available_at = InvoiceDate + transaction_lag_hours` (default 2h). This assumption is
recorded in the raw partition metadata.

## As-of retrieval for training

`FeatureStore.get_training_features(observations)` joins each observation
`(customer_id, observation_ts)` to the latest snapshot with `snapshot_ts <= observation_ts`
(Polars backward `join_asof`, with a staleness tolerance). Entities absent from a valid
snapshot follow the feature's null policy; observations with no usable snapshot get NULLs
and a NULL `feature_snapshot_ts` rather than borrowing a future snapshot.

## Incorrect joins this prevents

```text
Observation: customer A at 2025-03-01 12:00.  A buys again on 2025-03-02 03:00.

WRONG  "latest snapshot per entity" join   -> uses the 2025-03-03 snapshot
                                              purchases_7d = 2   (leaks the 03-02 order)
WRONG  join on snapshot_date == obs date   -> misses data / silently drops rows
RIGHT  as-of join (snapshot_ts <= obs_ts)  -> uses the 2025-03-01 snapshot
                                              purchases_7d = 1
```

`tests/unit/test_point_in_time.py::test_training_retrieval_is_asof_and_naive_latest_join_leaks`
executes exactly this scenario and asserts that the naive join leaks while the implemented
join does not.

## Tests that prove it

* Future transactions (and brand-new customers) cannot change earlier features or the input fingerprint.
* A late-arriving record is invisible at `T` and visible after its `available_at`.
* A corrected record uses version 1 before the correction is available and version 2 after.
* Observations before the first snapshot never receive future data.
* Training path, inference path and a fresh recomputation agree **by value** for the same entity and cutoff.

## ML label handling

The demo model's target is computed from transactions *after* `T` (it is the label, not a
feature), only for observation dates whose full 30-day window lies inside the data, with a
30-day purge gap between train and test (see `src/featureforge/ml/demo.py`).
