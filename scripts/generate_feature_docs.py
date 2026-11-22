"""Render docs/feature_definitions.md from the central registry (single source of truth)."""

from __future__ import annotations

from pathlib import Path

from featureforge.features.definitions import ENTITY_KEY, FEATURES, TRANSFORMATION_VERSION

OUT = Path(__file__).resolve().parents[1] / "docs" / "feature_definitions.md"


def render() -> str:
    lines = [
        "# Feature definitions",
        "",
        "_Generated from `src/featureforge/features/definitions.py` by "
        "`scripts/generate_feature_docs.py`. Do not edit by hand; a test fails if this file "
        "drifts from the registry._",
        "",
        f"* Transformation version: `{TRANSFORMATION_VERSION}`",
        f"* Entity key: `{ENTITY_KEY}`; snapshot timestamp column: `snapshot_ts` "
        "(= snapshot date at 00:00:00 UTC, the *cutoff* T).",
        "* All features are computed by one SQL file, `src/featureforge/features/sql/"
        "customer_features.sql`.",
        "* Windows are `[T - N days, T)`: lower bound inclusive, upper bound exclusive. Only "
        "records with `available_at <= T` are visible (see `point_in_time.md`).",
        "* Features marked **synthetic** depend on generated events: the real Online Retail "
        "dataset has no clickstream.",
        "",
        "| Feature | Type | Null policy | Source | Calculation | Description |",
        "|---|---|---|---|---|---|",
    ]
    for f in FEATURES:
        tag = " **(synthetic)**" if f.synthetic_source else ""
        lines.append(
            f"| `{f.name}`{tag} | {f.dtype} | {f.null_policy_text} | {f.source} | "
            f"`{f.calculation}` | {f.description} |"
        )
    lines += [
        "",
        "## Tests per feature",
        "",
        "Every feature is covered by `tests/unit/test_features.py` (hand-computed values, window "
        "boundaries, null policies, and a cross-check against an independent Polars "
        "implementation), `tests/unit/test_point_in_time.py` and "
        "`tests/unit/test_feature_store.py` (training/inference value consistency).",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    OUT.write_text(render(), encoding="utf-8")
    print(f"wrote {OUT}")
