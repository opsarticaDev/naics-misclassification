"""
Step [6]: Compute validation metrics from annotated sample.

Reads Analysis/validation_sample_2023.xlsx (one tab per stratum) after
reviewer fills in:
  - reviewer_naics: correct 6-digit NAICS code, or "UNK" if undeterminable
  - reviewer_notes: free text (typically the correct website/source)

Derives correctness by comparing:
  - reviewer_naics vs reported_naics  -> was the reported code correct?
  - reviewer_naics vs suggested_naics -> was the pipeline suggestion correct?

UNK rows are excluded from calculations.

Outputs:
  - Analysis/validation_metrics_2023.csv (one row per stratum)
  - Console summary

Dry-run mode: if annotations are empty, prints instructions and exits.
"""

import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLE_PATH = BASE_DIR / "Analysis" / "validation_sample_2023.xlsx"
METRICS_PATH = BASE_DIR / "Analysis" / "validation_metrics_2023.csv"

STRATA = ["CLEAN", "FLAGGED_NOT_PROMOTED", "AUTO_RESOLVED", "MANUAL_REVIEW", "NO_DATA"]

POOL_SIZES = {
    "CLEAN": 188_927,
    "FLAGGED_NOT_PROMOTED": 159_292,
    "AUTO_RESOLVED": 44_759,
    "MANUAL_REVIEW": 610,
    "NO_DATA": 646,
}


def _normalize_naics(val):
    """Normalize NAICS to 6-digit string, or None/UNK."""
    if pd.isna(val) or str(val).strip() == "":
        return None
    s = str(val).strip().upper()
    if s == "UNK":
        return "UNK"
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


def main():
    if not SAMPLE_PATH.exists():
        print(f"ERROR: {SAMPLE_PATH} not found. Run validation_sample.py first.")
        sys.exit(1)

    # Load all tabs
    all_frames = []
    xls = pd.ExcelFile(SAMPLE_PATH)
    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        df["stratum"] = sheet
        all_frames.append(df)
    combined = pd.concat(all_frames, ignore_index=True)
    print(f"Loaded {len(combined)} rows from {SAMPLE_PATH.name} ({len(xls.sheet_names)} tabs)")

    # Normalize
    combined["_reviewer"] = combined["reviewer_naics"].apply(_normalize_naics)
    combined["_reported"] = combined["reported_naics"].apply(_normalize_naics)
    combined["_suggested"] = combined["suggested_naics"].apply(_normalize_naics)

    # Check annotation status
    has_annotation = combined["_reviewer"].notna() & (combined["_reviewer"] != "")
    n_annotated = has_annotation.sum()

    if n_annotated == 0:
        print("\n" + "=" * 60)
        print("DRY RUN -- No annotations found.")
        print("=" * 60)
        print("""
To annotate, open Analysis/validation_sample_2023.xlsx and fill in:

  reviewer_naics:
    The correct 6-digit NAICS code for this establishment.
    Use "UNK" if you cannot determine the correct code.

  reviewer_notes:
    Source used (website URL, etc.) or any notes.

After annotation, re-run this script to compute metrics.
""")
        for stratum in STRATA:
            n = (combined["stratum"] == stratum).sum()
            print(f"  {stratum}: {n} records to annotate")
        sys.exit(0)

    # Filter to annotated, non-UNK rows for metrics
    scoreable = combined[has_annotation & (combined["_reviewer"] != "UNK")].copy()
    n_unk = (combined["_reviewer"] == "UNK").sum()

    if n_annotated < len(combined):
        print(f"\nWARNING: {n_annotated}/{len(combined)} rows annotated.")
    if n_unk > 0:
        print(f"UNK rows excluded from metrics: {n_unk}")
    print(f"Scoreable rows: {len(scoreable)}")

    # --- Per-stratum metrics ---
    results = []
    for stratum in STRATA:
        group = scoreable[scoreable["stratum"] == stratum]
        n = len(group)
        if n == 0:
            results.append({"stratum": stratum, "n_scored": 0})
            continue

        # Reported NAICS accuracy: how often the establishment's own code is correct
        reported_correct = (group["_reviewer"] == group["_reported"]).sum()
        reported_accuracy = reported_correct / n

        # Pipeline detection metrics
        # "Misclassified" = reviewer disagrees with reported NAICS
        truly_misclassified = group["_reviewer"] != group["_reported"]
        truly_correct = ~truly_misclassified

        row = {
            "stratum": stratum,
            "n_scored": n,
            "pool_size": POOL_SIZES.get(stratum, ""),
            "reported_accuracy": round(reported_accuracy, 4),
            "n_reported_correct": reported_correct,
            "n_reported_wrong": n - reported_correct,
        }

        # For triage strata: check if suggested NAICS matches reviewer
        if stratum in ("AUTO_RESOLVED", "MANUAL_REVIEW"):
            # suggested_naics is an actual code for these
            suggestion_correct = (group["_reviewer"] == group["_suggested"]).sum()
            row["suggestion_accuracy"] = round(suggestion_correct / n, 4) if n > 0 else ""
            row["n_suggestion_correct"] = suggestion_correct

        # Pipeline flagging performance
        # CLEAN: pipeline said "no problem"; FN = truly misclassified ones
        # FLAGGED*: pipeline said "problem"; FP = truly correct ones
        if stratum == "CLEAN":
            # These were NOT flagged. If reviewer says code is wrong, pipeline missed it (FN)
            row["false_negatives"] = truly_misclassified.sum()
        elif stratum in ("FLAGGED_NOT_PROMOTED", "AUTO_RESOLVED", "MANUAL_REVIEW"):
            # These were flagged. If reviewer says code is actually correct, that's FP
            row["false_positives"] = truly_correct.sum()
            row["true_positives"] = truly_misclassified.sum()

        results.append(row)

    metrics = pd.DataFrame(results)

    # --- Cross-stratum precision/recall ---
    # TP = flagged strata where reviewer confirms misclassification
    # FP = flagged strata where reviewer says code was correct
    # FN = CLEAN stratum where reviewer says code was actually wrong
    tp = sum(r.get("true_positives", 0) for r in results)
    fp = sum(r.get("false_positives", 0) for r in results)
    fn = sum(r.get("false_negatives", 0) for r in results)

    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None

    # Population-weighted reported accuracy
    total_pop = sum(POOL_SIZES.values())
    weighted_acc = 0.0
    for _, row in metrics.iterrows():
        if row.get("n_scored", 0) > 0 and row.get("pool_size", ""):
            weight = float(row["pool_size"]) / total_pop
            weighted_acc += weight * float(row["reported_accuracy"])

    # --- Console output ---
    print("\n" + "=" * 70)
    print("VALIDATION METRICS -- CY 2023")
    print("=" * 70)

    fmt = "{:<25s} {:>6s} {:>8s} {:>12s} {:>6s} {:>6s}"
    print(fmt.format("Stratum", "N", "Pool", "Reported Acc", "FP", "FN"))
    print("-" * 70)
    for _, row in metrics.iterrows():
        print(fmt.format(
            str(row.get("stratum", "")),
            str(row.get("n_scored", "")),
            str(row.get("pool_size", "")),
            str(row.get("reported_accuracy", "")),
            str(row.get("false_positives", "")),
            str(row.get("false_negatives", "")),
        ))
    print("-" * 70)
    print(f"Population-weighted reported accuracy: {weighted_acc:.4f}")
    if precision is not None:
        print(f"Pipeline flagging precision: {precision:.4f}  (TP={tp}, FP={fp})")
    if recall is not None:
        print(f"Pipeline flagging recall:    {recall:.4f}  (TP={tp}, FN={fn})")
    print(f"Total scored: {len(scoreable)}, UNK excluded: {n_unk}")

    # --- Write CSV ---
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(METRICS_PATH, index=False)
    print(f"\nWrote {len(metrics)} rows to {METRICS_PATH}")


if __name__ == "__main__":
    main()
