"""
Step [6]: Generate stratified validation sample from CY 2023 pipeline output.

Produces ~500 records across 5 strata for manual precision/recall annotation.
Output: Analysis/validation_sample_2023.xlsx (one tab per stratum)

Strata:
  CLEAN                  100  (n_flags=0, not in triage)
  FLAGGED_NOT_PROMOTED   100  (n_flags>0, not in triage)
  AUTO_RESOLVED          150  (triage tiers T0-T5d, sub-stratified by tier)
  HUMAN_REVIEW           100  (triage tiers T5e/T5f/T6/T7a)
  NO_DATA                 50  (triage tier T5)

Columns per tab:
  id, establishment_name, company_name, full_address, industry_description,
  reported_naics, suggested_naics, reviewer_naics, reviewer_notes
"""

import random
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
FLAGGED_PATH = BASE_DIR / "flagged_output_2023.csv"
TRIAGE_PATH = BASE_DIR / "triage_report_2023.csv"
OUTPUT_PATH = BASE_DIR / "Analysis" / "validation_sample_2023.xlsx"

SEED = 2026_06

STRATUM_SIZES = {
    "CLEAN": 100,
    "FLAGGED_NOT_PROMOTED": 100,
    "AUTO_RESOLVED": 150,
    "HUMAN_REVIEW": 100,
    "NO_DATA": 50,
}

AUTO_RESOLVED_TIERS = [
    "T0_RESOLVED", "T1_SAME_CODE", "T2_TRIVIAL_CORRECTION",
    "T2b_MULTI_ESTABLISHMENT_CONSISTENT", "T3_DESC_SUPPORTS_REPORTED",
    "T4_SAME_SECTOR", "T5b_NO_DESC_SIGNAL", "T5c_NONCREDIBLE_URL",
    "T5d_LOW_CONFIDENCE_GUESS",
]
HUMAN_REVIEW_TIERS = ["T5e_WEBSITE_UNSCRAPEABLE", "T5f_NO_NAICS_SIGNAL",
                      "T6_CROSS_SECTOR_CONTRADICTION", "T7a_INVALID_CODE"]
NO_DATA_TIER = "T5_NO_ALTERNATIVE"

# Reason text for suggested_naics when no code exists
SUGGESTED_FILL = {
    "CLEAN": "No flags raised",
    "FLAGGED_NOT_PROMOTED": "Flagged but not promoted to triage",
    "T5_NO_ALTERNATIVE": "No web presence found",
    "T5e_WEBSITE_UNSCRAPEABLE": "Website found but unscrapeable",
    "T5f_NO_NAICS_SIGNAL": "Website scraped, no NAICS signal",
}

FLAGGED_LOAD_COLS = [
    "id", "establishment_name", "company_name",
    "street_address", "city", "state", "zip_code",
    "naics_code", "industry_description", "n_flags",
]
TRIAGE_LOAD_COLS = [
    "id", "triage_tier", "triage_reason", "suggested_naics",
]

OUTPUT_COLS = [
    "id", "establishment_name", "company_name", "full_address",
    "industry_description", "reported_naics", "suggested_naics",
    "reviewer_naics", "reviewer_notes",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sample(pool: pd.DataFrame, n: int, rng: random.Random) -> pd.DataFrame:
    if len(pool) <= n:
        return pool.copy()
    indices = rng.sample(range(len(pool)), n)
    return pool.iloc[sorted(indices)].copy()


def _allocate_auto_resolved(tier_counts: dict, total: int, floor: int = 2) -> dict:
    grand = sum(tier_counts.values())
    alloc = {t: floor for t in tier_counts}
    remaining = total - sum(alloc.values())
    props = {t: c / grand for t, c in tier_counts.items()}
    fractional = {}
    for tier, prop in props.items():
        raw = prop * remaining
        alloc[tier] += int(raw)
        fractional[tier] = raw - int(raw)
    leftover = total - sum(alloc.values())
    for tier in sorted(fractional, key=fractional.get, reverse=True):
        if leftover <= 0:
            break
        alloc[tier] += 1
        leftover -= 1
    return alloc


def _int_str(val):
    """Convert numeric value to clean integer string, or empty string."""
    try:
        if pd.isna(val) or str(val).strip() == "":
            return ""
        return str(int(float(val)))
    except (ValueError, TypeError):
        return str(val)


def _build_address(row):
    """Combine street_address, city, state, zip_code into one string."""
    parts = []
    for col in ["street_address", "city", "state", "zip_code"]:
        val = row.get(col, "")
        if pd.notna(val) and str(val).strip():
            parts.append(str(val).strip())
    # Format as "street, city, state zip"
    if len(parts) >= 4:
        return f"{parts[0]}, {parts[1]}, {parts[2]} {parts[3]}"
    return ", ".join(parts)


def _build_suggested(row, stratum):
    """Build suggested_naics: actual code if available, reason text if not."""
    sn = row.get("suggested_naics")
    if pd.notna(sn) and str(sn).strip():
        return _int_str(sn)
    # No suggested code; fill with reason
    tier = row.get("triage_tier", "")
    if stratum in ("CLEAN", "FLAGGED_NOT_PROMOTED"):
        return SUGGESTED_FILL[stratum]
    if pd.notna(tier) and tier in SUGGESTED_FILL:
        return SUGGESTED_FILL[tier]
    return "No suggestion available"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    rng = random.Random(SEED)

    print(f"Loading {FLAGGED_PATH.name} ...")
    flagged = pd.read_csv(FLAGGED_PATH, usecols=FLAGGED_LOAD_COLS, low_memory=False)
    print(f"  {len(flagged):,} rows")

    print(f"Loading {TRIAGE_PATH.name} ...")
    triage = pd.read_csv(TRIAGE_PATH, usecols=TRIAGE_LOAD_COLS, low_memory=False)
    print(f"  {len(triage):,} rows")

    triage_id_set = set(triage["id"])

    # --- Build pools ---
    clean_pool = flagged[flagged["n_flags"] == 0].reset_index(drop=True)
    flagged_not_promoted = flagged[
        (flagged["n_flags"] > 0) & (~flagged["id"].isin(triage_id_set))
    ].reset_index(drop=True)

    tier_groups = triage.groupby("triage_tier")
    auto_resolved_pools = {}
    for tier in AUTO_RESOLVED_TIERS:
        if tier in tier_groups.groups:
            auto_resolved_pools[tier] = tier_groups.get_group(tier).reset_index(drop=True)
        else:
            auto_resolved_pools[tier] = pd.DataFrame(columns=triage.columns)

    human_review_pool = triage[triage["triage_tier"].isin(HUMAN_REVIEW_TIERS)].reset_index(drop=True)
    no_data_pool = triage[triage["triage_tier"] == NO_DATA_TIER].reset_index(drop=True)

    print(f"\nPool sizes:")
    print(f"  CLEAN:                {len(clean_pool):,}")
    print(f"  FLAGGED_NOT_PROMOTED: {len(flagged_not_promoted):,}")
    for tier, pool in auto_resolved_pools.items():
        print(f"  AUTO/{tier}: {len(pool):,}")
    print(f"  HUMAN_REVIEW:         {len(human_review_pool):,}")
    print(f"  NO_DATA:              {len(no_data_pool):,}")

    # --- Draw samples into stratum buckets ---
    stratum_frames = {}

    # 1. CLEAN
    s = _sample(clean_pool, STRATUM_SIZES["CLEAN"], rng)
    s["_stratum"] = "CLEAN"
    stratum_frames["CLEAN"] = s
    print(f"\nSampled CLEAN: {len(s)}")

    # 2. FLAGGED_NOT_PROMOTED
    s = _sample(flagged_not_promoted, STRATUM_SIZES["FLAGGED_NOT_PROMOTED"], rng)
    s["_stratum"] = "FLAGGED_NOT_PROMOTED"
    stratum_frames["FLAGGED_NOT_PROMOTED"] = s
    print(f"Sampled FLAGGED_NOT_PROMOTED: {len(s)}")

    # 3. AUTO_RESOLVED (sub-stratified)
    tier_counts = {t: len(p) for t, p in auto_resolved_pools.items() if len(p) > 0}
    alloc = _allocate_auto_resolved(tier_counts, STRATUM_SIZES["AUTO_RESOLVED"])
    print(f"AUTO_RESOLVED allocation: {alloc}")
    auto_parts = []
    for tier, n in alloc.items():
        part = _sample(auto_resolved_pools[tier], n, rng)
        part["_stratum"] = "AUTO_RESOLVED"
        auto_parts.append(part)
    stratum_frames["AUTO_RESOLVED"] = pd.concat(auto_parts, ignore_index=True)
    print(f"Sampled AUTO_RESOLVED: {len(stratum_frames['AUTO_RESOLVED'])}")

    # 4. HUMAN_REVIEW
    s = _sample(human_review_pool, STRATUM_SIZES["HUMAN_REVIEW"], rng)
    s["_stratum"] = "HUMAN_REVIEW"
    stratum_frames["HUMAN_REVIEW"] = s
    print(f"Sampled HUMAN_REVIEW: {len(s)}")

    # 5. NO_DATA
    s = _sample(no_data_pool, STRATUM_SIZES["NO_DATA"], rng)
    s["_stratum"] = "NO_DATA"
    stratum_frames["NO_DATA"] = s
    print(f"Sampled NO_DATA: {len(s)}")

    # --- Build flagged lookup for address + establishment info ---
    flagged_lookup = flagged.set_index("id")

    # --- Format each stratum and write tabs ---
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    all_ids = []

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        for stratum_name, raw_df in stratum_frames.items():
            rows = []
            for _, row in raw_df.iterrows():
                rec_id = row["id"]

                # Get establishment info from flagged_output (authoritative source)
                if rec_id in flagged_lookup.index:
                    fo = flagged_lookup.loc[rec_id]
                    est_name = fo.get("establishment_name", "")
                    comp_name = fo.get("company_name", "")
                    address = _build_address(fo)
                    ind_desc = fo.get("industry_description", "")
                    reported = _int_str(fo.get("naics_code", ""))
                else:
                    est_name = row.get("establishment_name", "")
                    comp_name = row.get("company_name", "")
                    address = ""
                    ind_desc = row.get("industry_description", "")
                    reported = ""

                suggested = _build_suggested(row, stratum_name)

                rows.append({
                    "id": int(rec_id),
                    "establishment_name": est_name if pd.notna(est_name) else "",
                    "company_name": comp_name if pd.notna(comp_name) else "",
                    "full_address": address,
                    "industry_description": ind_desc if pd.notna(ind_desc) else "",
                    "reported_naics": reported,
                    "suggested_naics": suggested,
                    "reviewer_naics": "",
                    "reviewer_notes": "",
                })

            tab_df = pd.DataFrame(rows, columns=OUTPUT_COLS)
            # Shuffle within tab
            tab_df = tab_df.sample(frac=1, random_state=SEED).reset_index(drop=True)

            sheet_name = stratum_name[:31]  # Excel 31-char limit
            tab_df.to_excel(writer, sheet_name=sheet_name, index=False)

            total_rows += len(tab_df)
            all_ids.extend(tab_df["id"].tolist())
            print(f"  Wrote tab '{sheet_name}': {len(tab_df)} rows")

    print(f"\nWrote {OUTPUT_PATH}")

    # --- Verification ---
    print(f"\n=== Verification ===")
    print(f"Total rows: {total_rows}")
    print(f"Unique IDs: {len(set(all_ids))}")
    if len(set(all_ids)) < total_rows:
        print(f"  WARNING: {total_rows - len(set(all_ids))} duplicate IDs!")
    print(f"Expected: {sum(STRATUM_SIZES.values())}")


if __name__ == "__main__":
    main()
