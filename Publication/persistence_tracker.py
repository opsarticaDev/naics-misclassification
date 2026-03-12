"""
Step [9]: Cross-year persistence analysis.

Tracks EINs and establishments filing invalid/placeholder NAICS codes
across multiple years (CY 2021-2024). Identifies willful vs. accidental
misreporting patterns.

Outputs (4 files in Analysis/):
  - persistence_999999_reporters.csv
  - persistence_yoy_shifts.csv
  - persistence_chronic_downshifters.csv
  - persistence_summary.csv
"""

import sys
from pathlib import Path

import pandas as pd
import openpyxl

BASE_DIR = Path(__file__).resolve().parent.parent
ANALYSIS_DIR = BASE_DIR / "Analysis"
YEARS = ["2021", "2022", "2023", "2024"]

# Sectors grouped by injury-risk profile
HIGH_RISK_SECTORS = {"23", "31", "32", "33", "48", "56"}
MEDIUM_RISK_SECTORS = {"42", "49", "81"}
LOW_RISK_SECTORS = {"44", "45", "62", "72"}

ITA_COLS = [
    "id", "ein", "establishment_id", "naics_code",
    "establishment_name", "company_name", "state",
    "annual_average_employees",
]
FLAGGED_COLS = ["id", "flag_invalid_naics_code"]
TRIAGE_COLS = ["id", "triage_tier", "suggested_naics"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_dart_rates(year):
    """Load BLS DART rates from the reference Excel file for a given year."""
    filepath = BASE_DIR / "Reference" / f"bls_dart_rates_{year}.xlsx"
    if not filepath.exists():
        return {}
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    rates = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        raw_code = str(row[1]).strip().replace("\xa0", "") if row[1] else ""
        raw_rate = str(row[3]).strip() if row[3] else ""
        if not raw_code or not raw_rate or raw_rate == "-":
            continue
        try:
            rate_val = float(raw_rate)
        except ValueError:
            continue
        if "-" in raw_code:
            parts = raw_code.split("-")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                for code_num in range(int(parts[0]), int(parts[1]) + 1):
                    rates[str(code_num)] = rate_val
                continue
        if not raw_code.isdigit():
            continue
        rates[raw_code] = rate_val
    wb.close()
    return rates


def lookup_dart(naics_code, dart_rates):
    """Look up DART rate for a NAICS code, falling back to shorter prefixes."""
    if not naics_code or not isinstance(naics_code, str) or not naics_code.isdigit():
        return None
    # Try 6, 4, 3, 2 digit lookups
    for length in [6, 4, 3, 2]:
        prefix = naics_code[:length]
        if prefix in dart_rates:
            return dart_rates[prefix]
    return None


def risk_tier_for_sector(sector):
    """Return risk tier string for a 2-digit NAICS sector code."""
    if sector in HIGH_RISK_SECTORS:
        return "HIGH"
    if sector in MEDIUM_RISK_SECTORS:
        return "MEDIUM"
    if sector in LOW_RISK_SECTORS:
        return "LOW"
    return "OTHER"


def safe_first(series):
    """Return first non-null value from a series, or empty string."""
    vals = series.dropna()
    if len(vals) > 0:
        return vals.iloc[0]
    return ""


# ---------------------------------------------------------------------------
# Stage 1: Per-Year Data Loading
# ---------------------------------------------------------------------------

def load_year(year):
    """Load and join ITA, flagged, and triage data for one year."""
    encoding = "utf-8-sig" if year == "2022" else "utf-8"

    pipeline_dir = BASE_DIR / "pipeline_output"
    ita_path = pipeline_dir / f"ita_data_{year}.csv"
    flagged_path = pipeline_dir / f"flagged_output_{year}.csv"
    triage_path = pipeline_dir / f"triage_report_{year}.csv"

    print(f"  Loading ita_data_{year}.csv ...", end=" ")
    ita = pd.read_csv(ita_path, encoding=encoding, dtype=str, usecols=ITA_COLS)
    print(f"{len(ita):,} rows")

    print(f"  Loading flagged_output_{year}.csv ...", end=" ")
    flagged = pd.read_csv(flagged_path, dtype=str, usecols=FLAGGED_COLS)
    print(f"{len(flagged):,} rows")

    print(f"  Loading triage_report_{year}.csv ...", end=" ")
    triage = pd.read_csv(triage_path, dtype=str, usecols=TRIAGE_COLS)
    print(f"{len(triage):,} rows")

    # Deduplicate ITA on id before joining (CY 2021 has 2,041 duplicate IDs
    # that create cartesian products during merge, inflating row count)
    ita = ita.drop_duplicates(subset=["id"], keep="first")

    # Join on id
    df = ita.merge(flagged, on="id", how="left")
    df = df.merge(triage, on="id", how="left")

    # Classify NAICS errors
    df["error_type"] = "VALID"

    mask_999999 = df["naics_code"] == "999999"
    mask_000000 = df["naics_code"] == "000000"
    mask_invalid = (df["flag_invalid_naics_code"] == "1") & ~mask_999999 & ~mask_000000

    # Incomplete: numeric but fewer than 6 digits (and not already caught)
    def is_incomplete(code):
        if pd.isna(code) or not isinstance(code, str):
            return False
        stripped = code.strip()
        return stripped.isdigit() and len(stripped) < 6

    mask_incomplete = df["naics_code"].apply(is_incomplete) & ~mask_999999 & ~mask_000000 & ~mask_invalid

    df.loc[mask_999999, "error_type"] = "PLACEHOLDER_999999"
    df.loc[mask_000000, "error_type"] = "PLACEHOLDER_000000"
    df.loc[mask_invalid, "error_type"] = "INVALID_CODE"
    df.loc[mask_incomplete, "error_type"] = "INCOMPLETE_CODE"

    df["year"] = year

    return df


def stage1_load_all():
    """Load and concatenate all years."""
    print("=" * 60)
    print("STAGE 1: Loading per-year data")
    print("=" * 60)
    frames = []
    for year in YEARS:
        print(f"\n--- CY {year} ---")
        df = load_year(year)
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    print(f"\nCombined dataset: {len(combined):,} rows across {len(YEARS)} years")

    # Quick error-type summary
    counts = combined["error_type"].value_counts()
    for etype, cnt in counts.items():
        print(f"  {etype}: {cnt:,}")

    return combined


# ---------------------------------------------------------------------------
# Stage 2: 999999 Persistence Detection
# ---------------------------------------------------------------------------

def stage2_999999_persistence(combined):
    """Identify EINs filing 999999 across multiple years."""
    print("\n" + "=" * 60)
    print("STAGE 2: 999999 multi-year persistence")
    print("=" * 60)

    mask = combined["error_type"] == "PLACEHOLDER_999999"
    nines = combined[mask].copy()
    print(f"Total 999999 records: {len(nines):,}")

    if nines.empty:
        print("  No 999999 records found.")
        return pd.DataFrame()

    # EIN-level aggregation
    def agg_ein(g):
        years_list = sorted(g["year"].unique())
        years_with_999999 = len(years_list)
        # Count all years this EIN appears in (any code)
        total_establishments = len(g)

        # Establishments per year
        per_year = g.groupby("year").size()
        est_per_year = "; ".join(f"{y}:{c}" for y, c in per_year.items())

        # Company names (deduplicated)
        names = g["company_name"].dropna().unique()
        company_name = "; ".join(sorted(set(n.strip() for n in names if n.strip())))[:200]
        if not company_name:
            names_est = g["establishment_name"].dropna().unique()
            company_name = "; ".join(sorted(set(n.strip() for n in names_est if n.strip())))[:200]

        states = sorted(g["state"].dropna().unique())

        return pd.Series({
            "company_name": company_name,
            "years_with_999999": years_with_999999,
            "years_list": ", ".join(years_list),
            "total_establishments": total_establishments,
            "establishments_per_year": est_per_year,
            "states": ", ".join(states),
        })

    ein_agg = nines.groupby("ein").apply(agg_ein, include_groups=False).reset_index()

    # Count total years each EIN is present (any NAICS, not just 999999)
    ein_years_total = combined.groupby("ein")["year"].nunique().reset_index()
    ein_years_total.columns = ["ein", "years_present_total"]
    ein_agg = ein_agg.merge(ein_years_total, on="ein", how="left")

    # Filter to multi-year 999999 reporters
    multi = ein_agg[ein_agg["years_with_999999"] >= 2].copy()
    multi = multi.sort_values(
        ["years_with_999999", "total_establishments"],
        ascending=[False, False],
    ).reset_index(drop=True)

    col_order = [
        "ein", "company_name", "years_with_999999", "years_present_total",
        "years_list", "total_establishments", "establishments_per_year", "states",
    ]
    multi = multi[col_order]

    print(f"EINs with 999999 in 2+ years: {len(multi):,}")
    if not multi.empty:
        print(f"  Top EIN: {multi.iloc[0]['ein']} - {multi.iloc[0]['company_name']}"
              f" ({multi.iloc[0]['years_with_999999']} years, "
              f"{multi.iloc[0]['total_establishments']} establishments)")

    out_path = ANALYSIS_DIR / "persistence_999999_reporters.csv"
    multi.to_csv(out_path, index=False)
    print(f"Wrote {out_path.name} ({len(multi):,} rows)")

    return multi


# ---------------------------------------------------------------------------
# Stage 3: Year-over-Year NAICS Shift Detection
# ---------------------------------------------------------------------------

def stage3_yoy_shifts(combined):
    """Detect NAICS code changes between consecutive years."""
    print("\n" + "=" * 60)
    print("STAGE 3: Year-over-year NAICS shift detection")
    print("=" * 60)

    # Load DART rates for all years
    print("Loading BLS DART rates ...")
    dart_by_year = {}
    for year in YEARS:
        rates = load_dart_rates(year)
        dart_by_year[year] = rates
        print(f"  {year}: {len(rates)} rate entries")

    # For each establishment_id + year, get the NAICS code used
    # Use first occurrence if duplicates exist
    est_year = (
        combined
        .dropna(subset=["establishment_id"])
        .groupby(["establishment_id", "year"])
        .agg(
            naics_code=("naics_code", "first"),
            ein=("ein", "first"),
            company_name=("company_name", lambda s: safe_first(s)),
            establishment_name=("establishment_name", lambda s: safe_first(s)),
            state=("state", "first"),
            annual_average_employees=("annual_average_employees", "first"),
        )
        .reset_index()
    )

    # Find establishments present in 2+ years
    est_year_counts = est_year.groupby("establishment_id")["year"].nunique()
    multi_year_eids = set(est_year_counts[est_year_counts >= 2].index)
    print(f"Establishments in 2+ years: {len(multi_year_eids):,}")

    multi = est_year[est_year["establishment_id"].isin(multi_year_eids)].copy()
    multi = multi.sort_values(["establishment_id", "year"])

    # Build consecutive-year pairs
    shifts = []
    for eid, grp in multi.groupby("establishment_id"):
        grp = grp.sort_values("year")
        rows = grp.to_dict("records")
        for i in range(len(rows) - 1):
            r_from = rows[i]
            r_to = rows[i + 1]

            naics_from = str(r_from["naics_code"]).strip() if pd.notna(r_from["naics_code"]) else ""
            naics_to = str(r_to["naics_code"]).strip() if pd.notna(r_to["naics_code"]) else ""

            sector_from = naics_from[:2] if len(naics_from) >= 2 else ""
            sector_to = naics_to[:2] if len(naics_to) >= 2 else ""

            # Shift type
            if naics_from == naics_to:
                shift_type = "STABLE"
            elif sector_from != sector_to:
                shift_type = "CROSS_SECTOR_SHIFT"
            else:
                shift_type = "WITHIN_SECTOR_SHIFT"

            # DART rates
            year_from = r_from["year"]
            year_to = r_to["year"]
            dart_from = lookup_dart(naics_from, dart_by_year.get(year_from, {}))
            dart_to = lookup_dart(naics_to, dart_by_year.get(year_to, {}))

            dart_delta = None
            direction = "UNKNOWN"
            if dart_from is not None and dart_to is not None:
                dart_delta = round(dart_to - dart_from, 2)
                if dart_from > dart_to:
                    direction = "DOWNWARD_SHIFT"
                elif dart_to > dart_from:
                    direction = "UPWARD_SHIFT"
                else:
                    direction = "LATERAL"

            # Risk tier from destination sector
            risk_tier = risk_tier_for_sector(sector_to) if sector_to else "UNKNOWN"

            # Employee count (use the 'to' year)
            emp_raw = r_to["annual_average_employees"]
            employees = pd.to_numeric(emp_raw, errors="coerce") if pd.notna(emp_raw) else None

            # Company name: prefer company_name, fall back to establishment_name
            cname = r_to["company_name"]
            if not cname or (isinstance(cname, str) and not cname.strip()):
                cname = r_to["establishment_name"]

            shifts.append({
                "establishment_id": eid,
                "ein": r_to["ein"],
                "company_name": cname,
                "state": r_to["state"],
                "year_from": year_from,
                "year_to": year_to,
                "naics_from": naics_from,
                "naics_to": naics_to,
                "sector_from": sector_from,
                "sector_to": sector_to,
                "shift_type": shift_type,
                "dart_rate_from": dart_from,
                "dart_rate_to": dart_to,
                "dart_delta": dart_delta,
                "direction": direction,
                "risk_tier": risk_tier,
                "employees": employees,
            })

    shifts_df = pd.DataFrame(shifts)
    print(f"Total year-over-year pairs: {len(shifts_df):,}")

    if not shifts_df.empty:
        type_counts = shifts_df["shift_type"].value_counts()
        for st, cnt in type_counts.items():
            print(f"  {st}: {cnt:,}")

        dir_counts = shifts_df[shifts_df["direction"] != "UNKNOWN"]["direction"].value_counts()
        for d, cnt in dir_counts.items():
            print(f"  Direction {d}: {cnt:,}")

    out_path = ANALYSIS_DIR / "persistence_yoy_shifts.csv"
    shifts_df.to_csv(out_path, index=False)
    print(f"Wrote {out_path.name} ({len(shifts_df):,} rows)")

    return shifts_df


# ---------------------------------------------------------------------------
# Stage 4: Chronic Downshifters
# ---------------------------------------------------------------------------

def stage4_chronic_downshifters(shifts_df):
    """Identify EINs with 2+ downward DART shifts."""
    print("\n" + "=" * 60)
    print("STAGE 4: Chronic downshifters")
    print("=" * 60)

    if shifts_df.empty:
        print("  No shifts data: skipping.")
        empty = pd.DataFrame(columns=[
            "ein", "company_name", "n_downshifts", "n_establishments_affected",
            "total_employees_affected", "avg_dart_delta", "worst_dart_delta",
            "years_active", "states",
        ])
        out_path = ANALYSIS_DIR / "persistence_chronic_downshifters.csv"
        empty.to_csv(out_path, index=False)
        return empty

    # Only count actual NAICS changes (not STABLE codes where BLS changed rates)
    actual_changes = shifts_df["shift_type"].isin(["CROSS_SECTOR_SHIFT", "WITHIN_SECTOR_SHIFT"])
    down = shifts_df[actual_changes & (shifts_df["direction"] == "DOWNWARD_SHIFT")].copy()
    print(f"Downward shifts (actual NAICS changes only): {len(down):,}")

    if down.empty:
        print("  No downward shifts found.")
        empty = pd.DataFrame(columns=[
            "ein", "company_name", "n_downshifts", "n_establishments_affected",
            "total_employees_affected", "avg_dart_delta", "worst_dart_delta",
            "years_active", "states",
        ])
        out_path = ANALYSIS_DIR / "persistence_chronic_downshifters.csv"
        empty.to_csv(out_path, index=False)
        return empty

    def agg_down(g):
        n_down = len(g)
        n_est = g["establishment_id"].nunique()
        total_emp = g["employees"].dropna().sum()

        deltas = g["dart_delta"].dropna()
        avg_delta = round(deltas.mean(), 2) if len(deltas) > 0 else None
        worst_delta = round(deltas.min(), 2) if len(deltas) > 0 else None

        all_years = sorted(set(g["year_from"].tolist() + g["year_to"].tolist()))
        states = sorted(g["state"].dropna().unique())

        names = g["company_name"].dropna().unique()
        company_name = "; ".join(sorted(set(n.strip() for n in names if isinstance(n, str) and n.strip())))[:200]

        return pd.Series({
            "company_name": company_name,
            "n_downshifts": n_down,
            "n_establishments_affected": n_est,
            "total_employees_affected": total_emp,
            "avg_dart_delta": avg_delta,
            "worst_dart_delta": worst_delta,
            "years_active": ", ".join(all_years),
            "states": ", ".join(states),
        })

    ein_down = down.groupby("ein").apply(agg_down, include_groups=False).reset_index()
    chronic = ein_down[ein_down["n_downshifts"] >= 2].copy()
    chronic = chronic.sort_values(
        ["n_downshifts", "n_establishments_affected"],
        ascending=[False, False],
    ).reset_index(drop=True)

    col_order = [
        "ein", "company_name", "n_downshifts", "n_establishments_affected",
        "total_employees_affected", "avg_dart_delta", "worst_dart_delta",
        "years_active", "states",
    ]
    chronic = chronic[col_order]

    print(f"Chronic downshifter EINs (2+ downward shifts): {len(chronic):,}")
    if not chronic.empty:
        print(f"  Top: {chronic.iloc[0]['ein']} - {chronic.iloc[0]['company_name']}"
              f" ({chronic.iloc[0]['n_downshifts']} shifts)")

    out_path = ANALYSIS_DIR / "persistence_chronic_downshifters.csv"
    chronic.to_csv(out_path, index=False)
    print(f"Wrote {out_path.name} ({len(chronic):,} rows)")

    return chronic


# ---------------------------------------------------------------------------
# Stage 5: Summary Statistics
# ---------------------------------------------------------------------------

def stage5_summary(combined, multi_999, shifts_df, chronic_df):
    """Compute and write high-level summary metrics."""
    print("\n" + "=" * 60)
    print("STAGE 5: Summary statistics")
    print("=" * 60)

    total_records = len(combined)
    total_999999 = (combined["error_type"] == "PLACEHOLDER_999999").sum()
    eins_999999_multi = len(multi_999) if multi_999 is not None else 0

    # Unique establishment_ids from multi-year 999999 EINs
    if multi_999 is not None and not multi_999.empty:
        multi_eins = set(multi_999["ein"])
        mask = (combined["ein"].isin(multi_eins)) & (combined["error_type"] == "PLACEHOLDER_999999")
        est_999_multi = combined.loc[mask, "establishment_id"].nunique()
    else:
        est_999_multi = 0

    # Shift counts
    if shifts_df is not None and not shifts_df.empty:
        total_cross_sector = (shifts_df["shift_type"] == "CROSS_SECTOR_SHIFT").sum()
        # Only count actual NAICS changes as downward shifts (not BLS rate changes on stable codes)
        actual_changes = shifts_df["shift_type"].isin(["CROSS_SECTOR_SHIFT", "WITHIN_SECTOR_SHIFT"])
        total_downward = (actual_changes & (shifts_df["direction"] == "DOWNWARD_SHIFT")).sum()
    else:
        total_cross_sector = 0
        total_downward = 0

    chronic_eins = len(chronic_df) if chronic_df is not None else 0

    # Establishment presence across years
    est_years = combined.dropna(subset=["establishment_id"]).groupby("establishment_id")["year"].nunique()
    est_all_4 = (est_years == 4).sum()
    est_2plus = (est_years >= 2).sum()

    metrics = {
        "total_records_4yr": total_records,
        "total_999999_all_years": total_999999,
        "eins_999999_multi_year": eins_999999_multi,
        "establishments_999999_multi_year": est_999_multi,
        "total_cross_sector_shifts": total_cross_sector,
        "total_downward_shifts": total_downward,
        "chronic_downshifter_eins": chronic_eins,
        "establishments_in_all_4_years": est_all_4,
        "establishments_in_2plus_years": est_2plus,
    }

    summary_df = pd.DataFrame(
        [{"metric": k, "value": v} for k, v in metrics.items()]
    )

    out_path = ANALYSIS_DIR / "persistence_summary.csv"
    summary_df.to_csv(out_path, index=False)
    print(f"Wrote {out_path.name}")

    print("\n--- Summary ---")
    for k, v in metrics.items():
        print(f"  {k}: {v:,}")

    return summary_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("PERSISTENCE TRACKER: Cross-Year NAICS Analysis")
    print(f"Years: {', '.join(YEARS)}")
    print(f"Base directory: {BASE_DIR}")
    print("=" * 60)

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    # Stage 1
    combined = stage1_load_all()

    # Stage 2
    multi_999 = stage2_999999_persistence(combined)

    # Stage 3
    shifts_df = stage3_yoy_shifts(combined)

    # Stage 4
    chronic_df = stage4_chronic_downshifters(shifts_df)

    # Stage 5
    stage5_summary(combined, multi_999, shifts_df, chronic_df)

    print("\n" + "=" * 60)
    print("PERSISTENCE TRACKER COMPLETE")
    print(f"Outputs in: {ANALYSIS_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
