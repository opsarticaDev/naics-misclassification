"""
Step [10] -- WCIRB workers' compensation premium impact analysis.

Maps NAICS misclassification patterns to California WC premium deltas
using WCIRB class codes and curated rate data. Estimates carrier losses
from systematic underclassification.

Outputs (4 files in Analysis/):
  - wcirb_naics_mapping.csv
  - wcirb_premium_deltas.csv
  - wcirb_egregious_cases.csv
  - wcirb_aggregate_summary.csv
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
ANALYSIS_DIR = BASE_DIR / "Analysis"

# ---------------------------------------------------------------------------
# Curated NAICS-to-WCIRB rate mapping
# Maps 2-digit NAICS sectors to representative WCIRB class codes and rates
# Rates are per $100 of payroll (2025 WCIRB advisory rates)
# ---------------------------------------------------------------------------
SECTOR_WCIRB_MAP = {
    "11": {"wcirb_code": "0005", "rate": 5.91, "desc": "Agriculture/Nurseries"},
    "21": {"wcirb_code": "1124", "rate": 4.72, "desc": "Mining/Oil & Gas"},
    "22": {"wcirb_code": "7539", "rate": 3.14, "desc": "Utilities"},
    "23": {"wcirb_code": "5403", "rate": 11.61, "desc": "Construction/Carpentry"},
    "31": {"wcirb_code": "3110", "rate": 6.31, "desc": "Manufacturing/Forging"},
    "32": {"wcirb_code": "4496", "rate": 5.24, "desc": "Plastics/Chemical Mfg"},
    "33": {"wcirb_code": "3632", "rate": 4.87, "desc": "Metal/Electronics Mfg"},
    "42": {"wcirb_code": "8018", "rate": 4.56, "desc": "Wholesale Trade"},
    "44": {"wcirb_code": "8006", "rate": 3.99, "desc": "Retail/Grocery"},
    "45": {"wcirb_code": "8017", "rate": 2.74, "desc": "Retail/General Merch"},
    "48": {"wcirb_code": "7219", "rate": 7.88, "desc": "Transportation/Trucking"},
    "49": {"wcirb_code": "8292", "rate": 7.18, "desc": "Warehousing"},
    "51": {"wcirb_code": "8859", "rate": 0.42, "desc": "Information/Tech"},
    "52": {"wcirb_code": "8810", "rate": 0.21, "desc": "Finance/Insurance"},
    "53": {"wcirb_code": "8741", "rate": 0.54, "desc": "Real Estate"},
    "54": {"wcirb_code": "8810", "rate": 0.21, "desc": "Professional Services"},
    "55": {"wcirb_code": "8810", "rate": 0.21, "desc": "Management of Companies"},
    "56": {"wcirb_code": "9014", "rate": 3.82, "desc": "Admin/Waste Services"},
    "61": {"wcirb_code": "8868", "rate": 0.64, "desc": "Education"},
    "62": {"wcirb_code": "8834", "rate": 0.94, "desc": "Healthcare/Physicians"},
    "71": {"wcirb_code": "9154", "rate": 2.89, "desc": "Arts/Entertainment"},
    "72": {"wcirb_code": "9079", "rate": 2.47, "desc": "Accommodation/Food"},
    "81": {"wcirb_code": "9586", "rate": 1.73, "desc": "Other Services"},
    "92": {"wcirb_code": "8810", "rate": 0.36, "desc": "Public Administration"},
}

# Average CA salary for premium estimation (BLS 2023 CA mean annual wage)
CA_AVG_SALARY = 74_580


# ===== Stage 1: Parse WCIRB Class Codes ====================================

def load_wcirb_codes():
    """Load and clean WCIRB class code list from xlsx."""
    path = BASE_DIR / "PassAlong" / "WCIRB_ClassCodes (1).xlsx"
    df = pd.read_excel(path, sheet_name="2025 Valid Class Codes", skiprows=2)
    df.columns = ["seq_no", "class_code", "wording"]
    df = df.dropna(subset=["class_code"])
    df["class_code"] = df["class_code"].astype(str).str.strip()
    df["wording"] = df["wording"].astype(str).str.strip()
    return df


def stage1_naics_mapping(wcirb_codes):
    """Write the sector-to-WCIRB mapping CSV and report code counts."""
    print("=" * 70)
    print("STAGE 1: NAICS Sector to WCIRB Mapping")
    print("=" * 70)

    n_wcirb = len(wcirb_codes)
    print(f"  Parsed {n_wcirb} WCIRB class codes from xlsx")

    rows = []
    for sector, info in sorted(SECTOR_WCIRB_MAP.items()):
        rows.append({
            "naics_sector": sector,
            "sector_name": info["desc"],
            "wcirb_code": info["wcirb_code"],
            "wcirb_rate": info["rate"],
            "wcirb_desc": info["desc"],
        })
    mapping_df = pd.DataFrame(rows)

    out_path = ANALYSIS_DIR / "wcirb_naics_mapping.csv"
    mapping_df.to_csv(out_path, index=False)
    print(f"  Wrote {len(mapping_df)} sector mappings -> {out_path.name}")
    print(f"  Rate range: ${mapping_df['wcirb_rate'].min():.2f} - "
          f"${mapping_df['wcirb_rate'].max():.2f} per $100 payroll")
    print()
    return mapping_df


# ===== Stage 2: Load Misclassification Patterns ===========================

def load_triage():
    """Load the triage report."""
    path = BASE_DIR / "triage_report_2023.csv"
    print(f"  Loading {path.name} ...")
    df = pd.read_csv(path, dtype=str)
    print(f"    {len(df):,} triage rows")
    return df


def load_ita():
    """Load ITA data (selected columns only)."""
    path = BASE_DIR / "ita_data_2023.csv"
    print(f"  Loading {path.name} ...")
    cols = ["id", "ein", "annual_average_employees", "establishment_id", "state"]
    df = pd.read_csv(path, dtype=str, usecols=cols)
    print(f"    {len(df):,} ITA rows")
    return df


def stage2_premium_deltas(triage_df, ita_df):
    """Compute premium deltas for cross-sector misclassifications."""
    print("=" * 70)
    print("STAGE 2: Premium Delta Analysis")
    print("=" * 70)

    # Merge triage with ITA for employee counts
    merged = triage_df.merge(
        ita_df[["id", "ein", "annual_average_employees", "state"]],
        on="id",
        how="left",
        suffixes=("", "_ita"),
    )
    # Use _ita state if triage state is missing (they should match, but be safe)
    if "state_ita" in merged.columns:
        merged["state"] = merged["state"].fillna(merged["state_ita"])
        merged.drop(columns=["state_ita"], inplace=True, errors="ignore")

    print(f"  Merged: {len(merged):,} rows")

    # Filter to rows with a valid suggested NAICS (5-6 digit numeric)
    def is_valid_naics(s):
        if pd.isna(s):
            return False
        s = str(s).strip()
        return s.isdigit() and len(s) in (5, 6)

    mask_valid = merged["suggested_naics"].apply(is_valid_naics)
    mask_diff = merged["suggested_naics"].astype(str).str.strip() != merged["naics_code"].astype(str).str.strip()
    scored = merged[mask_valid & mask_diff].copy()
    print(f"  Misclassifications with valid suggestion: {len(scored):,}")

    # Derive sectors
    scored["reported_sector"] = scored["naics_code"].astype(str).str[:2]
    scored["suggested_sector"] = scored["suggested_naics"].astype(str).str[:2]
    scored["cross_sector"] = scored["reported_sector"] != scored["suggested_sector"]

    n_cross = scored["cross_sector"].sum()
    n_within = (~scored["cross_sector"]).sum()
    print(f"  Cross-sector: {n_cross:,}  |  Within-sector: {n_within:,}")

    # Look up rates
    scored["reported_rate"] = scored["reported_sector"].map(
        lambda s: SECTOR_WCIRB_MAP.get(s, {}).get("rate", np.nan)
    )
    scored["suggested_rate"] = scored["suggested_sector"].map(
        lambda s: SECTOR_WCIRB_MAP.get(s, {}).get("rate", np.nan)
    )
    scored["reported_desc"] = scored["reported_sector"].map(
        lambda s: SECTOR_WCIRB_MAP.get(s, {}).get("desc", "Unknown")
    )
    scored["suggested_desc"] = scored["suggested_sector"].map(
        lambda s: SECTOR_WCIRB_MAP.get(s, {}).get("desc", "Unknown")
    )

    # Rate delta
    scored["rate_delta"] = scored["suggested_rate"] - scored["reported_rate"]

    def direction(d):
        if pd.isna(d) or d == 0:
            return "SAME"
        return "UNDERPAYING" if d > 0 else "OVERPAYING"

    scored["direction"] = scored["rate_delta"].apply(direction)

    # Employee count and annual loss
    # Cap at 50,000 per establishment. OSHA source data contains erroneous
    # values (e.g., 52M, 172M) that are clearly data-entry errors.
    EMP_CAP = 50_000
    scored["employees"] = pd.to_numeric(
        scored["annual_average_employees"], errors="coerce"
    ).fillna(1).clip(upper=EMP_CAP)
    n_capped = (pd.to_numeric(scored["annual_average_employees"], errors="coerce") > EMP_CAP).sum()
    if n_capped > 0:
        print(f"  NOTE: {n_capped} establishments capped at {EMP_CAP:,} employees (source data errors)")
    scored["estimated_annual_loss"] = (
        scored["rate_delta"].abs() * scored["employees"] * CA_AVG_SALARY / 100
    )

    # --- Group by sector pair ---
    grouped = (
        scored.groupby(["reported_sector", "suggested_sector"])
        .agg(
            reported_desc=("reported_desc", "first"),
            suggested_desc=("suggested_desc", "first"),
            reported_rate=("reported_rate", "first"),
            suggested_rate=("suggested_rate", "first"),
            rate_delta=("rate_delta", "first"),
            direction=("direction", "first"),
            n_establishments=("id", "count"),
            total_employees=("employees", "sum"),
            total_annual_loss=("estimated_annual_loss", "sum"),
        )
        .reset_index()
    )
    grouped["avg_loss_per_establishment"] = (
        grouped["total_annual_loss"] / grouped["n_establishments"]
    )
    grouped = grouped.sort_values("total_annual_loss", ascending=False).reset_index(drop=True)

    out_path = ANALYSIS_DIR / "wcirb_premium_deltas.csv"
    grouped.to_csv(out_path, index=False, float_format="%.2f")
    print(f"  Wrote {len(grouped)} sector-pair rows -> {out_path.name}")
    print()

    return scored, grouped


# ===== Stage 3: Egregious Individual Cases =================================

def stage3_egregious_cases(scored):
    """Output the top 50 individual establishments by premium impact."""
    print("=" * 70)
    print("STAGE 3: Egregious Individual Cases (Top 50)")
    print("=" * 70)

    cols = [
        "id", "ein", "establishment_name", "company_name", "state",
        "naics_code", "suggested_naics",
        "reported_sector", "suggested_sector",
        "reported_rate", "suggested_rate", "rate_delta",
        "employees", "estimated_annual_loss",
    ]
    # Keep only columns that exist (ein may come from merge)
    avail = [c for c in cols if c in scored.columns]
    top50 = (
        scored[avail]
        .sort_values("estimated_annual_loss", ascending=False)
        .head(50)
        .reset_index(drop=True)
    )

    # Rename for output clarity
    rename_map = {"naics_code": "reported_naics"}
    top50 = top50.rename(columns=rename_map)

    out_path = ANALYSIS_DIR / "wcirb_egregious_cases.csv"
    top50.to_csv(out_path, index=False, float_format="%.2f")
    print(f"  Wrote {len(top50)} rows -> {out_path.name}")

    if len(top50) > 0:
        print(f"  Largest single impact: ${top50['estimated_annual_loss'].iloc[0]:,.0f}")
        print(f"  #1: {top50['establishment_name'].iloc[0]} "
              f"({top50['reported_naics'].iloc[0]} -> {top50['suggested_naics'].iloc[0]})")
    print()
    return top50


# ===== Stage 4: Aggregate Summary ==========================================

def stage4_aggregate_summary(scored, grouped, total_ita_rows):
    """Compute and write the aggregate summary metrics."""
    print("=" * 70)
    print("STAGE 4: Aggregate Summary")
    print("=" * 70)

    total_misclass = len(scored)
    cross_sector = int(scored["cross_sector"].sum())
    within_sector = total_misclass - cross_sector

    # CA establishments
    ca_mask = scored["state"].str.upper().isin(["CA", "CALIFORNIA"])
    ca_count = int(ca_mask.sum())
    ca_loss = scored.loc[ca_mask, "estimated_annual_loss"].sum()

    total_loss = scored["estimated_annual_loss"].sum()
    avg_loss = scored["estimated_annual_loss"].mean() if total_misclass > 0 else 0
    max_loss = scored["estimated_annual_loss"].max() if total_misclass > 0 else 0

    # Top underclass pattern (highest total loss among UNDERPAYING)
    underpay = grouped[grouped["direction"] == "UNDERPAYING"]
    if len(underpay) > 0:
        top_row = underpay.iloc[0]
        top_pattern = (f"{top_row['reported_sector']} ({top_row['reported_desc']}) -> "
                       f"{top_row['suggested_sector']} ({top_row['suggested_desc']})")
    else:
        top_pattern = "N/A"

    # Pct underpaying among cross-sector
    cross_df = scored[scored["cross_sector"]]
    if len(cross_df) > 0:
        pct_underpay = (cross_df["rate_delta"] > 0).sum() / len(cross_df) * 100
    else:
        pct_underpay = 0

    # Extrapolation (rough)
    population_ratio = total_ita_rows / total_misclass if total_misclass > 0 else 1
    aggressive_ca = ca_loss * population_ratio

    metrics = {
        "total_misclassifications_with_suggestion": total_misclass,
        "cross_sector_misclassifications": cross_sector,
        "within_sector_misclassifications": within_sector,
        "ca_establishments_affected": ca_count,
        "total_annual_premium_gap_all_states": f"{total_loss:.2f}",
        "total_annual_premium_gap_ca_only": f"{ca_loss:.2f}",
        "avg_premium_gap_per_establishment": f"{avg_loss:.2f}",
        "max_individual_premium_gap": f"{max_loss:.2f}",
        "top_underclass_pattern": top_pattern,
        "pct_underpaying": f"{pct_underpay:.1f}",
        "conservative_ca_carrier_loss": f"{ca_loss:.2f}",
        "aggressive_ca_carrier_loss": (
            f"{aggressive_ca:.2f} (CAUTION: rough extrapolation, "
            f"population_ratio={population_ratio:.1f}, "
            f"based on {total_ita_rows:,} total ITA records / "
            f"{total_misclass:,} scored establishments)"
        ),
    }

    summary_df = pd.DataFrame(
        [{"metric": k, "value": v} for k, v in metrics.items()]
    )
    out_path = ANALYSIS_DIR / "wcirb_aggregate_summary.csv"
    summary_df.to_csv(out_path, index=False)
    print(f"  Wrote {len(summary_df)} metrics -> {out_path.name}")
    print()

    # Pretty-print summary
    print("-" * 60)
    for k, v in metrics.items():
        label = k.replace("_", " ").title()
        print(f"  {label:45s}  {v}")
    print("-" * 60)
    print()

    return summary_df


# ===== Console: Top 10 Cross-Sector Patterns ==============================

def print_top_patterns(grouped):
    """Print top 10 cross-sector premium impact patterns."""
    print("=" * 70)
    print("TOP 10 CROSS-SECTOR PATTERNS BY PREMIUM IMPACT")
    print("=" * 70)

    # Only cross-sector
    cross = grouped[grouped["reported_sector"] != grouped["suggested_sector"]].head(10)

    if len(cross) == 0:
        print("  (no cross-sector patterns found)")
        return

    print(f"  {'Reported':>12s} -> {'Suggested':<12s}  {'Dir':>10s}  "
          f"{'N':>6s}  {'Total Loss':>14s}  {'Avg Loss':>12s}")
    print(f"  {'-'*12}    {'-'*12}  {'-'*10}  {'-'*6}  {'-'*14}  {'-'*12}")

    for _, r in cross.iterrows():
        print(f"  {r['reported_desc']:>12s} -> {r['suggested_desc']:<12s}  "
              f"{r['direction']:>10s}  "
              f"{int(r['n_establishments']):>6,d}  "
              f"${r['total_annual_loss']:>13,.0f}  "
              f"${r['avg_loss_per_establishment']:>11,.0f}")
    print()


# ===== Main ================================================================

def main():
    print()
    print("WCIRB Workers' Compensation Premium Impact Analysis")
    print("NAICS Misclassification -> CA WC Premium Deltas")
    print(f"Dataset: CY 2023  |  CA Avg Salary: ${CA_AVG_SALARY:,}")
    print()

    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    # Stage 1
    wcirb_codes = load_wcirb_codes()
    stage1_naics_mapping(wcirb_codes)

    # Stage 2
    triage_df = load_triage()
    ita_df = load_ita()
    total_ita_rows = len(ita_df)
    scored, grouped = stage2_premium_deltas(triage_df, ita_df)

    # Stage 3
    stage3_egregious_cases(scored)

    # Stage 4
    stage4_aggregate_summary(scored, grouped, total_ita_rows)

    # Console summary
    print_top_patterns(grouped)

    print("Done. All outputs written to Analysis/")


if __name__ == "__main__":
    main()
