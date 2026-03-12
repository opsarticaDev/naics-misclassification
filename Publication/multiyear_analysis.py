"""
Multi-Year Comparative Analysis
================================
Produces publication-ready tables and figure data across CY 2021-2024.
Outputs go to Analysis/ (CSVs) and Handoff Docs/ (summary narrative).

This is a PUBLICATION script: it reads pipeline outputs but does not
modify them.  The parsing pipeline lives in Scripts/ (gates 1-17).

Usage:  python Publication/multiyear_analysis.py
"""

import csv
import os
import sys
from collections import Counter, defaultdict
from datetime import date

# Reach into Scripts/ for shared config (BASE_DIR, etc.)
_SCRIPT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Scripts")
sys.path.insert(0, _SCRIPT_DIR)
from util_pipeline_config import BASE_DIR

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
YEARS = ["2021", "2022", "2023", "2024"]

ANALYSIS_DIR = os.path.join(BASE_DIR, "Analysis")
HANDOFF_DIR = os.path.join(BASE_DIR, "Handoff Docs")

FLAG_COLUMNS = [
    "flag_invalid_naics_code",
    "flag_incomplete_naics_code",
    "flag_name_naics_mismatch",
    "flag_ein_multi_naics",
    "flag_naics_count_anomaly",
    "flag_emp_size_anomaly",
    "flag_injury_rate_outlier",
    "flag_high_emp_zero_injury",
]

FLAG_LABELS = {
    "flag_invalid_naics_code": "Invalid NAICS Code",
    "flag_incomplete_naics_code": "Incomplete NAICS Code",
    "flag_name_naics_mismatch": "Name-NAICS Mismatch",
    "flag_ein_multi_naics": "EIN Multi-NAICS",
    "flag_naics_count_anomaly": "NAICS Count Anomaly",
    "flag_emp_size_anomaly": "Employee Size Anomaly",
    "flag_injury_rate_outlier": "Injury Rate Outlier",
    "flag_high_emp_zero_injury": "High Emp Zero Injury",
}

# Tiers that count as auto-resolved (pipeline made a determination)
AUTO_RESOLVED_TIERS = {
    "T0_RESOLVED", "T1_SAME_CODE", "T2_TRIVIAL_CORRECTION",
    "T2b_MULTI_ESTABLISHMENT_CONSISTENT", "T3_DESC_SUPPORTS_REPORTED",
    "T4_SAME_SECTOR", "T5b_NO_DESC_SIGNAL", "T5c_NONCREDIBLE_URL",
    "T5d_LOW_CONFIDENCE_GUESS",
}
NO_DATA_TIERS = {"T5_NO_ALTERNATIVE"}
HUMAN_REVIEW_TIERS = {
    "T5e_WEBSITE_UNSCRAPEABLE", "T5f_NO_NAICS_SIGNAL",
    "T6_CROSS_SECTOR_CONTRADICTION", "T7a_INVALID_CODE",
}

TIER_DESCRIPTIONS = {
    "T0_RESOLVED": "Confirmed correct via web/EDGAR/SAM/KC",
    "T1_SAME_CODE": "Classifier agrees with reported code",
    "T2_TRIVIAL_CORRECTION": "Minor NAICS revision (same activity)",
    "T2b_MULTI_ESTABLISHMENT_CONSISTENT": "Multi-establishment EIN consistent",
    "T3_DESC_SUPPORTS_REPORTED": "Description supports reported NAICS",
    "T4_SAME_SECTOR": "Same 2-digit sector, different sub-code",
    "T5_NO_ALTERNATIVE": "No data - unreachable establishment",
    "T5b_NO_DESC_SIGNAL": "No description signal available",
    "T5c_NONCREDIBLE_URL": "Non-credible URL source",
    "T5d_LOW_CONFIDENCE_GUESS": "Low confidence suggestion",
    "T5e_WEBSITE_UNSCRAPEABLE": "Website found but unscrapeable",
    "T5f_NO_NAICS_SIGNAL": "Website scraped, no NAICS signal",
    "T6_CROSS_SECTOR_CONTRADICTION": "Cross-sector contradiction",
    "T7a_INVALID_CODE": "Invalid NAICS code with alternative",
    "T8_MANUAL_OVERRIDE": "Manual override",
}

# Canonical sector labels: map 2-digit prefix to display name
SECTOR_NAMES = {
    "11": "Agriculture",
    "21": "Mining",
    "22": "Utilities",
    "23": "Construction",
    "31": "Manufacturing",
    "32": "Manufacturing",
    "33": "Manufacturing",
    "42": "Wholesale Trade",
    "44": "Retail Trade",
    "45": "Retail Trade",
    "48": "Transportation",
    "49": "Transportation",
    "51": "Information",
    "52": "Finance/Insurance",
    "53": "Real Estate",
    "54": "Professional Services",
    "55": "Management",
    "56": "Admin/Waste Services",
    "61": "Education",
    "62": "Health Care",
    "71": "Arts/Entertainment",
    "72": "Accommodation/Food",
    "81": "Other Services",
    "92": "Public Administration",
}

SECTOR_RANGE_LABELS = {
    "Manufacturing": "Manufacturing (31-33)",
    "Retail Trade": "Retail Trade (44-45)",
    "Transportation": "Transportation (48-49)",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _path(filename, year):
    return os.path.join(BASE_DIR, filename.replace("{yr}", year))


def count_raw_records(year):
    """Count records in ita_data_{yr}.csv without loading the full file."""
    path = _path("ita_data_{yr}.csv", year)
    with open(path, "r", encoding="utf-8-sig") as f:
        return sum(1 for _ in f) - 1  # subtract header


def load_slim_flagged(year):
    """Load only the columns we need from flagged_output_{yr}.csv."""
    keep = set(FLAG_COLUMNS + ["n_flags", "ein", "naics_code",
                                "company_name", "establishment_name",
                                "state", "annual_average_employees"])
    path = _path("flagged_output_{yr}.csv", year)
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: row[k] for k in keep if k in row})
    return rows


def load_triage(year):
    path = _path("triage_report_{yr}.csv", year)
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_materiality(year):
    path = _path("materiality_report_{yr}.csv", year)
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def load_desc_contradictions(year):
    path = _path("desc_contradictions_{yr}.csv", year)
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def normalize_sector(raw_sector):
    """Convert '31 (Manufacturing)' -> 'Manufacturing (31-33)'."""
    code = raw_sector.split(" ")[0].strip() if raw_sector else ""
    name = SECTOR_NAMES.get(code, "Unknown")
    return SECTOR_RANGE_LABELS.get(name, name)


def write_csv(filepath, rows, fieldnames):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  -> {os.path.relpath(filepath, BASE_DIR)}")


# ---------------------------------------------------------------------------
# Table 1: Pipeline Overview
# ---------------------------------------------------------------------------

def build_table1(year_data):
    """One row per year with high-level pipeline stats."""
    rows = []
    for yr in YEARS:
        d = year_data[yr]
        total = d["raw_count"]
        flagged = d["flagged_count"]
        triage_n = d["triage_count"]
        auto = d["auto_resolved"]
        human = d["human_review"]
        nodata = d["no_data"]
        mat = d["material_count"]

        rows.append({
            "year": yr,
            "total_records": total,
            "total_flagged": flagged,
            "flag_rate": f"{flagged / total * 100:.1f}%" if total else "0%",
            "triage_rows": triage_n,
            "auto_resolved": auto,
            "auto_resolve_pct": f"{auto / triage_n * 100:.1f}%" if triage_n else "0%",
            "human_review": human,
            "no_data": nodata,
            "material_count": mat,
            "material_pct": f"{mat / d['materiality_total'] * 100:.1f}%"
                            if d["materiality_total"] else "N/A",
        })

    fields = ["year", "total_records", "total_flagged", "flag_rate",
              "triage_rows", "auto_resolved", "auto_resolve_pct",
              "human_review", "no_data", "material_count", "material_pct"]
    path = os.path.join(ANALYSIS_DIR, "table1_pipeline_overview.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Table 2: Flag Prevalence by Type
# ---------------------------------------------------------------------------

def build_table2(year_data):
    """One row per flag type, columns per year."""
    rows = []
    for flag_col in FLAG_COLUMNS:
        row = {"flag_name": FLAG_LABELS[flag_col]}
        counts = []
        for yr in YEARS:
            c = year_data[yr]["flag_counts"][flag_col]
            tot = year_data[yr]["flagged_denom"]
            row[f"count_{yr}"] = c
            row[f"pct_{yr}"] = f"{c / tot * 100:.2f}%" if tot else "0%"
            counts.append(c)
        # Simple trend: compare first half avg to second half avg
        first_half = (counts[0] + counts[1]) / 2
        second_half = (counts[2] + counts[3]) / 2
        if second_half > first_half * 1.1:
            row["trend_direction"] = "increasing"
        elif second_half < first_half * 0.9:
            row["trend_direction"] = "decreasing"
        else:
            row["trend_direction"] = "stable"
        rows.append(row)

    fields = ["flag_name"]
    for yr in YEARS:
        fields += [f"count_{yr}", f"pct_{yr}"]
    fields.append("trend_direction")

    path = os.path.join(ANALYSIS_DIR, "table2_flag_prevalence.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Table 3: Triage Tier Distribution
# ---------------------------------------------------------------------------

def build_table3(year_data):
    """One row per tier, counts and pcts for each year."""
    all_tiers = sorted(set().union(
        *(year_data[yr]["tier_counts"].keys() for yr in YEARS)
    ))
    rows = []
    for tier in all_tiers:
        row = {
            "tier": tier,
            "description": TIER_DESCRIPTIONS.get(tier, ""),
        }
        for yr in YEARS:
            c = year_data[yr]["tier_counts"].get(tier, 0)
            tot = year_data[yr]["triage_count"]
            row[f"count_{yr}"] = c
            row[f"pct_{yr}"] = f"{c / tot * 100:.2f}%" if tot else "0%"
        rows.append(row)

    fields = ["tier", "description"]
    for yr in YEARS:
        fields += [f"count_{yr}", f"pct_{yr}"]

    path = os.path.join(ANALYSIS_DIR, "table3_triage_tiers.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Table 4: Top 20 Persistent EINs
# ---------------------------------------------------------------------------

def build_table4(year_data):
    """EINs flagged in most years with highest cumulative flags."""
    # {ein: {year: (flag_count, company_name, naics_code)}}
    ein_info = defaultdict(dict)
    for yr in YEARS:
        for rec in year_data[yr]["flagged_rows"]:
            nf = int(rec.get("n_flags", 0))
            if nf == 0:
                continue
            ein = rec.get("ein", "").strip()
            if not ein:
                continue
            if yr not in ein_info[ein]:
                ein_info[ein][yr] = {"flags": 0, "name": "", "naics": set()}
            ein_info[ein][yr]["flags"] += nf
            name = rec.get("company_name", "").strip() or rec.get("establishment_name", "").strip()
            if name and not ein_info[ein][yr]["name"]:
                ein_info[ein][yr]["name"] = name
            nc = rec.get("naics_code", "").strip()
            if nc:
                ein_info[ein][yr]["naics"].add(nc)

    ranked = []
    for ein, yrs_dict in ein_info.items():
        years_flagged = len(yrs_dict)
        total_flags = sum(d["flags"] for d in yrs_dict.values())
        # Pick most recent company name
        name = ""
        all_naics = set()
        for yr in reversed(YEARS):
            if yr in yrs_dict:
                if not name and yrs_dict[yr]["name"]:
                    name = yrs_dict[yr]["name"]
                all_naics |= yrs_dict[yr]["naics"]
        sectors = set()
        for nc in all_naics:
            prefix = nc[:2] if len(nc) >= 2 else nc
            sn = SECTOR_NAMES.get(prefix, "")
            if sn:
                sectors.add(sn)
        ranked.append({
            "ein": ein,
            "company_name": name,
            "naics_codes": "; ".join(sorted(all_naics)),
            "years_flagged": years_flagged,
            "total_flags": total_flags,
            "flags_per_year": f"{total_flags / years_flagged:.1f}",
            "sectors_seen": "; ".join(sorted(sectors)),
        })

    ranked.sort(key=lambda x: (-x["years_flagged"], -x["total_flags"]))
    top20 = ranked[:20]

    fields = ["ein", "company_name", "naics_codes", "years_flagged",
              "total_flags", "flags_per_year", "sectors_seen"]
    path = os.path.join(ANALYSIS_DIR, "table4_persistent_eins.csv")
    write_csv(path, top20, fields)
    return top20


# ---------------------------------------------------------------------------
# Figure 2: Materiality by Year
# ---------------------------------------------------------------------------

def build_figure2(year_data):
    """Year x materiality_class grid."""
    classes = ["MATERIAL", "MODERATE", "IMMATERIAL", "INDETERMINATE"]
    rows = []
    for yr in YEARS:
        mc = year_data[yr]["materiality_counts"]
        tot = year_data[yr]["materiality_total"]
        for cls in classes:
            c = mc.get(cls, 0)
            rows.append({
                "year": yr,
                "materiality_class": cls,
                "count": c,
                "pct": f"{c / tot * 100:.1f}%" if tot else "0%",
            })

    fields = ["year", "materiality_class", "count", "pct"]
    path = os.path.join(ANALYSIS_DIR, "figure2_materiality_by_year.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Figure 3: Cross-Sector Heatmap
# ---------------------------------------------------------------------------

def build_figure3(year_data):
    """Pooled reported_sector x suggested_sector pair counts."""
    pair_counts = Counter()
    for yr in YEARS:
        for rec in year_data[yr]["desc_contradictions"]:
            rs = normalize_sector(rec.get("reported_sector", ""))
            ss = normalize_sector(rec.get("suggested_sector", ""))
            if rs and ss and rs != ss:
                pair_counts[(rs, ss)] += 1

    rows = []
    for (rs, ss), cnt in sorted(pair_counts.items(), key=lambda x: -x[1]):
        rows.append({
            "reported_sector": rs,
            "suggested_sector": ss,
            "count": cnt,
        })

    fields = ["reported_sector", "suggested_sector", "count"]
    path = os.path.join(ANALYSIS_DIR, "figure3_sector_heatmap.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Figure 4: Year-over-Year Flag Rate Trend
# ---------------------------------------------------------------------------

def build_figure4(year_data):
    """One row per year with key rates."""
    rows = []
    for yr in YEARS:
        d = year_data[yr]
        total = d["raw_count"]
        flagged = d["flagged_count"]
        mat = d["material_count"]
        mat_tot = d["materiality_total"]
        cross = d["cross_sector_count"]

        rows.append({
            "year": yr,
            "total_records": total,
            "flagged": flagged,
            "flag_rate": f"{flagged / total * 100:.2f}%" if total else "0%",
            "material_rate": f"{mat / mat_tot * 100:.1f}%" if mat_tot else "N/A",
            "cross_sector_count": cross,
            "cross_sector_rate": f"{cross / flagged * 100:.2f}%"
                                 if flagged else "0%",
        })

    fields = ["year", "total_records", "flagged", "flag_rate",
              "material_rate", "cross_sector_count", "cross_sector_rate"]
    path = os.path.join(ANALYSIS_DIR, "figure4_flag_trend.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Helpers for new analyses
# ---------------------------------------------------------------------------

def _sector_from_naics(naics_code):
    """Map a raw NAICS code to its canonical sector label."""
    code = str(naics_code).strip()[:2]
    name = SECTOR_NAMES.get(code, "")
    if not name:
        return ""
    return SECTOR_RANGE_LABELS.get(name, name)


def _spearman_rho(ranks_a, ranks_b):
    """Spearman rank correlation (no scipy dependency)."""
    n = len(ranks_a)
    if n < 3:
        return float("nan")
    d_sq = sum((a - b) ** 2 for a, b in zip(ranks_a, ranks_b))
    return 1.0 - (6.0 * d_sq) / (n * (n * n - 1))


def _employee_bucket(raw_val):
    """Map employee count to size category."""
    try:
        emp = float(raw_val)
    except (ValueError, TypeError):
        return None
    if emp < 10:
        return "<10"
    elif emp < 50:
        return "10-49"
    elif emp < 250:
        return "50-249"
    elif emp < 500:
        return "250-499"
    else:
        return "500+"


EMPLOYEE_BUCKET_ORDER = ["<10", "10-49", "50-249", "250-499", "500+"]


def _compute_sector_year_stats(year_data):
    """Compute per-sector flag rate and rank for each year.

    Returns {sector: {year: {"flagged": int, "total": int, "rate": float, "rank": int}}}
    """
    # Count total and flagged per sector per year
    sector_stats = defaultdict(lambda: {yr: {"flagged": 0, "total": 0}
                                         for yr in YEARS})
    for yr in YEARS:
        for rec in year_data[yr]["flagged_rows"]:
            sector = _sector_from_naics(rec.get("naics_code", ""))
            if not sector:
                continue
            nf = int(rec.get("n_flags", 0))
            sector_stats[sector][yr]["total"] += 1
            if nf > 0:
                sector_stats[sector][yr]["flagged"] += nf

    # Compute rate and rank per year
    for sector in sector_stats:
        for yr in YEARS:
            s = sector_stats[sector][yr]
            s["rate"] = s["flagged"] / s["total"] if s["total"] else 0.0

    for yr in YEARS:
        sectors_with_data = [(sec, sector_stats[sec][yr]["rate"])
                             for sec in sector_stats
                             if sector_stats[sec][yr]["total"] > 0]
        sectors_with_data.sort(key=lambda x: -x[1])
        for rank, (sec, _) in enumerate(sectors_with_data, 1):
            sector_stats[sec][yr]["rank"] = rank

    return dict(sector_stats)


# ---------------------------------------------------------------------------
# Table 5: Sector Stability
# ---------------------------------------------------------------------------

def build_table5(year_data):
    """Per-sector flag rate and rank across years with stability metrics."""
    sector_stats = _compute_sector_year_stats(year_data)

    rows = []
    for sector, yr_dict in sector_stats.items():
        ranks = [yr_dict[yr].get("rank", 0) for yr in YEARS]
        rates = [yr_dict[yr]["rate"] for yr in YEARS]
        valid_ranks = [r for r in ranks if r > 0]
        mean_rank = sum(valid_ranks) / len(valid_ranks) if valid_ranks else 0
        mean_rate = sum(rates) / len(rates) if rates else 0

        # Spearman rho: correlation of ranks with time order
        if len(valid_ranks) >= 3:
            time_ranks = list(range(1, len(valid_ranks) + 1))
            rho = _spearman_rho(valid_ranks, time_ranks)
        else:
            rho = float("nan")

        # Consistency label based on rank variance
        if valid_ranks:
            rank_range = max(valid_ranks) - min(valid_ranks)
            if rank_range <= 2:
                consistency = "Very Stable"
            elif rank_range <= 5:
                consistency = "Stable"
            elif rank_range <= 10:
                consistency = "Moderate"
            else:
                consistency = "Volatile"
        else:
            consistency = "N/A"

        row = {"sector": sector, "mean_rank": f"{mean_rank:.1f}",
               "mean_rate": f"{mean_rate:.4f}",
               "consistency": consistency}
        for i, yr in enumerate(YEARS):
            row[f"rate_{yr}"] = f"{rates[i]:.4f}"
            row[f"rank_{yr}"] = ranks[i] if ranks[i] > 0 else ""
        row["spearman_rho"] = f"{rho:.3f}" if rho == rho else "N/A"
        rows.append(row)

    rows.sort(key=lambda x: float(x["mean_rank"]) if x["mean_rank"] != "0.0" else 999)

    fields = ["sector", "mean_rank", "mean_rate", "consistency"]
    for yr in YEARS:
        fields += [f"rate_{yr}", f"rank_{yr}"]
    fields.append("spearman_rho")

    path = os.path.join(ANALYSIS_DIR, "table5_sector_stability.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Table 5b: DART Rate Distortion from Cross-Sector Misclassification
# ---------------------------------------------------------------------------

def build_table5b(year_data):
    """Per-sector DART rate distortion from cross-sector misclassification."""
    # Count cross-sector flow per sector: how many records move in/out
    sector_flow = defaultdict(lambda: {"outflows": 0, "inflows": 0,
                                        "total_flagged": 0})

    for yr in YEARS:
        for rec in year_data[yr]["desc_contradictions"]:
            if rec.get("crosses_sector", "") != "Y":
                continue
            rs = normalize_sector(rec.get("reported_sector", ""))
            ss = normalize_sector(rec.get("suggested_sector", ""))
            if rs and ss and rs != ss:
                sector_flow[rs]["outflows"] += 1
                sector_flow[ss]["inflows"] += 1

        # Total flagged per sector for context
        for rec in year_data[yr]["flagged_rows"]:
            nf = int(rec.get("n_flags", 0))
            if nf > 0:
                sector = _sector_from_naics(rec.get("naics_code", ""))
                if sector:
                    sector_flow[sector]["total_flagged"] += nf

    rows = []
    for sector, flow in sector_flow.items():
        net = flow["inflows"] - flow["outflows"]
        total = flow["total_flagged"]
        rows.append({
            "sector": sector,
            "cross_sector_outflows": flow["outflows"],
            "cross_sector_inflows": flow["inflows"],
            "net_flow": net,
            "net_direction": "net importer" if net > 0 else ("net exporter" if net < 0 else "balanced"),
            "total_flagged_4yr": total,
            "distortion_pct": f"{abs(net) / total * 100:.2f}%" if total else "N/A",
        })

    rows.sort(key=lambda x: abs(x["net_flow"]), reverse=True)

    fields = ["sector", "cross_sector_outflows", "cross_sector_inflows",
              "net_flow", "net_direction", "total_flagged_4yr", "distortion_pct"]
    path = os.path.join(ANALYSIS_DIR, "table5b_dart_distortion.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Table 6: State Patterns
# ---------------------------------------------------------------------------

def build_table6(year_data):
    """Flag rate by state per year."""
    state_counts = defaultdict(lambda: {yr: {"flagged": 0, "total": 0}
                                         for yr in YEARS})
    for yr in YEARS:
        for rec in year_data[yr]["flagged_rows"]:
            st = rec.get("state", "").strip().upper()
            if not st:
                continue
            nf = int(rec.get("n_flags", 0))
            state_counts[st][yr]["total"] += 1
            if nf > 0:
                state_counts[st][yr]["flagged"] += 1

    rows = []
    for state, yr_dict in state_counts.items():
        rates = []
        row = {"state": state}
        for yr in YEARS:
            s = yr_dict[yr]
            rate = s["flagged"] / s["total"] if s["total"] else 0.0
            row[f"total_{yr}"] = s["total"]
            row[f"flagged_{yr}"] = s["flagged"]
            row[f"rate_{yr}"] = f"{rate * 100:.2f}%"
            rates.append(rate)
        avg_rate = sum(rates) / len(rates) if rates else 0
        std_dev = (sum((r - avg_rate) ** 2 for r in rates) / len(rates)) ** 0.5 if rates else 0
        row["avg_rate"] = f"{avg_rate * 100:.2f}%"
        row["std_dev"] = f"{std_dev * 100:.2f}"
        row["avg_rate_num"] = avg_rate  # for sorting
        rows.append(row)

    rows.sort(key=lambda x: -x["avg_rate_num"])
    # Assign ranking
    for i, row in enumerate(rows, 1):
        row["rank"] = i
    # Remove sort key
    for row in rows:
        del row["avg_rate_num"]

    fields = ["rank", "state"]
    for yr in YEARS:
        fields += [f"total_{yr}", f"flagged_{yr}", f"rate_{yr}"]
    fields += ["avg_rate", "std_dev"]

    path = os.path.join(ANALYSIS_DIR, "table6_state_patterns.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Figure 6: Size Patterns (Employee Size Bucket)
# ---------------------------------------------------------------------------

def build_figure6(year_data):
    """Flag rate by employee size bucket per year."""
    bucket_counts = {b: {yr: {"flagged": 0, "total": 0} for yr in YEARS}
                     for b in EMPLOYEE_BUCKET_ORDER}

    for yr in YEARS:
        for rec in year_data[yr]["flagged_rows"]:
            b = _employee_bucket(rec.get("annual_average_employees", ""))
            if b is None:
                continue
            nf = int(rec.get("n_flags", 0))
            bucket_counts[b][yr]["total"] += 1
            if nf > 0:
                bucket_counts[b][yr]["flagged"] += 1

    rows = []
    for b in EMPLOYEE_BUCKET_ORDER:
        row = {"size_bucket": b}
        for yr in YEARS:
            s = bucket_counts[b][yr]
            rate = s["flagged"] / s["total"] if s["total"] else 0.0
            row[f"total_{yr}"] = s["total"]
            row[f"flagged_{yr}"] = s["flagged"]
            row[f"rate_{yr}"] = f"{rate * 100:.2f}%"
        rows.append(row)

    fields = ["size_bucket"]
    for yr in YEARS:
        fields += [f"total_{yr}", f"flagged_{yr}", f"rate_{yr}"]

    path = os.path.join(ANALYSIS_DIR, "figure6_size_patterns.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Figure 7: Sector Flag Rate Trends
# ---------------------------------------------------------------------------

def build_figure7(year_data):
    """Per-sector flag rate trend with direction label."""
    sector_stats = _compute_sector_year_stats(year_data)

    rows = []
    for sector, yr_dict in sector_stats.items():
        rates = [yr_dict[yr]["rate"] for yr in YEARS]
        row = {"sector": sector}
        for i, yr in enumerate(YEARS):
            row[f"rate_{yr}"] = f"{rates[i]:.4f}"

        # Direction: compare first 2 vs last 2
        first_half = (rates[0] + rates[1]) / 2
        second_half = (rates[2] + rates[3]) / 2
        if second_half > first_half * 1.1:
            row["direction"] = "increasing"
        elif second_half < first_half * 0.9:
            row["direction"] = "decreasing"
        else:
            row["direction"] = "stable"

        row["mean_rate"] = f"{sum(rates) / len(rates):.4f}"
        rows.append(row)

    rows.sort(key=lambda x: -float(x["mean_rate"]))

    fields = ["sector"]
    for yr in YEARS:
        fields.append(f"rate_{yr}")
    fields += ["mean_rate", "direction"]

    path = os.path.join(ANALYSIS_DIR, "figure7_sector_trends.csv")
    write_csv(path, rows, fields)
    return rows


# ---------------------------------------------------------------------------
# Summary narrative
# ---------------------------------------------------------------------------

def build_summary(year_data, table1, table4, table5=None, table6=None):
    lines = []
    lines.append("=" * 70)
    lines.append("OSHA 300A NAICS Misclassification: 4-Year Comparative Summary")
    lines.append(f"Generated: {date.today().isoformat()}")
    lines.append("=" * 70)
    lines.append("")

    # Overview
    total_records = sum(year_data[yr]["raw_count"] for yr in YEARS)
    total_flagged = sum(year_data[yr]["flagged_count"] for yr in YEARS)
    total_triage = sum(year_data[yr]["triage_count"] for yr in YEARS)
    total_auto = sum(year_data[yr]["auto_resolved"] for yr in YEARS)
    total_human = sum(year_data[yr]["human_review"] for yr in YEARS)
    total_nodata = sum(year_data[yr]["no_data"] for yr in YEARS)

    lines.append("DATASET OVERVIEW")
    lines.append("-" * 40)
    lines.append(f"Period:           CY 2021 – CY 2024")
    lines.append(f"Total records:    {total_records:,}")
    lines.append(f"Total flagged:    {total_flagged:,} "
                 f"({total_flagged / total_records * 100:.1f}%)")
    lines.append(f"Triage rows:      {total_triage:,}")
    lines.append(f"Auto-resolved:    {total_auto:,} "
                 f"({total_auto / total_triage * 100:.1f}%)")
    lines.append(f"Human review:     {total_human:,} "
                 f"({total_human / total_triage * 100:.1f}%)")
    lines.append(f"No data:          {total_nodata:,} "
                 f"({total_nodata / total_triage * 100:.1f}%)")
    lines.append("")

    # Per-year table
    lines.append("PER-YEAR BREAKDOWN")
    lines.append("-" * 40)
    lines.append(f"{'Year':<6} {'Records':>10} {'Flagged':>10} {'Rate':>7} "
                 f"{'Auto':>7} {'Human':>7} {'NoData':>7}")
    for yr in YEARS:
        d = year_data[yr]
        lines.append(
            f"{yr:<6} {d['raw_count']:>10,} {d['flagged_count']:>10,} "
            f"{d['flagged_count'] / d['raw_count'] * 100:>6.1f}% "
            f"{d['auto_resolved']:>7,} {d['human_review']:>7,} "
            f"{d['no_data']:>7,}"
        )
    lines.append("")

    # Materiality
    lines.append("MATERIALITY DISTRIBUTION")
    lines.append("-" * 40)
    lines.append(f"{'Year':<6} {'Material':>10} {'Moderate':>10} "
                 f"{'Immaterial':>10} {'Indeterm':>10}")
    for yr in YEARS:
        mc = year_data[yr]["materiality_counts"]
        lines.append(
            f"{yr:<6} {mc.get('MATERIAL', 0):>10,} "
            f"{mc.get('MODERATE', 0):>10,} "
            f"{mc.get('IMMATERIAL', 0):>10,} "
            f"{mc.get('INDETERMINATE', 0):>10,}"
        )
    lines.append("")

    # Top persistent EINs
    lines.append("TOP 10 PERSISTENT EINs (flagged across multiple years)")
    lines.append("-" * 40)
    for i, rec in enumerate(table4[:10], 1):
        lines.append(f"  {i:>2}. {rec['company_name'][:50]:<50s}  "
                     f"years={rec['years_flagged']}  "
                     f"total_flags={rec['total_flags']}")
    lines.append("")

    # Key findings
    lines.append("KEY FINDINGS")
    lines.append("-" * 40)
    flag_rates = [year_data[yr]["flagged_count"] / year_data[yr]["raw_count"] * 100
                  for yr in YEARS]
    lines.append(f"1. Flag rates ranged from {min(flag_rates):.1f}% to "
                 f"{max(flag_rates):.1f}% across the 4-year period.")
    lines.append(f"2. The pipeline auto-resolved {total_auto / total_triage * 100:.1f}% "
                 f"of all flagged records without human review.")
    lines.append(f"3. Only {total_human:,} records ({total_human / total_triage * 100:.1f}%) "
                 f"require manual expert review across all 4 years.")

    # Cross-sector
    total_cross = sum(year_data[yr]["cross_sector_count"] for yr in YEARS)
    lines.append(f"4. {total_cross:,} cross-sector contradictions identified "
                 f"(description contradicts reported NAICS at 2-digit level).")

    # 4-year EINs
    four_year_eins = sum(1 for r in table4 if r["years_flagged"] == 4)
    lines.append(f"5. {four_year_eins} EINs were flagged in all 4 years "
                 f"(persistent misclassification).")

    # Sector stability
    if table5:
        very_stable = sum(1 for r in table5 if r["consistency"] == "Very Stable")
        volatile = sum(1 for r in table5 if r["consistency"] == "Volatile")
        top_sector = table5[0]["sector"] if table5 else "N/A"
        lines.append(f"6. Sector stability: {very_stable} sectors very stable across "
                     f"4 years, {volatile} volatile. Highest flag rate: {top_sector}.")

    # State patterns
    if table6:
        top_states = [r["state"] for r in table6[:3]]
        bottom_states = [r["state"] for r in table6[-3:]]
        lines.append(f"7. Top flag-rate states: {', '.join(top_states)}. "
                     f"Lowest: {', '.join(bottom_states)}.")

    lines.append("")
    lines.append("=" * 70)
    lines.append("See Analysis/ directory for full CSV tables and figure data.")
    lines.append("=" * 70)

    text = "\n".join(lines)
    path = os.path.join(HANDOFF_DIR, "multiyear_summary.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  -> {os.path.relpath(path, BASE_DIR)}")
    return text


# ---------------------------------------------------------------------------
# Data loading orchestrator
# ---------------------------------------------------------------------------

def load_year(yr):
    """Load all per-year data and compute aggregates."""
    print(f"\n  Loading CY {yr} ...")
    data = {}

    # Raw record count (line counting only)
    data["raw_count"] = count_raw_records(yr)
    print(f"    ita_data:          {data['raw_count']:>10,} records")

    # Flagged output (slim load)
    flagged = load_slim_flagged(yr)
    data["flagged_rows"] = flagged
    data["flagged_denom"] = len(flagged)
    data["flagged_count"] = sum(1 for r in flagged if int(r.get("n_flags", 0)) > 0)
    print(f"    flagged_output:    {len(flagged):>10,} rows "
          f"({data['flagged_count']:,} flagged)")

    # Flag counts per type
    fc = Counter()
    for r in flagged:
        for col in FLAG_COLUMNS:
            if r.get(col, "0") not in ("0", ""):
                fc[col] += 1
    data["flag_counts"] = fc

    # Triage
    triage = load_triage(yr)
    data["triage_count"] = len(triage)
    tier_counts = Counter(r["triage_tier"] for r in triage)
    data["tier_counts"] = tier_counts
    data["auto_resolved"] = sum(v for k, v in tier_counts.items()
                                if k in AUTO_RESOLVED_TIERS)
    data["no_data"] = sum(v for k, v in tier_counts.items()
                          if k in NO_DATA_TIERS)
    data["human_review"] = sum(v for k, v in tier_counts.items()
                               if k in HUMAN_REVIEW_TIERS)
    print(f"    triage_report:     {data['triage_count']:>10,} rows  "
          f"(auto={data['auto_resolved']:,}  "
          f"human={data['human_review']:,}  "
          f"nodata={data['no_data']:,})")

    # Materiality
    mat = load_materiality(yr)
    data["materiality_rows"] = mat
    mc = Counter(r["materiality_class"] for r in mat)
    data["materiality_counts"] = mc
    data["materiality_total"] = len(mat)
    data["material_count"] = mc.get("MATERIAL", 0)
    print(f"    materiality:       {data['materiality_total']:>10,} rows  "
          f"(MATERIAL={data['material_count']:,})")

    # Desc contradictions
    desc = load_desc_contradictions(yr)
    data["desc_contradictions"] = desc
    data["cross_sector_count"] = sum(
        1 for r in desc if r.get("crosses_sector", "") == "Y"
    )
    print(f"    desc_contrad:      {len(desc):>10,} rows  "
          f"(cross-sector={data['cross_sector_count']:,})")

    return data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Step 18: Multi-Year Comparative Analysis (CY 2021-2024)")
    print("=" * 60)

    os.makedirs(ANALYSIS_DIR, exist_ok=True)
    os.makedirs(HANDOFF_DIR, exist_ok=True)

    # Load all years
    year_data = {}
    for yr in YEARS:
        year_data[yr] = load_year(yr)

    # Build outputs
    print("\nGenerating tables and figures ...")

    print("\n[Table 1] Pipeline Overview")
    table1 = build_table1(year_data)

    print("\n[Table 2] Flag Prevalence by Type")
    build_table2(year_data)

    print("\n[Table 3] Triage Tier Distribution")
    build_table3(year_data)

    print("\n[Table 4] Top 20 Persistent EINs")
    table4 = build_table4(year_data)

    print("\n[Figure 2] Materiality by Year")
    build_figure2(year_data)

    print("\n[Figure 3] Cross-Sector Heatmap")
    build_figure3(year_data)

    print("\n[Figure 4] Year-over-Year Flag Rate Trend")
    build_figure4(year_data)

    print("\n[Table 5] Sector Stability")
    table5 = build_table5(year_data)

    print("\n[Table 5b] DART Rate Distortion")
    build_table5b(year_data)

    print("\n[Table 6] State Patterns")
    table6 = build_table6(year_data)

    print("\n[Figure 6] Size Patterns")
    build_figure6(year_data)

    print("\n[Figure 7] Sector Trends")
    build_figure7(year_data)

    print("\n[Summary] Narrative text")
    summary = build_summary(year_data, table1, table4, table5, table6)

    # Console output
    print("\n" + summary)

    print("\nDone. All outputs written to Analysis/ and Handoff Docs/.")


if __name__ == "__main__":
    main()
