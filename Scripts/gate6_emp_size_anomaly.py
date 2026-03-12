"""
Gate 6: Employee Size Anomaly
==============================
INPUT:  ita_data.csv (OSHA 300A records)
OUTPUT: flag column flag_emp_size_anomaly in flagged_output_{YEAR}.csv

Methodology
-----------
Flags records where the reported employee count is a statistical extreme
outlier for the given NAICS code.

For each 6-digit NAICS code in the dataset, this gate computes the
interquartile range (IQR) of annual_average_employees. A record is
flagged if its employee count falls outside Q3 + 3*IQR or Q1 - 3*IQR.

We use 3x IQR (not the standard 1.5x) because establishment sizes
legitimately vary widely within an industry. This catches only severe
outliers: e.g., a "farm" reporting 5,000 employees or a "steel mill"
with 2.

Exclusions:
  - Records with fewer than EMP_SIZE_FLOOR (5) employees are ignored
    (too small to be meaningful outliers).
  - NAICS codes with fewer than 10 records in the dataset are excluded
    (insufficient sample for reliable IQR computation).

No reference files needed: this gate uses only the dataset itself.
"""

from collections import defaultdict
from statistics import median, quantiles
from util_pipeline_config import EMP_SIZE_FLOOR


def build_emp_bounds(records):
    """Compute IQR-based outlier bounds per 6-digit NAICS code.

    Returns:
        dict mapping naics_code -> (lower_bound, upper_bound, median, q1, q3)
    """
    naics_emps = defaultdict(list)
    for r in records:
        emp = r.get("annual_average_employees", "").strip()
        naics = r["naics_code"]
        if emp and emp.isdigit() and int(emp) >= EMP_SIZE_FLOOR:
            naics_emps[naics].append(int(emp))

    bounds = {}
    for naics, emps in naics_emps.items():
        if len(emps) < 10:
            continue
        # Use statistics.quantiles for proper interpolated quartiles
        # (inclusive method, equivalent to Excel QUARTILE.INC)
        q1, _, q3 = quantiles(emps, n=4, method='inclusive')
        iqr = q3 - q1
        lower = max(EMP_SIZE_FLOOR, q1 - 3 * iqr)
        upper = q3 + 3 * iqr
        bounds[naics] = (lower, upper, median(emps), q1, q3)
    return bounds


def is_emp_outlier(emp_str, naics_code, emp_bounds):
    """Return True if employee count is an extreme outlier for this NAICS."""
    if not emp_str or not emp_str.strip().isdigit():
        return False
    emp = int(emp_str)
    if emp < EMP_SIZE_FLOOR:
        return False
    if naics_code not in emp_bounds:
        return False
    lower, upper, _, _, _ = emp_bounds[naics_code]
    return emp < lower or emp > upper


def gate6_process(records):
    """Add flag_emp_size_anomaly to each record.

    Returns:
        (records, flagged_count)
    """
    emp_bounds = build_emp_bounds(records)

    flagged = 0
    for r in records:
        flag = is_emp_outlier(
            r.get("annual_average_employees", ""),
            r["naics_code"],
            emp_bounds
        )
        r["flag_emp_size_anomaly"] = 1 if flag else 0
        if flag:
            flagged += 1

    return records, flagged


if __name__ == "__main__":
    from util_pipeline_config import load_records, INPUT_FILE
    print("Gate 6: Employee Size Anomaly")
    print("Loading records...")
    records = load_records(INPUT_FILE)
    emp_bounds = build_emp_bounds(records)
    records, flagged = gate6_process(records)
    print(f"  {len(records):,} records checked")
    print(f"  {flagged:,} flagged ({100*flagged/len(records):.1f}%)")
    print(f"  Bounds computed for {len(emp_bounds):,} NAICS codes (10+ records each)")
