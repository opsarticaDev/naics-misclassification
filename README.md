---
title: Systematic NAICS Misclassification in OSHA 300A Establishment Data (2021-2024)
tags: []
type: note
project: NAICS Project
status: active
created: 2026-03-07
modified: 2026-03-07
---
# Systematic NAICS Misclassification in OSHA 300A Establishment Data (2021-2024)

[[index_naics-project]]

Rule-based validation pipeline for identifying NAICS code misclassification in OSHA establishment-level injury and illness records. Four years of data, 1.46 million records, 17 processing stages.

## Problem

Employers self-report their NAICS industry classification code when submitting injury data to OSHA. No systematic verification exists. Misclassification distorts injury rate comparisons, misdirects inspection targeting, and creates workers' compensation premium gaps estimated at $32.9 billion annually.

## Data

- 1,455,348 OSHA ITA (Injury Tracking Application) 300A summary records
- Calendar years 2021 through 2024
- Source: publicly available OSHA establishment-specific injury data
- Reference data: official NAICS hierarchies (2017, 2022), BLS injury rates, SIC-NAICS crosswalks, SAM.gov entity records

## Method

17-stage rule-based pipeline (no machine learning):

**Detection (Gates 1-8):** Invalid NAICS codes, incomplete codes, name-code mismatches, multi-NAICS EINs, employee count anomalies, size distribution anomalies, injury rate outliers, high-employment zero-injury flags.

**Classification and Enrichment (Steps 9-14):** Keyword classification, web scraping via SearXNG and Playwright, SEC EDGAR lookup, SAM.gov bulk matching, multi-source merge, known company enrichment (128 curated rules), multi-establishment propagation.

**Assessment (Steps 15-17):** BLS DART rate materiality scoring, description-based contradiction detection, 15-tier triage classification.

**Validation:** 500-record stratified sample, 5 strata. Population-weighted accuracy: 90.9%. Pipeline recall: 94.9%. Flagging precision: 21.1% (by design; screening tool, not auto-corrector).

## Key Findings

- 162,072 misclassified establishments (11.1% flag rate, stable across four years)
- 382 chronic downshifter EINs with repeated movement to lower-risk codes
- 86-90% of misclassifications cross NAICS sector boundaries
- 44-65% are material (DART rate difference exceeds 50%)
- $32.9 billion estimated annual workers' compensation premium gap; $9.4 billion in California alone

## Repository Structure

```
Scripts/              31 pipeline scripts (gates, classifiers, enrichment, triage)
Analysis/             22 analysis CSVs (tables, figures, persistence, premium impact)
Publication/          20 HTML visualizations (10 figures, 9 tables, 1 index)
  figures_png/        10 high-resolution PNG exports
  references_ajim.txt 47-reference APA bibliography
Reference/            NAICS hierarchies, BLS rates, crosswalks
```

## How to Run

```python
# Process a single year
python Scripts/util_run_pipeline.py --year 2023

# Run multi-year analysis
python Publication/multiyear_analysis.py
python Publication/persistence_tracker.py
python Publication/wcirb_premium_analysis.py
```

Pipeline requires Python 3.10+, pandas, SearXNG instance for web enrichment. Raw OSHA ITA data available from OSHA's public injury tracking application.

## Citation

See `CITATION.cff` for citation information.

## License

MIT. See `LICENSE`.

## OSF Project

https://osf.io/5b6ma/
