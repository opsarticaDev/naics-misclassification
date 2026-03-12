---
title: Figure Captions Ajim
tags: []
type: note
project: NAICS Project
status: active
created: 2026-03-12
modified: 2026-03-12
---

[[index_naics-project]]

**Figure 1.** Validation Pipeline Funnel: From Raw Records to Triage Resolution
Of 1,455,348 OSHA 300A establishment records (CY 2021-2024), 774,113 (53.2%) triggered at least one of eight validation gates, yielding 162,072 unique EIN-NAICS pairs promoted to triage. Resolution outcomes show that 156,494 pairs (96.6%) were auto-resolved by the pipeline, 2,987 (1.8%) required manual review, and 2,591 (1.6%) lacked sufficient data for classification. The high auto-resolution rate reflects the pipeline's emphasis on ruling out false positives before escalating to human reviewers.

**Figure 2.** Materiality Distribution of Flagged Records by Calendar Year
Stacked bar chart showing the materiality classification of flagged records across CY 2021-2024, with categories for material, moderate, immaterial, and indeterminate severity. Material flags account for the largest share in 2021 (56.8%) and 2022 (64.9%), while moderate flags rise sharply in 2023 (17,853 records, 49.7%). Annual totals range from 28,413 (2022) to 35,884 (2023), with the indeterminate category declining steadily from 177 to 42 records as pipeline enrichment improved over time.

**Figure 3.** Cross-Sector Contradiction Heatmap
Heatmap displaying the frequency of cross-sector NAICS contradictions, where the pipeline's suggested 2-digit sector differs from the employer-reported sector. Rows represent the reported sector and columns represent the suggested sector. Transportation (48-49) is the most frequent source of contradictions, with 39 cases shifting to Administrative/Waste Services and 35 to Professional Services. The 360 records with unknown reported sector are excluded.

**Figure 4.** Flag Rate and Material Rate Trends, CY 2021-2024
Dual-axis chart overlaying total record counts (bars) with flag rate and material rate (lines) across four calendar years. The flag rate remains stable, ranging from 52.1% (2023) to 54.6% (2021), while total submissions grew from 317,978 to 396,263. The material rate, representing the share of flagged records classified as material severity, declined from 64.9% in 2022 to 44.0% in 2023. This divergence between a stable flag rate and a declining material rate suggests that newer submissions carry more moderate or ambiguous misclassifications.

**Figure 5.** Triage Resolution Summary: Outcomes and Tier Breakdown
Panel A shows a donut chart of 162,072 triage records by resolution outcome: 96.6% auto-resolved, 1.8% manual review, and 1.6% no data. Panel B presents a horizontal bar chart of the 14 resolution tiers ranked by four-year aggregate count. Tier T3 (description supports reported code) dominates at 78,273 records, followed by T5b (no description signal, 24,019) and T1 (classifier agrees, 18,176). Cross-sector contradictions (T6) account for 529 records, while invalid codes with alternatives (T7a) total 260.

**Figure 6.** Flag Rate by Establishment Size, CY 2021-2024
Grouped bar chart showing flag rates across five establishment size categories (fewer than 10, 10-49, 50-249, 250-499, and 500+ employees) for each calendar year. Flag rates increase monotonically with size: establishments with 500+ employees are flagged at approximately 76% across all four years, compared to 41-47% for those with fewer than 10 employees. This 32-percentage-point gap is consistent across years, indicating that establishment size is a structural predictor of NAICS coding complexity.

**Figure 7.** Sector-Level Flag Rate Trends, CY 2021-2024
Line chart tracking flag rates (flags per record) for the top five, bottom five, and middle ten NAICS sectors over four years. Retail Trade (44-45) leads all sectors with rates above 1.0 throughout the period. The Information sector shows the steepest decline, falling from 1.10 in 2021 to 0.59 in 2024, while the Management sector rises from 0.52 to 0.71. The middle ten sectors are rendered as a shaded band, spanning approximately 0.50 to 0.73, indicating moderate convergence across industries over time.

**Figure 8.** Validation Accuracy by Pipeline Stratum
Panel A displays reported NAICS accuracy for a stratified random sample of 500 CY 2023 records across five pipeline strata: CLEAN (96.0%, n = 100), FLAGGED_NOT_PROMOTED (85.0%, n = 100), AUTO_RESOLVED (90.7%, n = 150), MANUAL_REVIEW (55.0%, n = 100), and NO_DATA (90.0%, n = 50). The population-weighted accuracy is 90.9%. Panel B reports pipeline flagging performance: recall (sensitivity) of 94.9%, with 74 of 78 truly misclassified records correctly flagged, and precision (positive predictive value) of 21.1%. Low precision is by design, as 97.3% of flagged records are auto-resolved without human intervention.

**Figure 9.** Year-over-Year NAICS Code Stability
Panel A presents stacked bar charts of consecutive year-pair transitions for 366,870 establishments appearing in two or more years, totaling 623,102 year-over-year pairs. Stability is high at 97.0%, with within-sector shifts declining from 4,607 (2021-2022) to 3,196 (2023-2024) and cross-sector shifts peaking at 3,396 in the 2022-2023 pair. Panel B shows the DART rate direction among 18,600 establishments that changed codes: 9,220 (49.6%) shifted downward to a lower DART rate, 5,474 (29.4%) shifted upward, and 3,385 (18.2%) were lateral. The excess of downward shifts suggests that NAICS code changes disproportionately move establishments into less hazardous industry classifications.

**Figure 10.** Workers' Compensation Premium Rate Ladder and Cross-Sector Misclassification Patterns
Panel A ranks 24 NAICS sub-sectors by 2025 WCIRB California advisory pure premium rates (per $100 of payroll), ranging from $11.61 for Construction to $0.21 for Professional Services, Finance/Insurance, and Management of Companies. Panel B displays the top 10 cross-sector misclassification patterns by premium impact. The largest rate delta is $11.40 (Construction to Professional Services, n = 490), followed by $6.24 (Warehousing to Healthcare, n = 561). Among 36,365 cross-sector misclassifications with valid rate suggestions, 27.7% involve underpayment, where the reported sector carries a lower workers' compensation rate than the correct sector.
