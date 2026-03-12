"""
Step 9: NAICS Classifier - Knowledge-Based Correction
======================================================
INPUT:  flagged_output_{YEAR}.csv (records from Gates 1-8)
OUTPUT: classified_output_{YEAR}.csv - all records with match_status + naics_verified
        uncertain_for_websearch_{YEAR}.csv: subset routed to web enrichment

Methodology
-----------
This gate applies domain-knowledge keyword rules to the company/establishment
name to determine whether the reported NAICS code is plausible.

For each record, rules are evaluated in PRIORITY ORDER (specific before
generic: "animal hospital" before "hospital") until the first match:

    1. If the keyword matches AND the reported NAICS sector is in the rule's
       valid_sectors list → CONFIRMED (the code is consistent with the name).
    2. If the keyword matches AND the sector is NOT in valid_sectors →
       CORRECTED with a suggested replacement NAICS code.
    3. If no keyword matches at all → UNCONTESTED (no evidence of mismatch,
       but also no positive confirmation; confidence=low).

The valid_sectors lists are deliberately broad to avoid false corrections.
For example, "hospital" doesn't flag sector 23 (construction AT hospitals)
or 56 (janitorial services AT hospitals).

UNCERTAIN routing
-----------------
Records classified as CONFIRMED or UNCONTESTED are promoted to UNCERTAIN
and written to uncertain_for_websearch.csv when they meet either criterion:

    1. n_flags >= 2: Multiple gate flags compound the probability that
       the code is wrong. At threshold=1, the uncertain population
       balloons to ~150K records (38%) making web enrichment infeasible.
       At threshold=2, it's ~44K (11%): tractable with automated scraping.

    2. n_flags == 1 with a HARD quality flag: Records with a single flag
       from flag_invalid_naics_code, flag_name_naics_mismatch, or
       flag_naics_count_anomaly are routed to UNCERTAIN regardless of
       flag count. These flags independently indicate a likely wrong code
       (unlike the softer flag_ein_multi_naics, which often reflects
       legitimate conglomerates). Adds ~2,700 records to the scrape
       pipeline: trivial cost for meaningful coverage improvement.

    This two-pathway routing was added 2026-02-20 after triage analysis
    revealed ~97,908 Tier 2 records with quality flags were escaping
    review because the original n_flags>=2 threshold missed single-flag
    records with genuinely bad codes.

"""

import csv
import re
import os
from collections import Counter

from util_pipeline_config import DATASET_YEAR

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIPELINE_DIR = os.path.join(BASE_DIR, "pipeline_output")
INPUT_FILE = os.path.join(PIPELINE_DIR, f"flagged_output_{DATASET_YEAR}.csv")
OUTPUT_FILE = os.path.join(PIPELINE_DIR, f"classified_output_{DATASET_YEAR}.csv")
UNCERTAIN_FILE = os.path.join(PIPELINE_DIR, f"uncertain_for_websearch_{DATASET_YEAR}.csv")

# ---------------------------------------------------------------------------
# Keyword-to-NAICS correction rules
# ---------------------------------------------------------------------------
# Each rule: (compiled_regex, correct_naics_6digit, naics_description, valid_sectors)
# valid_sectors = sectors where this keyword is NOT a mismatch
# If the reported NAICS sector is NOT in valid_sectors, apply the correction.
# ---------------------------------------------------------------------------

# Negative lookahead for common street/address suffixes: prevents matching
# keywords that are actually street names (e.g., "Railroad Ave", "Airline Hwy")
_ADDR = (r"(?!\s+(?:ave|avenue|blvd|boulevard|rd|road|st|street|dr|drive|"
         r"hwy|highway|ln|lane|way|park|springs|pkwy|ct|circle|loop|"
         r"place|pl|row)\b)")

_CORRECTION_RULES_RAW = [
    # ==========================================================================
    # RULE ORDER MATTERS: specific rules must come BEFORE generic ones.
    # "animal hospital" must match before generic "hospital" to avoid
    # vet clinics being corrected to general hospitals.
    # ==========================================================================

    # Veterinary: MUST come before generic hospital/clinic rules
    (r"\bvet(erinary)?\s+(clinic|hospital|care|service|urgent)", "541940", "Veterinary Services", ["54", "62"]),
    (r"\banimal\s+hospital\b", "541940", "Veterinary Services", ["54", "62"]),
    (r"\banimal\s+clinic\b", "541940", "Veterinary Services", ["54", "62"]),

    # Healthcare facilities
    # hospital: 23=construction AT hospitals, 42=hospital supply, 44/45=hospital pharmacy/supply retail, 48=shuttles
    (r"\bhospital\b", "622110", "General Medical and Surgical Hospitals", ["62", "72", "56", "54", "81", "92", "23", "42", "44", "45", "48"]),
    # nursing home: 56=janitorial/Sodexo AT nursing homes, 72=food service AT nursing homes
    (r"\bnursing\s+home\b", "623110", "Nursing Care Facilities", ["62", "56", "72"]),
    (r"\bassisted\s+living\b", "623312", "Assisted Living Facilities for the Elderly", ["62", "72"]),
    # home health: 53=equipment rental, 44/45=home health equipment retailers
    (r"\bhome\s+health\b", "621610", "Home Health Care Services", ["62", "44", "45", "53"]),
    # urgent care: 54=vet urgent care, 92=government clinics
    (r"\burgent\s+care\b", "621493", "Freestanding Ambulatory Surgical and Emergency Centers", ["62", "54", "92"]),
    # hospice: 44/45/53=hospice thrift stores and equipment rental, 55=corporate offices
    (r"\bhospice\b", "621610", "Home Health Care Services", ["62", "44", "45", "53", "55"]),
    (r"\bphysical\s+therapy\b", "621340", "Offices of Physical Therapists", ["62"]),
    # daycare/child care: 92=government programs, 56=Sodexo at childcare, 72=food service at childcare, 81=pet daycare
    (r"\bdaycare\b", "624410", "Child Day Care Services", ["62", "92", "56", "72", "81"]),
    (r"\bchild\s+care\b", "624410", "Child Day Care Services", ["62", "92", "56", "72"]),

    # Food service contractors (the Sodexo pattern)
    (r"\bfood\s+service\s+contractor", "722310", "Food Service Contractors", ["72"]),
    (r"\bcafeteria\b", "722310", "Food Service Contractors", ["72", "61"]),
    # catering: 56=Sodexo catering at corporate sites
    (r"\bcatering\b", "722320", "Caterers", ["72", "56"]),

    # Restaurants
    # restaurant: 42=restaurant supply, 55=corporate offices, 23=construction, 56/81=repair/services
    (r"\brestaurant\b", "722511", "Full-Service Restaurants", ["72", "42", "55", "23", "56", "81"]),
    (r"\bpizzeria\b", "722511", "Full-Service Restaurants", ["72"]),

    # Hotels
    # hotel: 56=janitorial AT hotels, 23=construction/rehab, 71=casino hotels, 42=hotel supply, 81=linen svc
    (r"\bhotel\b", "721110", "Hotels (except Casino Hotels)", ["72", "53", "56", "23", "71", "42", "81"]),
    (r"\bmotel\b", "721110", "Hotels (except Casino Hotels)", ["72", "56"]),
    # resort: 53=RV resort property mgmt, 62=medical "resorts", 56=ABM/janitorial, 81=pet resort,
    #         23=resort construction, 92=state resort parks, 42=resort supply
    (r"\bresort\b", "721110", "Hotels and Motels", ["72", "71", "53", "62", "56", "81", "23", "92", "42"]),

    # Construction
    # plumbing: 56/72=Sodexo, 32/33=plumbing fixture/product mfg, 44=plumbing supply retail, 22=water, 49=warehouse
    (r"\bplumbing\b", "238220", "Plumbing, Heating, and Air-Conditioning Contractors", ["23", "42", "81", "56", "72", "32", "33", "44", "22", "49"]),
    # roofing: 42=roofing material wholesalers, 44=roofing supply retail, 49=warehousing
    (r"\broofing\b", "238160", "Roofing Contractors", ["23", "32", "33", "42", "44", "49"]),
    # excavating: 48/49=trucking+excavating dual, 21=oil field, 56=landscaping+excavating, 81=repair
    (r"\bexcavating\b", "238910", "Site Preparation Contractors", ["23", "48", "49", "21", "56", "81"]),
    (r"\bdemolition\b", "238910", "Site Preparation Contractors", ["23", "56"]),
    # paving: 21=mining/quarry+paving, 48=trucking+paving, 81=repair
    (r"\bpaving\b", "238990", "All Other Specialty Trade Contractors", ["23", "32", "21", "48", "81"]),
    # concrete: exclude "crushed concrete" and "recycled concrete" (those are demolition waste, not construction)
    # 44=concrete form retailers, 56=waste/janitorial, 21=mining, 48=trucking, 72=Sodexo, 53=equipment rental
    (r"(?<!crushed\s)(?<!recycled\s)\bconcrete\b(?!\s+product)", "238110", "Poured Concrete Foundation and Structure Contractors", ["23", "32", "42", "44", "56", "21", "48", "72", "53"]),
    # masonry: 32/33=masonry product mfg, 44=masonry supply retailers, 72=Sodexo at masonry training
    (r"\bmasonry\b", "238140", "Masonry Contractors", ["23", "42", "32", "33", "44", "72"]),
    (r"\bdrywall\b", "238310", "Drywall and Insulation Contractors", ["23", "42", "44"]),
    (r"\bcarpentry\b", "238350", "Finish Carpentry Contractors", ["23", "32"]),
    # hvac: 56=Sodexo HVAC, 72=food service contractors doing HVAC, 55=corporate offices
    (r"\bhvac\b", "238220", "Plumbing, Heating, and Air-Conditioning Contractors", ["23", "33", "42", "81", "56", "72", "55"]),
    (r"\bgeneral\s+contractor", "236220", "Commercial and Institutional Building Construction", ["23"]),

    # Manufacturing
    # foundry: commonly used as building/location name ("The Foundry at...", "Foundry Commons")
    # 32=nonmetallic mfg, 23=construction at foundry-named buildings, 42=foundry supply wholesale,
    # 53=property mgmt, 54=consulting, 56=facility services, 72=food service, 49=warehouse/courier, 23=construction
    (r"\bfoundry\b" + _ADDR, "331511", "Iron Foundries", ["33", "32", "23", "42", "53", "54", "56", "72", "49"]),
    (r"\bmachine\s+shop\b", "332710", "Machine Shops", ["33"]),
    # welding: 72=Sodexo at welding facilities, 21=oil field welding, 54/61=welding schools/consulting
    (r"\bwelding\b", "332710", "Machine Shops", ["33", "23", "81", "42", "72", "21", "54", "61"]),
    # semiconductor: 42=semiconductor wholesalers, 32=materials mfg
    (r"\bsemiconductor\b", "334413", "Semiconductor and Related Device Manufacturing", ["33", "42", "32"]),
    # refinery: 31=food-grade refining, 56=janitorial, 72=food svc, 48=transport AT refineries, 33=metals refining
    (r"\brefinery\b", "324110", "Petroleum Refineries", ["32", "21", "23", "31", "56", "72", "48", "33"]),
    # brewery: 72=brewpubs, 42=brewery supply wholesale, 44/45=brewery retail
    (r"\bbrewery\b", "312120", "Breweries", ["31", "72", "42", "44", "45"]),
    # bakery: 33=bakery equipment, 42=bakery supply, 56=ABM sanitation at bakeries, 48/49=bakery transport
    (r"\bbakery\b", "311811", "Retail Bakeries", ["31", "72", "44", "45", "33", "42", "56", "48", "49", "62"]),
    # slaughter: commonly a surname or place name - add address exclusion
    # 44=retail on Slaughter Lane, 62=biomat on Slaughter St, 71=bowling alley named Slaughter
    (r"\bslaughter(?:house|ing)" + _ADDR, "311611", "Animal (except Poultry) Slaughtering", ["31"]),
    (r"\bdistillery\b", "312140", "Distilleries", ["31", "72"]),
    # winery: 72=restaurant/bar, 44/45=tasting rooms, 42=wine wholesalers, 62=social services at winery loc
    (r"\bwinery\b", "312130", "Wineries", ["31", "72", "44", "45", "42", "62"]),

    # Transportation
    # trucking: broad set of sectors - many companies with "trucking" in name across industries
    (r"\btrucking\b", "484110", "General Freight Trucking, Local", ["48", "49", "81", "23", "42", "21", "11", "56", "32", "33"]),
    # freight: often in company names unrelated to trucking ("Harbor Freight Tools", "American Freight")
    # 54=consulting, 33=mfg, 44/45=retail, 53=leasing
    (r"\bfreight\b" + _ADDR, "484110", "General Freight Trucking, Local", ["48", "49", "42", "21", "54", "33", "44", "45", "53", "32"]),
    # courier: 32=printing companies named "Courier", 92=government courier services
    (r"\bcourier\b", "492110", "Couriers and Express Delivery Services", ["49", "48", "42", "56", "32", "92"]),
    # railroad: commonly a street name - add address exclusion
    # 33/34=railroad car/equipment mfg, 53=railroad equipment leasing, 71=railroad museums/theme parks,
    # 22=utilities on railroad streets, 56=waste haulers
    (r"\brailroad\b" + _ADDR, "482111", "Line-Haul Railroads", ["48", "23", "81", "33", "53", "71", "22", "56"]),
    # airline: commonly a street name - add address exclusion
    # 81=car washes, 53=storage, 44/45=retail, 22=utilities, 56=logistics at Airline addresses
    (r"\bairline\b" + _ADDR, "481111", "Scheduled Passenger Air Transportation", ["48", "81", "53", "44", "45", "22", "56"]),
    # ambulance: 92=fire depts running EMS
    (r"\bambulance\b", "621910", "Ambulance Services", ["62", "48", "92"]),

    # Agriculture
    (r"\bfeedlot\b", "112112", "Cattle Feedlots", ["11"]),
    # hatchery: 31=poultry processing at hatcheries, 44=auto dealers on Hatchery Rd
    (r"\bhatchery\b" + _ADDR, "112340", "Poultry Hatcheries", ["11", "92", "31"]),
    # greenhouse: commonly a building name ("Greenhouse Apartments", "MillerKnoll Greenhouse")
    # 33=mfg buildings, 53=apartments, 54=R&D, 62=sheltered workshops, 56=landscaping
    (r"\bgreenhouse\b", "111419", "Other Food Crops Grown Under Cover", ["11", "44", "33", "53", "54", "62", "56"]),

    # Mining
    # mining: commonly in street/location names ("Florida Mining Blvd"), add address exclusion
    # 81=repair at mines, 33=mining equipment mfg, 42=mining supply wholesale,
    # 23=mining construction, 32=mining chemicals, 72=Sodexo at mines, 22=power at mine sites
    (r"\bmining\b" + _ADDR, "212210", "Iron Ore Mining", ["21", "81", "33", "42", "23", "32", "72", "22"]),
    # quarry: very commonly a place/location name ("Quarry Market", "Quarry Hill", "Alamo Quarry")
    # Broad valid sectors since most "quarry" matches are location names, not actual quarries
    (r"\bquarry\b" + _ADDR, "212312", "Crushed and Broken Limestone Mining and Quarrying",
     ["21", "32", "23", "42", "44", "45", "53", "56", "62", "71", "72", "49", "92"]),

    # Utilities
    # power plant: 23=construction, 56=services, 72=Sodexo at power plants
    (r"\bpower\s+plant\b", "221112", "Fossil Fuel Electric Power Generation", ["22", "23", "56", "72"]),
    # wastewater: 23=construction, 56=services, 72=Sodexo, 33=equipment mfg, 92=government
    (r"\bwastewater\b", "221320", "Sewage Treatment Facilities", ["22", "92", "23", "56", "72", "33"]),
]

# Pre-compile
CORRECTION_RULES = [
    (re.compile(pattern, re.IGNORECASE), naics, desc, sectors)
    for pattern, naics, desc, sectors in _CORRECTION_RULES_RAW
]


def classify_record(name, reported_naics):
    """
    Returns (match_status, naics_verified, description, confidence, reasoning)

    Logic: iterate through rules in order. On FIRST keyword match:
      - If reported sector IS in valid_sectors → CONFIRMED (stop checking)
      - If reported sector is NOT in valid_sectors → CORRECTED (stop checking)
    This prevents later, broader rules from overriding earlier, specific ones
    (e.g., "animal hospital" matching vet rule before generic "hospital" rule).
    """
    if not name or not reported_naics or len(reported_naics) < 2:
        return "UNCONTESTED", reported_naics, "", "low", "Insufficient data for classification"

    reported_sector = reported_naics[:2]

    for pattern, correct_naics, desc, valid_sectors in CORRECTION_RULES:
        if pattern.search(name):
            if reported_sector not in valid_sectors:
                correct_sector = correct_naics[:2]
                reasoning = (f"Name keyword matches {desc} (sector {correct_sector}), "
                             f"but reported NAICS {reported_naics} is sector {reported_sector}")
                return "CORRECTED", correct_naics, desc, "high", reasoning
            else:
                # Keyword matched but sector is valid: confirmed, stop here
                return "CONFIRMED", reported_naics, "", "high", f"Keyword matched but sector {reported_sector} is valid"

    # No keyword match at all: uncontested (absence of evidence, not confirmation)
    return "UNCONTESTED", reported_naics, "", "low", "No keyword-sector conflict detected"


def main():
    """Classify all records and route UNCERTAIN ones to web enrichment.

    Three-phase process:
      1. Load flagged_output_{YEAR}.csv (all records with gate flags).
      2. Run classify_record() on each: produces CONFIRMED or CORRECTED.
      3. Promote CONFIRMED records with n_flags >= 2 to UNCERTAIN for web
         enrichment (see module docstring for threshold rationale).

    Writes:
      - classified_output.csv:         full dataset with match_status column
      - uncertain_for_websearch.csv:   UNCERTAIN subset for Gates 10-13
    """
    print("Loading flagged records...")
    records = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            records.append(row)
    print(f"  Loaded {len(records):,} records")

    # Classify
    print("Classifying...")
    stats = Counter()
    results = []
    uncertain = []

    for r in records:
        name = r.get("establishment_name", "") or r.get("company_name", "")
        reported = r["naics_code"]

        status, verified, desc, conf, reasoning = classify_record(name, reported)
        stats[status] += 1

        out = {
            "id": r["id"],
            "establishment_name": r["establishment_name"],
            "company_name": r.get("company_name", ""),
            "street_address": r["street_address"],
            "city": r["city"],
            "state": r["state"],
            "zip_code": r["zip_code"],
            "naics_code": reported,
            "industry_description": r.get("industry_description", ""),
            "naics_verified": verified,
            "naics_description": desc,
            "confidence": conf,
            "reasoning": reasoning,
            "match_status": status,
            "priority_tier": r["priority_tier"],
            "n_flags": r["n_flags"],
        }
        results.append(out)

        # Promote to UNCERTAIN: records where gate flags suggest the code may
        # be wrong despite no keyword mismatch. Two pathways:
        #   1. n_flags >= 2: multiple flags compound probability of wrong code
        #   2. n_flags == 1 with a HARD quality flag: invalid_naics, name_mismatch,
        #      or count_anomaly each independently indicate a likely wrong code
        # The soft flag (ein_multi_naics) alone is insufficient: many multi-NAICS
        # EINs are legitimate conglomerates.
        n_flags = int(r.get("n_flags", 0))
        has_hard_flag = (
            str(r.get("flag_invalid_naics_code", "")).strip() in ("1", "True", "true")
            or str(r.get("flag_name_naics_mismatch", "")).strip() in ("1", "True", "true")
            or str(r.get("flag_naics_count_anomaly", "")).strip() in ("1", "True", "true")
        )
        # CORRECTED records are intentionally excluded: the classifier found a
        # definitive keyword mismatch and already assigned a replacement NAICS.
        # They don't need web verification: the correction IS the resolution.
        if status in ("CONFIRMED", "UNCONTESTED") and (n_flags >= 2 or (n_flags == 1 and has_hard_flag)):
            reason_text = ("Hard quality flag: needs web verification"
                           if n_flags == 1 else
                           "Multiple flags but no keyword mismatch: needs web verification")
            out_u = dict(out)
            out_u["match_status"] = "UNCERTAIN"
            out_u["confidence"] = "low"
            out_u["reasoning"] = reason_text
            uncertain.append(out_u)

    # Write full output
    out_fields = ["id", "establishment_name", "company_name",
                  "street_address", "city", "state", "zip_code",
                  "naics_code", "industry_description",
                  "naics_verified", "naics_description",
                  "confidence", "reasoning", "match_status",
                  "priority_tier", "n_flags"]

    print(f"Writing {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(results)

    print(f"Writing {UNCERTAIN_FILE}...")
    with open(UNCERTAIN_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(uncertain)

    # Summary
    print("\n" + "=" * 50)
    print("CLASSIFICATION SUMMARY")
    print("=" * 50)
    print(f"  Total records:  {len(records):,}")
    print(f"  CONFIRMED:      {stats['CONFIRMED']:,} (keyword matched, sector valid)")
    print(f"  UNCONTESTED:    {stats['UNCONTESTED']:,} (no keyword match, low confidence)")
    print(f"  CORRECTED:      {stats['CORRECTED']:,}")
    print(f"  UNCERTAIN:      {len(uncertain):,} (multi-flag or quality-flag, no keyword match)")

    # Show correction breakdown
    correction_types = Counter()
    for r in results:
        if r["match_status"] == "CORRECTED":
            key = f"{r['naics_code']} -> {r['naics_verified']}"
            correction_types[key] += 1

    print("\n  CORRECTION BREAKDOWN:")
    for key, count in correction_types.most_common(20):
        print(f"    {key}: {count:,}")

    print(f"\n  Output: {OUTPUT_FILE}")
    print(f"  Uncertain (for web search): {UNCERTAIN_FILE}")


if __name__ == "__main__":
    main()
