"""Format annotated reference list into AJIM-ready APA bibliography.

Reads the annotated reference list, extracts citation lines (strips annotations),
and outputs alphabetical APA-style bibliography.
"""
import re

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT = os.path.join(_PROJECT_ROOT, "notes", "reference_list_longitudinal_stability.txt")
OUTPUT = os.path.join(_PROJECT_ROOT, "Publication", "references_ajim.txt")

with open(INPUT, "r", encoding="utf-8") as f:
    text = f.read()

# Each reference starts with a number followed by period and space
# The citation is the first line; subsequent lines until the next number are annotation
entries = re.split(r'\n(?=\d+\.\s)', text)

citations = []
for entry in entries:
    entry = entry.strip()
    if not entry or not re.match(r'\d+\.\s', entry):
        continue

    # Remove the leading number
    entry = re.sub(r'^\d+\.\s*', '', entry)

    # Split into lines: first line (or until the first sentence that is clearly annotation)
    lines = entry.split('\n')

    # The citation is everything up to and including the DOI/URL line
    # Annotations are descriptive sentences about what the paper found
    citation_parts = []
    for line in lines:
        line = line.strip()
        if not line:
            break
        citation_parts.append(line)
        # Stop after a line ending with a URL or DOI
        if re.search(r'https?://\S+$', line) or re.search(r'doi\.org/\S+$', line):
            break
        # Stop after a line ending with a page range, year in parens, or publisher
        if re.search(r'\d+[-–]\d+\.$', line):
            break
        if re.search(r'Press\.$', line):
            break

    citation = ' '.join(citation_parts)
    if citation:
        citations.append(citation)

# Sort alphabetically by first author surname
citations.sort(key=lambda c: c.lower())

with open(OUTPUT, "w", encoding="utf-8") as f:
    f.write("References\n\n")
    for i, cite in enumerate(citations, 1):
        f.write(f"{cite}\n\n")

print(f"Done. {len(citations)} references written to {OUTPUT}")
