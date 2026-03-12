"""Convert 10 HTML figures to high-resolution PNG for journal submission."""
import os
from playwright.sync_api import sync_playwright

PUB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Publication")
OUT_DIR = os.path.join(PUB_DIR, "figures_png")

FIGURES = [
    "fig1_pipeline_funnel.html",
    "fig2_materiality_stacked.html",
    "fig3_sector_heatmap.html",
    "fig4_flag_trends.html",
    "fig5_triage_resolution.html",
    "fig6_size_patterns.html",
    "fig7_sector_trends.html",
    "fig8_validation_accuracy.html",
    "fig9_yoy_stability.html",
    "fig10_wcirb_rate_ladder.html",
]

os.makedirs(OUT_DIR, exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1200, "height": 900}, device_scale_factor=3)

    for fname in FIGURES:
        src = os.path.join(PUB_DIR, fname)
        out_name = fname.replace(".html", ".png")
        out_path = os.path.join(OUT_DIR, out_name)

        page.goto(f"file:///{src.replace(os.sep, '/')}")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(500)

        # Get actual content height
        height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": 1200, "height": height + 40})
        page.wait_for_timeout(200)

        page.screenshot(path=out_path, full_page=True)
        print(f"OK  {out_name}")

    browser.close()

print(f"\nDone. {len(FIGURES)} figures saved to {OUT_DIR}")
