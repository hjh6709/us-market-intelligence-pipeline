"""Crop the 2x (2880x1800) local UI captures into slide-ready assets.

Inputs are produced by capture_ui.mjs against the local serving app
(port 8018, existing research PostgreSQL, Paper writes disabled).
Crops are deterministic so the deck can be regenerated.
"""
from pathlib import Path
from PIL import Image

HERE = Path(__file__).parent
A = HERE / "assets"

def crop(src, box, dst):
    img = Image.open(A / src)
    img.crop(box).save(A / dst, optimize=True)
    print(dst, Image.open(A / dst).size)

# Research: price chart + release marker + research-only gate (dominant slide-9 visual)
crop("research-chart.png", (40, 220, 1880, 1110), "crop-research-chart.png")
# Research: RESEARCH_ONLY / NO_TRADE gate box
crop("research-chart.png", (1925, 440, 2770, 660), "crop-research-gate.png")
# Overview: KPI grid top two rows only (compact slide-9 support)
crop("overview.png", (240, 680, 2640, 1080), "crop-overview-kpi-top.png")
