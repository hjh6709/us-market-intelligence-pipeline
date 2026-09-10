# Final deck — generation source

Final 10-minute deck (10 main + 4 backup slides). Output lives in `../output/`.

| Output | What it is |
| --- | --- |
| `output/economic-event-intelligence-final.pdf` | primary presentation artifact, 14 pages, 16:9 (13.333 × 7.5 in), Pretendard embedded |
| `output/economic-event-intelligence-final.pptx` | same 14 slides as full-bleed 2× renders + speaker notes; layout identical to the PDF, text not individually editable |
| `output/final-montage.png` | contact sheet used for the visual review pass |

## Pipeline

```text
deck.html (+ architecture.svg inlined)  ──puppeteer/Chrome──▶  PDF
                                        ──per-slide 2× PNG──▶  PPTX (pptxgenjs, notes from <aside class="notes">)
                                                            ──▶  montage.py contact sheet
```

- `deck.html` — single source. One `<section class="slide">` per slide; `<aside class="notes">` = speaker notes.
  Canvas is 1280 × 720 CSS px (== 13.333 × 7.5 in). Grid: 69 px margins, title top 43 px, content top 196 px, footer 677 px.
- `architecture.svg` — presentation redraw of `../CURRENT_ARCHITECTURE.mmd` (semantic source). Inlined at `<!--INLINE:architecture.svg-->`.
- `build.mjs` — inlines SVG, renders PDF, renders slide PNGs, runs an out-of-bounds / footer-collision / clipped-text check, writes PPTX, calls `montage.py`.
- `montage.py` — contact sheet (Pillow).
- `capture_ui.mjs` — captured the real local app (`uvicorn src.serving_api:app --port 8018`, research PostgreSQL read-only, Paper writes disabled) at 1440 × 900 @2×.
- `prepare_assets.py` — deterministic crops of those captures into `assets/crop-*.png` (used on slide 9).

## Regenerate

```bash
cd docs/presentation/final/source
npm install                      # puppeteer-core, pptxgenjs (node_modules ignored)
node build.mjs                   # -> ../output/*.pdf, *.pptx, final-montage.png ; .tmp/slides/*.png for inspection
```

Requirements: Google Chrome at `/Applications/Google Chrome.app` (override with `CHROME_PATH`), node ≥ 18, python3 + Pillow, Pretendard installed locally (falls back to Noto Sans KR / Apple SD Gothic Neo).

To refresh UI screenshots, start the serving app on port 8018 against the research database, then:

```bash
node capture_ui.mjs assets && python3 prepare_assets.py
```

## Truth sources

All numbers come from `../PRESENTATION_CLAIMS.md`; status tags (CURRENT / FOUNDATION / TARGET / HISTORICAL) follow `../PRESENTATION_TRUTH.md`; screenshot decisions follow `../PRESENTATION_ASSET_MAP.md`. Speaker notes in `deck.html` carry the evidence paths and the wording that must stay conservative.
