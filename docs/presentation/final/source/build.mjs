// Build the final presentation from deck.html.
//
//   node build.mjs            -> output/economic-event-intelligence-final.pdf
//                                output/economic-event-intelligence-final.pptx
//                                output/final-montage.png
//                                .tmp/slides/slide-NN.png (inspection renders, not committed)
//
// Requirements: Google Chrome (macOS path below), node >= 18, `npm i` in this
// directory (puppeteer-core, pptxgenjs), python3 + Pillow for the montage.
// Fonts: Pretendard must be installed locally; PDF embeds the glyphs.

import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import puppeteer from 'puppeteer-core';
import pptxgen from 'pptxgenjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const FINAL = path.resolve(HERE, '..');
const OUT = path.join(FINAL, 'output');
const TMP = path.join(HERE, '.tmp');
const SLIDES = path.join(TMP, 'slides');
const CHROME = process.env.CHROME_PATH || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const NAME = 'economic-event-intelligence-final';

fs.mkdirSync(OUT, { recursive: true });
fs.mkdirSync(SLIDES, { recursive: true });

// 1. inline architecture.svg into the deck (single-source HTML build)
let html = fs.readFileSync(path.join(HERE, 'deck.html'), 'utf8');
html = html.replace(/<!--INLINE:([^>]+?)-->/g, (_, f) => fs.readFileSync(path.join(HERE, f.trim()), 'utf8'));
const builtHtml = path.join(TMP, 'deck.built.html');
fs.writeFileSync(builtHtml, html);
// assets are referenced relatively from source/, so copy them next to the built html
fs.cpSync(path.join(HERE, 'assets'), path.join(TMP, 'assets'), { recursive: true });

const browser = await puppeteer.launch({ executablePath: CHROME, headless: true, args: ['--hide-scrollbars', '--font-render-hinting=none'] });
const page = await browser.newPage();
await page.setViewport({ width: 1280, height: 720, deviceScaleFactor: 2 });
await page.goto('file://' + builtHtml, { waitUntil: 'networkidle0' });
await page.evaluate(() => document.fonts.ready);

// 2. PDF (print CSS: @page 13.333in x 7.5in, one .slide per page)
await page.emulateMediaType('print');
await page.pdf({ path: path.join(OUT, `${NAME}.pdf`), preferCSSPageSize: true, printBackground: true });
await page.emulateMediaType('screen');

// 3. per-slide PNG renders (2x) + speaker notes + automated overflow check
const meta = await page.evaluate(() => {
  const out = [];
  document.querySelectorAll('.slide').forEach((s, i) => {
    const r = s.getBoundingClientRect();
    const issues = [];
    s.querySelectorAll('*').forEach(el => {
      if (el.classList.contains('notes') || el.closest('.notes')) return;
      const b = el.getBoundingClientRect();
      if (b.width === 0 || b.height === 0) return;
      if (b.right > r.right + 0.5 || b.bottom > r.bottom + 0.5 || b.left < r.left - 0.5 || b.top < r.top - 0.5) {
        issues.push(`out-of-bounds <${el.tagName.toLowerCase()} class="${el.className}"> (${Math.round(b.left - r.left)},${Math.round(b.top - r.top)},${Math.round(b.right - r.left)},${Math.round(b.bottom - r.top)})`);
      }
      if (el.scrollHeight > el.clientHeight + 2 && getComputedStyle(el).overflow !== 'visible') issues.push(`clipped text <${el.tagName.toLowerCase()} class="${el.className}">`);
    });
    // footer collision: any non-footer element whose bottom crosses the footer top
    const footer = s.querySelector('.footer');
    if (footer) {
      const ft = footer.getBoundingClientRect().top;
      s.querySelectorAll('.content *, .hero-row *, .life *, .conc-note *').forEach(el => {
        const b = el.getBoundingClientRect();
        if (b.height > 0 && b.bottom > ft - 4 && el.textContent.trim()) issues.push(`footer collision <${el.tagName.toLowerCase()} class="${el.className}"> bottom=${Math.round(b.bottom - r.top)}`);
      });
    }
    out.push({ i, id: s.dataset.id, top: r.top, notes: (s.querySelector('.notes')?.innerText || '').trim(), issues: [...new Set(issues)] });
  });
  return out;
});

const slides = await page.$$('.slide');
for (let i = 0; i < slides.length; i++) {
  await slides[i].screenshot({ path: path.join(SLIDES, `slide-${String(i + 1).padStart(2, '0')}.png`) });
}
await browser.close();

fs.writeFileSync(path.join(TMP, 'notes.json'), JSON.stringify(meta.map(({ id, notes, issues }) => ({ id, notes, issues })), null, 2));
let issueCount = 0;
for (const m of meta) { for (const is of m.issues) { issueCount++; console.log(`[check] slide ${m.id}: ${is}`); } }
console.log(`[check] ${slides.length} slides, ${issueCount} layout issues`);

// 4. PPTX: 16:9, each slide = full-bleed 2x render of the same HTML slide + speaker notes.
//    Layout fidelity is exact (same render as the PDF); text is not individually editable.
const pptx = new pptxgen();
pptx.layout = 'LAYOUT_WIDE'; // 13.333 x 7.5 in
pptx.title = 'Economic Event Intelligence & Strategy Validation Platform';
pptx.author = 'hjh6709';
for (let i = 0; i < slides.length; i++) {
  const s = pptx.addSlide();
  s.addImage({ path: path.join(SLIDES, `slide-${String(i + 1).padStart(2, '0')}.png`), x: 0, y: 0, w: 13.333, h: 7.5 });
  if (meta[i].notes) s.addNotes(meta[i].notes);
}
await pptx.writeFile({ fileName: path.join(OUT, `${NAME}.pptx`) });

// 5. montage of the 10 main slides (5 x 2) + backup strip
execFileSync('python3', [path.join(HERE, 'montage.py'), SLIDES, path.join(OUT, 'final-montage.png')], { stdio: 'inherit' });
console.log('done ->', OUT);
