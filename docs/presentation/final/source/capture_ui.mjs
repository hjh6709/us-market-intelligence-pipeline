import puppeteer from 'puppeteer-core';
const CH = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const out = process.argv[2] || 'shots';
const browser = await puppeteer.launch({ executablePath: CH, headless: true, args: ['--hide-scrollbars'] });
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: 2 });
const base = 'http://127.0.0.1:8018';
const wait = (ms) => new Promise(r => setTimeout(r, ms));

// Overview
await page.goto(base + '/overview', { waitUntil: 'networkidle0' });
await wait(800);
await page.screenshot({ path: `${out}/overview.png` });

// Research: CPI / NVDA, 1m
await page.goto(base + '/', { waitUntil: 'networkidle0' });
await wait(800);
await page.select('#event-type', 'CPI').catch(()=>{});
await wait(800);
const opts = await page.$$eval('#event-select option', o => o.map(x => ({v:x.value,t:x.textContent})));
console.log('events:', opts.slice(0,3), opts.length);
const cpi = opts.find(o => o.t.includes('CPI') && o.t.includes('2026-07')) || opts.find(o=>o.t.includes('CPI')) || opts[0];
await page.select('#event-select', cpi.v);
await wait(500);
await page.select('#symbol-select', 'NVDA').catch(()=>{});
await page.click('#load-button');
await wait(2500);
await page.screenshot({ path: `${out}/research-top.png` });
await page.evaluate(() => window.scrollTo(0, 420));
await wait(500);
await page.screenshot({ path: `${out}/research-chart.png` });
await page.screenshot({ path: `${out}/research-full.png`, fullPage: true });

// Pipelines
await page.goto(base + '/pipelines', { waitUntil: 'networkidle0' });
await wait(1200);
await page.screenshot({ path: `${out}/pipelines.png` });
await page.screenshot({ path: `${out}/pipelines-full.png`, fullPage: true });
const tabs = await page.$$eval('[role=tab], .tab, button', b => b.map(x => x.textContent.trim()).filter(Boolean).slice(0,20));
console.log('tabs', tabs);

// Paper (disabled)
await page.goto(base + '/paper', { waitUntil: 'networkidle0' });
await wait(1200);
await page.screenshot({ path: `${out}/paper.png` });
await browser.close();
