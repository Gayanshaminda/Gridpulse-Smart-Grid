// Optional UI verification with Playwright and a locally installed Edge browser.
const { chromium } = require('playwright');
const fs = require('fs');
(async () => {
  fs.mkdirSync('output/screenshots', {recursive:true});
  const browser = await chromium.launch({channel:'msedge', headless:true});
  const page = await browser.newPage({viewport:{width:1440,height:1500}});
  const errors = [];
  page.on('pageerror', e => errors.push(e.message));
  await page.goto(process.env.DASHBOARD_URL || 'http://127.0.0.1:8502');
  await page.getByText('Your grid, in focus.', {exact:true}).waitFor({timeout:120000});
  await page.locator('[data-testid="stPlotlyChart"]').first().waitFor({timeout:120000});
  await page.getByText('Highest grid demand', {exact:true}).waitFor();
  await page.locator('[data-testid="stDataFrame"]').first().waitFor();
  await page.locator('[data-testid="stDataFrame"]').first().scrollIntoViewIfNeeded();
  await page.waitForTimeout(1500); // Allow the canvas-based table to paint after mounting.
  await page.screenshot({path:'output/screenshots/live-energy.png',fullPage:true});
  await page.getByText('Daily billing', {exact:true}).click();
  await page.getByText('Every unit, accounted for.', {exact:true}).waitFor();
  await page.getByText('Your grid, in focus.', {exact:true}).waitFor({state:'hidden'});
  await page.getByText('Estimated bills (LKR)', {exact:true}).waitFor();
  await page.getByText('Download daily report', {exact:true}).waitFor();
  await page.screenshot({path:'output/screenshots/daily-billing.png',fullPage:true});
  await page.getByText('Pipeline health', {exact:true}).click();
  await page.getByText('A healthy flow of data.', {exact:true}).waitFor();
  await page.getByText('Accepted readings', {exact:true}).waitFor();
  await page.getByText('Every unit, accounted for.', {exact:true}).waitFor({state:'hidden'});
  await page.getByText('Processing checkpoints', {exact:true}).waitFor();
  await page.screenshot({path:'output/screenshots/pipeline-health.png',fullPage:true});
  if (errors.length) throw new Error(errors.join('\n'));
  console.log('All three dashboard pages rendered without browser errors.');
  await browser.close();
})().catch(e => {console.error(e);process.exit(1)});
