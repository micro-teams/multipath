import { chromium } from "@playwright/test";
const BASE = "http://127.0.0.1:8931";
const browser = await chromium.launch();
const ctx = await browser.newContext();
const p = await ctx.newPage();
await p.goto(BASE + "/", { waitUntil: "networkidle" });
console.log("controller after first load:", await p.evaluate(() => !!navigator.serviceWorker.controller));
await p.waitForFunction(async () => (await caches.keys()).length > 0, null, { timeout: 15000 });
const keys = await p.evaluate(async () => {
  const c = await caches.open((await caches.keys())[0]);
  return (await c.keys()).map(r => r.url);
});
console.log("cached:", keys);
const matched = await p.evaluate(async () => {
  const c = await caches.open((await caches.keys())[0]);
  const m = await c.match("/");
  return m ? m.status + " " + (await m.text()).slice(0, 40) : "no match for /";
});
console.log("match('/'):", matched);

await ctx.setOffline(true);
const off = await ctx.newPage();
off.on("console", m => console.log("  [offline console]", m.type(), m.text().slice(0,200)));
off.on("pageerror", e => console.log("  [offline pageerror]", String(e).slice(0,200)));
const resp = await off.goto(BASE + "/", { waitUntil: "load" }).catch(e => { console.log("goto threw:", String(e).slice(0,200)); return null; });
console.log("offline response:", resp && resp.status());
console.log("offline controller:", await off.evaluate(() => !!navigator.serviceWorker.controller).catch(e => "n/a"));
try {
  await off.waitForFunction(() => document.querySelector("#root") && document.querySelector("#root").innerHTML.length > 0, null, { timeout: 10000 });
  console.log("offline #root rendered:", await off.$eval("#root", e => e.innerHTML.length), "bytes");
} catch (e) {
  console.log("offline #root stayed empty");
  console.log("offline body:", (await off.content()).slice(0, 600));
}
await browser.close();
