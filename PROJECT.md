# Lake Roosevelt — project reference

A single-file weather / wind / water-level dashboard for **Lake Roosevelt at
Two Rivers Marina / Fort Spokane, WA**. Built as static HTML, hosted on GitHub
Pages, with an hourly GitHub Action that collects forecast-accuracy data.

## Where things live

| What | Path |
|---|---|
| Main dashboard | `index.html` (single file — HTML + CSS + vanilla JS, ~138 KB) |
| Forecast-accuracy page | `accuracy.html` |
| Hourly data collector | `scripts/collect.mjs` (Node, run by GitHub Action) |
| Collector schedule | `.github/workflows/collect.yml` (cron `5 * * * *`, uses built-in `GITHUB_TOKEN`) |
| Collected data (append-only JSONL) | `data/forecasts.jsonl`, `data/actuals.jsonl`, `data/lake_actuals.jsonl`, `data/state.json` |
| Header photo | `images/boats.jpg` |
| Air-quality logo icon (unused after icons removed) | `images/air.png` |

## Hosting, repo, deploy

- **GitHub repo:** `509spokane/thelake` — remote is **SSH** (`git@github.com:509spokane/thelake.git`).
- **Live site:** https://509spokane.github.io/thelake/  (accuracy page: `/accuracy.html`)
- **Deploy = push to `main`.** GitHub Pages rebuilds automatically; propagation
  typically 30–90 s. Verify a deploy landed by curling the raw file for a
  unique marker string, e.g.
  `curl -s https://509spokane.github.io/thelake/index.html | grep -c "someMarker"`.
- **Cache-bust the browser** by appending `?v=NN` to the URL when re-checking.
- The hourly collector commits `data/…` to `main` too, so **pulls often need a
  rebase before push**: `git pull --rebase origin main && git push origin main`.
- Auth: pushes go over SSH (verified working). The Action uses the automatic
  per-run `GITHUB_TOKEN`. **Neither uses a personal access token** — the
  expired `thelake2` PAT the owner got an email about is unrelated to this repo.

## Location & coordinates

- Request point for all sources: **47.9048, −118.3046** (Fort Spokane / Two Rivers).
- Home marina (memory): 47.90523, −118.32188.
- Dock weather station (ground truth for accuracy): **Weather Underground PWS
  `KWADAVEN5`**, labeled **"F-Dock"**, at the waterline ~1,287 ft.
- Each forecast source snaps the request to its own grid, so they forecast
  slightly different places, all *above* the lake on dry ground — the likely
  reason all three run warm vs the dock reading.

## Forecast sources (user-selectable, persisted in localStorage `forecastSource`)

| Key | Label | Notes |
|---|---|---|
| `weatherapi` | WeatherAPI.com | **Default.** Free tier = 3-day forecast only. Weakest for wind (badly under-forecast the 8/1 Red Flag event: 23 vs NWS 38 / alert 50). |
| `openmeteo` | Open-Meteo | 10-day, no key, detailed precip/gusts. Also the **wind fallback** for all sources. |
| `nws` | NWS (weather.gov) | Official; alerts; gridpoint gust product. Fetched every load regardless of selection (see gotcha). |

### API keys (already public in `index.html`)
- `WEATHERAPI_KEY = '26df06835776479798240519262704'`
- `WUNDERGROUND_KEY = '442d6039082a4860ad6039082a1860c0'` (station `KWADAVEN5`)
- `OPENWEATHER_KEY` is a placeholder — OpenWeather source is effectively unused.
- Same LAT/LON/STATION/keys are duplicated in `scripts/collect.mjs`.

## Page structure (top → bottom)

1. **Hero header** — title "Lake Roosevelt" / "Fort Spokane/Two Rivers" over
   `images/boats.jpg`, photo masked to fade out its top third so text sits on
   dark sky. Full-bleed (`100vw`) on phones, capped at card width ≥800px.
   Revert commit for the hero design if ever wanted: `git revert b6ea210`.
2. **Two map links** under the header (plain text, icons removed):
   *Fire & smoke map ↗* → `https://fire.airnow.gov/#8/47.905/-118.322`
   (deep-links to marina; `#zoom/lat/lon`), and *Air quality map ↗* →
   `https://www.airnow.gov/?city=Fort%20Spokane&state=WA&country=USA`.
3. **Conditions Summary** card — text only, centered, one line per group:
   `High Temps  Avg : 86°. Peaking: 87° on Tue, the 28th`
   `Wind  Avg: 5 mph. Gusting to 14 mph at 2pm & 6pm on Tue, the 28th`
   Dates are ordinals ("the 28th"); day+date wrapped in `white-space:nowrap`.
4. **Temperature and Wind** card — the temp chart (Summary / Columns toggle)
   and the wind chart (Summary / Detailed toggle) + source labels.
5. **Lake Level** card — mini elevation curve; each day shows the +/- change
   from the previous day (green rise / orange drop), to a tenth of a foot.
6. **Weather Sources** collapsible section, alert banner (collapsed, shows
   category words like "(fire, wind)"), font-size picker, GoatCounter.

## Key JS conventions in `index.html`

- **Font scaling:** CSS custom property `--fs` (`:root` 1.25, `lg` 1.58,
  `xl` 2.05). SVG text can't read a CSS var, so charts scale via JS `_svgScale`
  read from `SVG_SCALES = {normal:1.25, lg:1.62, xl:2.15}` — **keep the two in
  step.** Font picker labels: Normal / "I forgot my readers" / "I have my
  readers and still can't read anything".
- **Wind chart is 48 hours**, floored to the current hour so an in-progress
  windy hour stays until it passes.
- **Windy-run marking:** relative threshold (near window max) **capped by an
  absolute floor of 14.5 mph** so a soon event isn't hidden behind a bigger
  later gust; gusts rounding to 15+ always get dots + a time-span label.
- **`renderWindFrom(source, hourly)`** gates which source draws the wind chart
  so fetch arrival order can't put the wrong source under the label.
- **Summary chart text size** is matched to the detailed chart via
  `wFS(base)` = convert 560-unit viewBox to on-screen px using container width.

## Alert-gust overlay (recent major feature)

`fetchAlerts()` parses active NWS alerts into `_alertGusts` and the charts draw
a **red line at the alert's gust level** across the alert's time window —
because alerts routinely forecast stronger wind than the gridded models
(8/1 Red Flag: alert 50 vs forecast 23–38). Chart y-scale stretches to fit.

- Gust parsed from description: `/gusts?\s+(?:up to|of|to|as high as)?\s*(?:(\d+)\s*to\s*)?(\d+)\s*mph/i`.
- Window from structured `onset`/`ends` (falls back to `effective`/`expires`).
- **PDS core:** "Particularly Dangerous Situation" alerts name a tighter core
  window in free text ("PDS RED FLAG IN EFFECT 2PM UNTIL 8PM"). Parsed with a
  PDS regex, clamped inside the alert window, drawn as a **solid 4px segment**
  over the dashed span. Label stacks: `Alert: gusts to 50 mph` on top,
  `worst 2pm–8pm` below.
- Overlay code is shared by both charts: `alertGustOverlay()` /
  `alertGustMax()`, plus `_alertGusts` populated in `fetchAlerts()`.

**PDS vs PDT:** PDS = Particularly Dangerous Situation (weather severity tier).
PDT = Pacific Daylight Time (the timezone, UTC−7). One letter apart, unrelated.

## Collector (`scripts/collect.mjs`) & accuracy page

- Runs hourly; decides actions by **Pacific time windows** (not exact hours,
  because Actions cron can lag 15–45 min):
  - 6–8 AM: capture overnight low + fetch 10-day forecasts from all 3 sources;
    plan wind-alert check hours (≥18 mph) + one random check hour.
  - 2–4 PM: capture the afternoon high.
  - planned wind-alert hours + the random hour: capture WU.
- `data/actuals.jsonl` ground truth is WU `KWADAVEN5`. Types: `afternoon_high`,
  `morning_low`, `wind_alert`, `random`.
- **Gusts now recorded for all three sources** (was null for weatherapi/nws):
  weatherapi derives daily max from its hourly `gust_mph`; NWS pulls the
  gridpoint `windGust` product (unit read from response, expanded across
  interval hours). Existing historical rows stay null — gust accuracy only
  becomes meaningful after ~2 weeks of new collection.
- accuracy.html sections: plain-English leaderboard verdict (WeatherAPI is #1
  for **high temp**, ~4.7° avg error; NWS is #1 for **wind**); today's high
  vs actual; **recent-days table is hidden** (overflowed phones); accuracy by
  days-out **color-coded** best=green/mid=amber/worst=red, cells with <8 days
  greyed; wind-alert reality-check summary (all alerts came from Open-Meteo
  because collector logs only the highest gust/hr — can't compare sources yet);
  worst-misses summary. GoatCounter tag added to accuracy.html too.

## Known gotchas / decisions

- **Only render the column grid (`#waterLevelGrid`) for the selected source.**
  NWS `fetchWeather()` used to call `mergeAndRenderWaterLevelBg()` on every load
  and clobbered the grid with NWS data even when WeatherAPI was selected — made
  the temp Columns view disagree with the Summary. Fixed; keep all grid renders
  guarded on `_forecastSource`.
- `#waterLevelSection` (the old 10-day grid card) is **hidden, not deleted** —
  the temp "Columns" toggle copies day columns out of `#waterLevelGrid`.
- WU and AirNow APIs are **CORS-blocked** in the browser → any live use needs
  the collector (server-side) or a fallback. Open-Meteo (weather + air quality)
  is CORS-open and keyless.
- **Air quality tracking** was discussed but NOT built. If wanted: Open-Meteo
  Air Quality API is keyless, marina-exact, returns `past_days` so a trend is
  available immediately (approach A, no storage). AirNow would need a user-
  generated key + collector storage + gives only a distant-monitor value.

## Working preferences (this project)

- Commit before editing; commit messages end with the Claude co-author trailer.
- After a change that's visible in the browser: push, wait for GitHub Pages,
  then verify live in the browser (measure the DOM, don't just eyeball). The
  in-app preview browser sometimes renders offset/blank — prefer reading DOM
  values via `javascript_tool` over screenshots when a screenshot looks wrong.
- Syntax-check `index.html`'s script block after edits (wrap in `new Function`).
