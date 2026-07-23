// Weather data collector for The Lake.
// Runs hourly via GitHub Actions; uses Pacific time to decide what to do.
//
// Tasks (Pacific time):
//   06:00  morning: capture overnight low + fetch 10-day forecasts from all 3 sources;
//                   plan today's wind-alert checks and pick a random check hour
//   15:00  afternoon: capture WU temp/wind (likely afternoon high)
//   any hour matching a planned wind-alert: capture WU
//   the planned random hour: capture WU
//
// Data files (newline-delimited JSON):
//   data/forecasts.jsonl  — one record per (source, target_date) when forecasts captured
//   data/actuals.jsonl    — one record per WU capture
//   data/state.json       — today's plan + last-completed timestamps

import fs from 'node:fs';
import path from 'node:path';

// ── Config ─────────────────────────────────────────────────────────
const LAT = 47.9048;
const LON = -118.3046;
const STATION = 'KWADAVEN5';
// Keys are already public in index.html; harmless to keep here too.
const WEATHERAPI_KEY = process.env.WEATHERAPI_KEY  || '26df06835776479798240519262704';
const WUNDERGROUND_KEY = process.env.WUNDERGROUND_KEY || '442d6039082a4860ad6039082a1860c0';

const WIND_ALERT_THRESHOLD_MPH = 18;   // forecasted wind speed that triggers a check
const RANDOM_CHECK_MIN_HOUR = 8;       // earliest random check hour (PT)
const RANDOM_CHECK_MAX_HOUR = 22;      // latest random check hour (PT)

const FILES = {
  forecasts:   'data/forecasts.jsonl',
  actuals:     'data/actuals.jsonl',
  lakeActuals: 'data/lake_actuals.jsonl',
  state:       'data/state.json',
};

const LAKE_URL = 'https://www.usbr.gov/pn/grandcoulee/lakelevel/';

// ── Time helpers ───────────────────────────────────────────────────
function pacificParts(d = new Date()) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', hour12: false,
  });
  const parts = fmt.formatToParts(d).reduce((a, p) => (a[p.type] = p.value, a), {});
  // hour can be "24" at midnight in some locales; normalize
  let hour = parseInt(parts.hour, 10);
  if (hour === 24) hour = 0;
  return { date: `${parts.year}-${parts.month}-${parts.day}`, hour };
}

function dayDiff(targetISO, baseISO) {
  const a = new Date(targetISO + 'T00:00:00Z'), b = new Date(baseISO + 'T00:00:00Z');
  return Math.round((a - b) / 86400000);
}

// ── File helpers ───────────────────────────────────────────────────
function readJsonOr(file, fallback) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return fallback; }
}
function appendJsonl(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.appendFileSync(file, JSON.stringify(obj) + '\n');
}
function writeJson(file, obj) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(obj, null, 2));
}

// ── Source fetchers ────────────────────────────────────────────────
// Each returns { source, daily: [{date, high_f, low_f, max_wind_mph, max_gust_mph, precip_chance}], hourlyToday: [{hour, wind_mph, gust_mph, temp_f}] }

async function fetchOpenMeteo() {
  const url = 'https://api.open-meteo.com/v1/forecast'
    + `?latitude=${LAT}&longitude=${LON}`
    + '&daily=temperature_2m_max,temperature_2m_min,windspeed_10m_max,windgusts_10m_max,precipitation_probability_max'
    + '&hourly=wind_speed_10m,wind_gusts_10m,temperature_2m'
    + '&temperature_unit=fahrenheit&wind_speed_unit=mph'
    + '&timezone=America%2FLos_Angeles&forecast_days=10';
  const r = await fetch(url);
  if (!r.ok) throw new Error('open-meteo ' + r.status);
  const j = await r.json();
  const daily = j.daily.time.map((d, i) => ({
    date: d,
    high_f: Math.round(j.daily.temperature_2m_max[i]),
    low_f:  Math.round(j.daily.temperature_2m_min[i]),
    max_wind_mph: Math.round(j.daily.windspeed_10m_max[i]),
    max_gust_mph: Math.round(j.daily.windgusts_10m_max[i]),
    precip_chance: j.daily.precipitation_probability_max[i] ?? null,
  }));
  const hourlyToday = [];
  if (j.hourly?.time) {
    const today = daily[0]?.date;
    j.hourly.time.forEach((t, i) => {
      if (!t.startsWith(today)) return;
      const hr = parseInt(t.slice(11, 13), 10);
      hourlyToday.push({
        hour: hr,
        wind_mph: Math.round(j.hourly.wind_speed_10m[i]),
        gust_mph: Math.round(j.hourly.wind_gusts_10m[i]),
        temp_f: Math.round(j.hourly.temperature_2m[i]),
      });
    });
  }
  return { source: 'openmeteo', daily, hourlyToday };
}

async function fetchWeatherAPI() {
  const url = `https://api.weatherapi.com/v1/forecast.json?key=${WEATHERAPI_KEY}&q=${LAT},${LON}&days=10&aqi=no&alerts=no`;
  const r = await fetch(url);
  if (!r.ok) throw new Error('weatherapi ' + r.status);
  const j = await r.json();
  const daily = j.forecast.forecastday.map(d => {
    // The daily block has no gust max, but every hourly record carries
    // gust_mph, so derive the day's peak from those.
    const gusts = (d.hour || []).map(h => h.gust_mph).filter(v => typeof v === 'number');
    return {
      date: d.date,
      high_f: Math.round(d.day.maxtemp_f),
      low_f:  Math.round(d.day.mintemp_f),
      max_wind_mph: Math.round(d.day.maxwind_mph),
      max_gust_mph: gusts.length ? Math.round(Math.max(...gusts)) : null,
      precip_chance: d.day.daily_chance_of_rain ?? null,
    };
  });
  const hourlyToday = (j.forecast.forecastday[0]?.hour || []).map(h => ({
    hour: parseInt(h.time.slice(11, 13), 10),
    wind_mph: Math.round(h.wind_mph),
    gust_mph: Math.round(h.gust_mph),
    temp_f: Math.round(h.temp_f),
  }));
  return { source: 'weatherapi', daily, hourlyToday };
}

async function fetchNWS() {
  // NWS gridpoint forecast API (clean JSON; no scraping).
  const points = await fetch(`https://api.weather.gov/points/${LAT},${LON}`, {
    headers: { 'User-Agent': 'thelake-collector (github.com/509spokane/thelake)' },
  }).then(r => r.json());
  const fcUrl = points?.properties?.forecast;
  if (!fcUrl) throw new Error('nws no forecast url');
  const fc = await fetch(fcUrl, {
    headers: { 'User-Agent': 'thelake-collector (github.com/509spokane/thelake)' },
  }).then(r => r.json());

  // Gusts live in the raw gridpoint data, not the day/night periods above.
  const gustByDate = await fetchNWSGustsByDate(points?.properties?.forecastGridData);
  // Periods alternate day/night. Group by date in Pacific tz.
  const byDate = new Map();
  for (const p of fc.properties.periods) {
    const isoDate = p.startTime.slice(0, 10); // local date in NWS forecast
    if (!byDate.has(isoDate)) byDate.set(isoDate, { high_f: null, low_f: null, max_wind_mph: 0, precip_chance: null });
    const slot = byDate.get(isoDate);
    if (p.isDaytime) slot.high_f = p.temperature;
    else slot.low_f = p.temperature;
    // windSpeed is like "5 to 10 mph"
    const m = String(p.windSpeed || '').match(/(\d+)\s*(?:to\s*(\d+))?/);
    if (m) {
      const v = parseInt(m[2] || m[1], 10);
      if (v > slot.max_wind_mph) slot.max_wind_mph = v;
    }
    if (p.probabilityOfPrecipitation?.value != null) {
      slot.precip_chance = Math.max(slot.precip_chance ?? 0, p.probabilityOfPrecipitation.value);
    }
  }
  const daily = [...byDate.entries()].map(([date, v]) => ({
    date,
    high_f: v.high_f,
    low_f: v.low_f,
    max_wind_mph: v.max_wind_mph || null,
    max_gust_mph: gustByDate.get(date) ?? null,
    precip_chance: v.precip_chance,
  }));
  return { source: 'nws', daily, hourlyToday: [] };
}

// Peak gust per Pacific calendar day, from the NWS gridpoint windGust product.
// Values are intervals ("2026-07-22T22:00:00+00:00/PT3H"), so each one is
// expanded across the hours it covers before bucketing into local days.
// Returns an empty map on any failure — a missing gust is recorded as null
// rather than blocking the rest of the NWS forecast.
async function fetchNWSGustsByDate(gridUrl) {
  const byDate = new Map();
  if (!gridUrl) return byDate;
  try {
    const grid = await fetch(gridUrl, {
      headers: { 'User-Agent': 'thelake-collector (github.com/509spokane/thelake)' },
    }).then(r => r.json());
    const gust = grid?.properties?.windGust;
    const values = gust?.values || [];
    if (!values.length) return byDate;

    // Don't assume the unit — NWS reports km/h here, but say so explicitly.
    const uom = gust.uom || '';
    let toMph;
    if (uom.includes('km_h')) toMph = v => v * 0.621371;
    else if (uom.includes('m_s')) toMph = v => v * 2.236936;
    else if (uom.includes('mi_h')) toMph = v => v;
    else { console.warn(`nws windGust: unrecognized uom "${uom}", skipping gusts`); return byDate; }

    const dayFmt = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/Los_Angeles', year: 'numeric', month: '2-digit', day: '2-digit',
    });

    for (const { validTime, value } of values) {
      if (value == null) continue;
      const [startStr, durStr] = String(validTime).split('/');
      const start = new Date(startStr);
      if (isNaN(start)) continue;
      const d = /P(?:(\d+)D)?(?:T(?:(\d+)H)?)?/.exec(durStr || '') || [];
      const hours = (parseInt(d[1] || 0, 10) * 24) + parseInt(d[2] || 0, 10) || 1;
      const mph = toMph(value);
      for (let h = 0; h < hours; h++) {
        const date = dayFmt.format(new Date(start.getTime() + h * 3600000));
        const prev = byDate.get(date);
        if (prev == null || mph > prev) byDate.set(date, mph);
      }
    }
    for (const [date, mph] of byDate) byDate.set(date, Math.round(mph));
  } catch (e) {
    console.warn('nws gust fetch failed:', e.message);
  }
  return byDate;
}

async function fetchLakeProseReading() {
  const r = await fetch(LAKE_URL);
  if (!r.ok) throw new Error('usbr ' + r.status);
  const html = await r.text();
  const plain = html.replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ');
  const m = plain.match(/elevation of Lake Roosevelt was ([\d.]+) feet above sea level at midnight on ([A-Za-z]+ \d+, \d{4})/i);
  if (!m) return null;
  const elev = parseFloat(m[1]);
  const d = new Date(m[2]);
  if (isNaN(d.getTime())) return null;
  return { date: d.toISOString().slice(0, 10), elevation_ft: elev };
}

async function captureLakeActualIfNew(now) {
  const reading = await safe('usbr', fetchLakeProseReading);
  if (!reading) return;
  // Idempotent: skip if a record for this date already exists. The hourly cron
  // hits this code several times per day; only one record per measurement date.
  try {
    const existing = fs.readFileSync(FILES.lakeActuals, 'utf8').split('\n');
    for (const line of existing) {
      if (!line.trim()) continue;
      try { if (JSON.parse(line).date === reading.date) return; } catch {}
    }
  } catch { /* file doesn't exist yet */ }
  appendJsonl(FILES.lakeActuals, {
    captured_at: now.toISOString(),
    date: reading.date,
    elevation_ft: reading.elevation_ft,
  });
  console.log(`→ captured lake actual: ${reading.date} = ${reading.elevation_ft}'`);
}

async function fetchWU() {
  const url = `https://api.weather.com/v2/pws/observations/current?stationId=${STATION}&format=json&units=e&numericPrecision=decimal&apiKey=${WUNDERGROUND_KEY}`;
  const r = await fetch(url);
  if (!r.ok) throw new Error('wu ' + r.status);
  const j = await r.json();
  const obs = j?.observations?.[0];
  if (!obs) throw new Error('wu no obs');
  const im = obs.imperial;
  return {
    obs_time: obs.obsTimeUtc,
    temp_f: im.temp,
    wind_mph: im.windSpeed,
    gust_mph: im.windGust,
    wind_dir_deg: obs.winddir,
    humidity: obs.humidity,
    precip_today_in: im.precipTotal,
  };
}

async function safe(label, fn) {
  try { return await fn(); }
  catch (e) { console.error(`[${label}] error:`, e.message); return null; }
}

// ── Main ───────────────────────────────────────────────────────────
async function main() {
  const now = new Date();
  const { date: pstDate, hour: pstHour } = pacificParts(now);

  // Reset daily fields if new day
  let state = readJsonOr(FILES.state, {});
  if (state.day !== pstDate) {
    state = {
      day: pstDate,
      morning_done: false,
      afternoon_done: false,
      wind_alerts: [],     // [{hour, predicted_wind_mph, predicted_gust_mph, source, checked}]
      random_hour: null,
      random_done: false,
    };
  }

  console.log(`Pacific ${pstDate} ${pstHour}:00 — state day=${state.day}`);

  // Lake-level snapshot runs every hour but only writes when USBR's prose
  // reports a date we haven't recorded yet.
  await captureLakeActualIfNew(now);

  // Morning routine — 06:00–08:59 PT (window to handle GitHub Actions cron delays)
  if (pstHour >= 6 && pstHour <= 8 && !state.morning_done) {
    console.log('→ morning routine');
    const [om, wa, nws] = await Promise.all([
      safe('openmeteo', fetchOpenMeteo),
      safe('weatherapi', fetchWeatherAPI),
      safe('nws',       fetchNWS),
    ]);
    const sources = [om, wa, nws].filter(Boolean);

    // Save daily forecasts
    for (const src of sources) {
      for (const d of src.daily) {
        appendJsonl(FILES.forecasts, {
          captured_at: now.toISOString(),
          captured_date: pstDate,
          source: src.source,
          target_date: d.date,
          days_out: dayDiff(d.date, pstDate),
          high_f: d.high_f,
          low_f: d.low_f,
          max_wind_mph: d.max_wind_mph,
          max_gust_mph: d.max_gust_mph,
          precip_chance: d.precip_chance,
        });
      }
    }

    // Plan today's wind-alert hours: any hour where any source forecasts wind ≥ threshold
    const alertMap = new Map(); // hour -> {predicted_wind, predicted_gust, source}
    for (const src of sources) {
      for (const h of (src.hourlyToday || [])) {
        if (h.hour < RANDOM_CHECK_MIN_HOUR || h.hour > RANDOM_CHECK_MAX_HOUR) continue;
        if (h.hour <= pstHour) continue; // only future hours
        if (h.wind_mph >= WIND_ALERT_THRESHOLD_MPH || h.gust_mph >= WIND_ALERT_THRESHOLD_MPH + 5) {
          const cur = alertMap.get(h.hour);
          if (!cur || h.gust_mph > (cur.predicted_gust_mph ?? 0)) {
            alertMap.set(h.hour, {
              hour: h.hour,
              predicted_wind_mph: h.wind_mph,
              predicted_gust_mph: h.gust_mph,
              source: src.source,
              checked: false,
            });
          }
        }
      }
    }
    state.wind_alerts = [...alertMap.values()].sort((a, b) => a.hour - b.hour);

    // Pick a random check hour (excluding wind alert hours and the morning hour itself)
    const occupied = new Set([pstHour, 15, ...state.wind_alerts.map(a => a.hour)]);
    const candidates = [];
    for (let h = RANDOM_CHECK_MIN_HOUR; h <= RANDOM_CHECK_MAX_HOUR; h++) {
      if (!occupied.has(h)) candidates.push(h);
    }
    state.random_hour = candidates.length ? candidates[Math.floor(Math.random() * candidates.length)] : null;

    // Capture WU as morning_low reference
    const wu = await safe('wu', fetchWU);
    if (wu) appendJsonl(FILES.actuals, {
      recorded_at: now.toISOString(),
      date: pstDate, hour: pstHour, type: 'morning_low',
      ...wu,
    });

    state.morning_done = true;
    console.log(`  wind_alerts: ${state.wind_alerts.length}, random_hour: ${state.random_hour}`);
  }

  // Afternoon — 14:00–16:59 PT (window to handle GitHub Actions cron delays)
  if (pstHour >= 14 && pstHour <= 16 && !state.afternoon_done) {
    console.log('→ afternoon high check');
    const wu = await safe('wu', fetchWU);
    if (wu) appendJsonl(FILES.actuals, {
      recorded_at: now.toISOString(),
      date: pstDate, hour: pstHour, type: 'afternoon_high',
      ...wu,
    });
    state.afternoon_done = true;
  }

  // Wind alerts
  for (const a of (state.wind_alerts || [])) {
    if (a.hour <= pstHour && !a.checked) {  // <= catches delayed cron runs
      console.log(`→ wind alert check hour=${pstHour} predicted=${a.predicted_wind_mph}/${a.predicted_gust_mph} (${a.source})`);
      const wu = await safe('wu', fetchWU);
      if (wu) appendJsonl(FILES.actuals, {
        recorded_at: now.toISOString(),
        date: pstDate, hour: pstHour, type: 'wind_alert',
        predicted_wind_mph: a.predicted_wind_mph,
        predicted_gust_mph: a.predicted_gust_mph,
        predicted_source: a.source,
        ...wu,
      });
      a.checked = true;
    }
  }

  // Random check
  if (state.random_hour === pstHour && !state.random_done) {
    console.log(`→ random check hour=${pstHour}`);
    const wu = await safe('wu', fetchWU);
    if (wu) appendJsonl(FILES.actuals, {
      recorded_at: now.toISOString(),
      date: pstDate, hour: pstHour, type: 'random',
      ...wu,
    });
    state.random_done = true;
  }

  writeJson(FILES.state, state);
  console.log('done.');
}

main().catch(e => { console.error('fatal:', e); process.exit(1); });
