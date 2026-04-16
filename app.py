"""
GTFS Sweden 3 – Incomplete Bus Trips Web Interface
===================================================
Flask app with Cloudflare R2 as the file cache so it can run on Render
(or any stateless host) without losing downloaded archives between restarts.

Local dev  : python app.py   →  http://localhost:5000
Production : gunicorn app:app (Render starts this automatically)
"""

import io
import csv
import zipfile
import datetime
import time
import logging
import collections
import threading
import queue
import json
import os
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import py7zr
import requests
import pandas as pd
from flask import Flask, Response, render_template_string, request
from google.transit import gtfs_realtime_pb2

# ─────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────
STATIC_URL = "https://opendata.samtrafiken.se/gtfs-sweden/sweden.zip?key={key}"
KODA_RT_URL = (
    "https://api.koda.trafiklab.se/KoDa/api/v2/gtfs-rt"
    "/{operator}/{feed}?date={date}&key={key}"
)
REALTIME_OPERATORS = [
    "sl", "ul", "otraf", "jlt", "krono", "klt", "gotland",
    "blekinge", "skane", "varm", "orebro", "vastmanland",
    "dt", "xt", "dintur",
]
OPERATOR_NAMES = {
    "sl": "SL", "ul": "UL", "otraf": "Östgötatrafiken",
    "jlt": "JLT", "krono": "Kronoberg", "klt": "KLT",
    "gotland": "Gotland", "blekinge": "Blekingetrafiken",
    "skane": "Skånetrafiken", "varm": "Värmlandstrafik",
    "orebro": "Örebro", "vastmanland": "Västmanland",
    "dt": "Dalatrafik", "xt": "X-trafik", "dintur": "Din Tur",
}
OCCUPANCY_OPERATORS = {"otraf", "skane"}
OCCUPANCY_LABELS = {
    0: "EMPTY", 1: "MANY_SEATS_AVAILABLE", 2: "FEW_SEATS_AVAILABLE",
    3: "STANDING_ROOM_ONLY", 4: "CRUSHED_STANDING_ROOM_ONLY",
    5: "FULL", 6: "NOT_ACCEPTING_PASSENGERS",
}
ROUTE_TYPE_BUS = {3, 700, 701, 702, 703, 704, 705, 706, 707,
                  708, 709, 710, 711, 712, 713, 714, 715}
POLL_INTERVAL_SEC = 30
MAX_POLL_ATTEMPTS = 40

# ─────────────────────────────────────────────────────────
# R2 CACHE
# Falls back to a local ./gtfs_cache/ folder when R2 env
# vars are not set (i.e. during local development).
# ─────────────────────────────────────────────────────────
R2_ACCOUNT_ID    = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY    = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY    = os.getenv("R2_SECRET_KEY")
R2_BUCKET        = os.getenv("R2_BUCKET", "gtfs-cache")
LOCAL_CACHE_DIR  = Path("./gtfs_cache")

_r2_client = None

def _get_r2():
    global _r2_client
    if _r2_client is None and R2_ACCOUNT_ID and R2_ACCESS_KEY and R2_SECRET_KEY:
        _r2_client = boto3.client(
            service_name="s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="eu",
        )
    return _r2_client


def cache_get(key: str) -> bytes | None:
    """Read a file from R2 (or local cache). Returns None if not found."""
    r2 = _get_r2()
    if r2:
        try:
            obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
            data = obj["Body"].read()
            return data if data else None
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
    else:
        path = LOCAL_CACHE_DIR / key.replace("/", "_")
        if path.exists() and path.stat().st_size > 0:
            return path.read_bytes()
        return None


def cache_put(key: str, data: bytes):
    """Write a file to R2 (or local cache). Silently skips empty data."""
    if not data:
        return
    r2 = _get_r2()
    if r2:
        r2.upload_fileobj(io.BytesIO(data), R2_BUCKET, key)
    else:
        LOCAL_CACHE_DIR.mkdir(exist_ok=True)
        path = LOCAL_CACHE_DIR / key.replace("/", "_")
        path.write_bytes(data)


def cache_exists(key: str) -> bool:
    """Check whether a key exists in the cache (including empty sentinel)."""
    r2 = _get_r2()
    if r2:
        try:
            r2.head_object(Bucket=R2_BUCKET, Key=key)
            return True
        except ClientError:
            return False
    else:
        path = LOCAL_CACHE_DIR / key.replace("/", "_")
        return path.exists()


# ─────────────────────────────────────────────────────────
# FLASK APP
# ─────────────────────────────────────────────────────────
app = Flask(__name__)
_job_lock    = threading.Lock()
_job_running = False
_log_queue: queue.Queue = queue.Queue()
_results: dict = {}

# ─────────────────────────────────────────────────────────
# HTML TEMPLATE
# ─────────────────────────────────────────────────────────
HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GTFS Sweden 3 – Incomplete Bus Trips</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #f4f6f9; color: #1a1a2e; }
  header { background: #1a1a2e; color: #fff; padding: 1.2rem 2rem; }
  header h1 { font-size: 1.3rem; font-weight: 600; }
  header p  { font-size: .85rem; opacity: .7; margin-top: .2rem; }
  main { max-width: 1100px; margin: 2rem auto; padding: 0 1.5rem; }
  .card { background: #fff; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,.08);
          padding: 1.5rem; margin-bottom: 1.5rem; }
  .card h2 { font-size: 1rem; font-weight: 600; margin-bottom: 1rem;
             border-bottom: 2px solid #e8ecf0; padding-bottom: .5rem; }
  label  { display: block; font-size: .85rem; font-weight: 500; margin-bottom: .3rem; }
  input[type=text], input[type=password], input[type=date] {
    width: 100%; padding: .55rem .75rem; border: 1px solid #d0d7de;
    border-radius: 6px; font-size: .9rem; margin-bottom: 1rem; }
  input:focus { outline: none; border-color: #3b82f6; box-shadow: 0 0 0 3px rgba(59,130,246,.15); }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  .ops-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(170px,1fr)); gap: .5rem; }
  .ops-grid label { display: flex; align-items: center; gap: .4rem; font-weight: 400;
                    font-size: .85rem; cursor: pointer; margin: 0; }
  button { padding: .65rem 1.6rem; border: none; border-radius: 7px; font-size: .95rem;
           font-weight: 600; cursor: pointer; transition: background .15s; }
  .btn-primary { background: #3b82f6; color: #fff; }
  .btn-primary:hover { background: #2563eb; }
  .btn-primary:disabled { background: #93c5fd; cursor: not-allowed; }
  .btn-secondary { background: #e8ecf0; color: #374151; margin-left: .5rem; }
  .btn-secondary:hover { background: #d1d5db; }
  #log-box { background: #0f172a; color: #94a3b8; font-family: monospace;
             font-size: .8rem; padding: 1rem; border-radius: 6px; height: 280px;
             overflow-y: auto; white-space: pre-wrap; word-break: break-all; }
  #log-box .info  { color: #7dd3fc; }
  #log-box .warn  { color: #fbbf24; }
  #log-box .error { color: #f87171; }
  #log-box .ok    { color: #4ade80; }
  #status-bar { display: flex; align-items: center; gap: .75rem; margin-bottom: .75rem; }
  .badge { padding: .2rem .7rem; border-radius: 999px; font-size: .78rem; font-weight: 600; }
  .badge-idle    { background: #e8ecf0; color: #6b7280; }
  .badge-running { background: #fef3c7; color: #d97706; }
  .badge-done    { background: #d1fae5; color: #065f46; }
  .badge-error   { background: #fee2e2; color: #991b1b; }
  progress { width: 100%; height: 8px; border-radius: 4px; margin-bottom: .5rem; }
  table { width: 100%; border-collapse: collapse; font-size: .85rem; }
  th { background: #f1f5f9; text-align: left; padding: .6rem .75rem;
       border-bottom: 2px solid #e2e8f0; white-space: nowrap; }
  td { padding: .55rem .75rem; border-bottom: 1px solid #f1f5f9; }
  tr:hover td { background: #f8fafc; }
  .num { text-align: right; }
  .downloads { display: flex; gap: .75rem; margin-top: 1rem; }
  #results-section { display: none; }
  .select-all-btn { font-size: .8rem; color: #3b82f6; cursor: pointer;
                    background: none; border: none; padding: 0; margin-bottom: .5rem;
                    text-decoration: underline; }
</style>
</head>
<body>
<header>
  <h1>GTFS Sweden 3 — Incomplete Bus Trips Analyser</h1>
  <p>Identifies bus trips that skipped planned stops · KoDa + GTFS Sweden 3</p>
</header>
<main>
  <div class="card">
    <h2>Configuration</h2>
    <div class="grid2">
      <div>
        <label>GTFS Sweden 3 Static API Key</label>
        <input type="password" id="static-key" placeholder="Paste your key here">
      </div>
      <div>
        <label>KoDa API Key</label>
        <input type="password" id="koda-key" placeholder="Paste your key here">
      </div>
      <div>
        <label>Start Date</label>
        <input type="date" id="start-date" value="2026-03-01">
      </div>
      <div>
        <label>End Date</label>
        <input type="date" id="end-date" value="2026-03-31">
      </div>
    </div>
    <label>Operators to analyse</label>
    <button class="select-all-btn" onclick="toggleAll()">Select / deselect all</button>
    <div class="ops-grid" id="ops-grid"></div>
  </div>

  <div class="card">
    <div id="status-bar">
      <span id="badge" class="badge badge-idle">Idle</span>
      <span id="status-text" style="font-size:.85rem;color:#6b7280">Ready to run.</span>
    </div>
    <progress id="progress-bar" value="0" max="100" style="display:none"></progress>
    <div id="log-box"></div>
    <div style="margin-top:1rem">
      <button class="btn-primary" id="run-btn" onclick="runAnalysis()">▶ Run Analysis</button>
      <button class="btn-secondary" onclick="clearLog()">Clear log</button>
    </div>
  </div>

  <div class="card" id="results-section">
    <h2>Results — Summary by Operator</h2>
    <div id="summary-table"></div>
    <div class="downloads">
      <button class="btn-secondary" onclick="location.href='/download/summary'">⬇ Download Summary CSV</button>
      <button class="btn-secondary" onclick="location.href='/download/detail'">⬇ Download Detail CSV</button>
    </div>
  </div>
</main>

<script>
const OPERATORS = {{ operators_json|safe }};
let allSelected = true;
let evtSource = null;

const grid = document.getElementById('ops-grid');
OPERATORS.forEach(([code, name]) => {
  const lbl = document.createElement('label');
  lbl.innerHTML = `<input type="checkbox" class="op-cb" value="${code}" checked> ${name} <small style="color:#9ca3af">(${code})</small>`;
  grid.appendChild(lbl);
});

function toggleAll() {
  allSelected = !allSelected;
  document.querySelectorAll('.op-cb').forEach(cb => cb.checked = allSelected);
}

function selectedOps() {
  return [...document.querySelectorAll('.op-cb:checked')].map(cb => cb.value);
}

function setStatus(state, text) {
  const badge = document.getElementById('badge');
  badge.className = 'badge badge-' + state;
  badge.textContent = { idle:'Idle', running:'Running…', done:'Done', error:'Error' }[state] || state;
  document.getElementById('status-text').textContent = text;
}

function appendLog(msg, cls) {
  const box = document.getElementById('log-box');
  const line = document.createElement('span');
  if (cls) line.className = cls;
  line.textContent = msg + '\\n';
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}

function clearLog() { document.getElementById('log-box').innerHTML = ''; }

function runAnalysis() {
  const staticKey = document.getElementById('static-key').value.trim();
  const kodaKey   = document.getElementById('koda-key').value.trim();
  const startDate = document.getElementById('start-date').value;
  const endDate   = document.getElementById('end-date').value;
  const ops       = selectedOps();

  if (!staticKey || !kodaKey) { alert('Please enter both API keys.'); return; }
  if (ops.length === 0)        { alert('Please select at least one operator.'); return; }

  document.getElementById('run-btn').disabled = true;
  document.getElementById('results-section').style.display = 'none';
  document.getElementById('progress-bar').style.display = 'block';
  document.getElementById('progress-bar').value = 0;
  clearLog();
  setStatus('running', 'Starting analysis…');

  if (evtSource) evtSource.close();

  const params = new URLSearchParams({
    static_key: staticKey, koda_key: kodaKey,
    start_date: startDate, end_date: endDate,
    operators: ops.join(',')
  });

  evtSource = new EventSource('/run?' + params.toString());

  evtSource.addEventListener('log', e => {
    const d = JSON.parse(e.data);
    appendLog(d.msg, d.level === 'WARNING' ? 'warn' : d.level === 'ERROR' ? 'error' : 'info');
  });

  evtSource.addEventListener('progress', e => {
    const d = JSON.parse(e.data);
    document.getElementById('progress-bar').value = d.pct;
    setStatus('running', d.msg);
  });

  evtSource.addEventListener('done', e => {
    const d = JSON.parse(e.data);
    evtSource.close();
    document.getElementById('run-btn').disabled = false;
    document.getElementById('progress-bar').value = 100;
    if (d.error) {
      setStatus('error', d.error);
      appendLog('ERROR: ' + d.error, 'error');
    } else {
      setStatus('done', `Complete — ${d.total_incidents.toLocaleString()} incidents across ${d.unique_trips.toLocaleString()} trips.`);
      appendLog('✓ Analysis complete.', 'ok');
      renderSummary(d.summary);
    }
  });

  evtSource.onerror = () => {
    evtSource.close();
    document.getElementById('run-btn').disabled = false;
    setStatus('error', 'Connection lost. Check the terminal for errors.');
  };
}

function renderSummary(rows) {
  const section = document.getElementById('results-section');
  section.style.display = 'block';
  const cols = [
    ['operator',               'Operator'],
    ['agency_name',            'Agency'],
    ['total_incomplete_trips', 'Incomplete Trips'],
    ['total_skipped_stops',    'Total Skipped Stops'],
    ['avg_skipped_per_trip',   'Avg Skipped / Trip'],
    ['days_with_issues',       'Days with Issues'],
  ];
  const numCols = new Set(['total_incomplete_trips','total_skipped_stops','avg_skipped_per_trip','days_with_issues']);
  let html = '<table><thead><tr>';
  cols.forEach(([,label]) => html += `<th>${label}</th>`);
  html += '</tr></thead><tbody>';
  rows.forEach(row => {
    html += '<tr>';
    cols.forEach(([key]) => {
      const cls = numCols.has(key) ? ' class="num"' : '';
      let val = row[key];
      if (key === 'avg_skipped_per_trip') val = parseFloat(val).toFixed(2);
      html += `<td${cls}>${val}</td>`;
    });
    html += '</tr>';
  });
  html += '</tbody></table>';
  document.getElementById('summary-table').innerHTML = html;
  section.scrollIntoView({ behavior: 'smooth' });
}
</script>
</body>
</html>
"""

# ─────────────────────────────────────────────────────────
# ANALYSIS CORE
# ─────────────────────────────────────────────────────────

def _read_csv_from_zip(zf, filename):
    with zf.open(filename) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))


def _build_static_index(zf, emit):
    emit("Parsing routes.txt …")
    routes = _read_csv_from_zip(zf, "routes.txt")
    bus_route_ids = {r["route_id"] for r in routes if int(r.get("route_type", -1)) in ROUTE_TYPE_BUS}
    route_to_agency = {r["route_id"]: r["agency_id"] for r in routes}

    emit("Parsing agency.txt …")
    agencies = _read_csv_from_zip(zf, "agency.txt")
    agency_names = {a["agency_id"]: a["agency_name"] for a in agencies}

    emit("Parsing trips.txt …")
    trips = _read_csv_from_zip(zf, "trips.txt")
    bus_trips     = [t for t in trips if t["route_id"] in bus_route_ids]
    bus_trip_ids  = {t["trip_id"] for t in bus_trips}
    trip_to_route = {t["trip_id"]: t["route_id"] for t in bus_trips}
    emit(f"Found {len(bus_route_ids):,} bus routes, {len(bus_trip_ids):,} bus trips.")

    emit("Parsing stop_times.txt (may take a minute) …")
    planned_stops = collections.defaultdict(list)
    with zf.open("stop_times.txt") as f:
        for row in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
            tid = row["trip_id"]
            if tid in bus_trip_ids:
                planned_stops[tid].append(int(row["stop_sequence"]))
    for tid in planned_stops:
        planned_stops[tid].sort()
    emit(f"Indexed stop_times for {len(planned_stops):,} bus trips.")
    return bus_trip_ids, planned_stops, trip_to_route, route_to_agency, agency_names


def _fetch_archive(operator, date, feed, static_key, koda_key, emit):
    cache_key = f"koda_{operator}_{feed}_{date}.7z"

    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    # Empty sentinel means we already know there's no data for this combo
    if cache_exists(cache_key):
        return None

    url = KODA_RT_URL.format(operator=operator, feed=feed, date=date.isoformat(), key=koda_key)

    for attempt in range(MAX_POLL_ATTEMPTS):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 202:
                if attempt == 0:
                    emit(f"  KoDa building archive for {operator} {date} ({feed}), polling …")
                time.sleep(POLL_INTERVAL_SEC)
                continue
            if r.status_code == 404:
                cache_put(cache_key, b"")   # empty sentinel
                return None
            r.raise_for_status()
            raw = r.content
            cache_put(cache_key, raw)
            time.sleep(0.3)
            return raw
        except requests.RequestException as e:
            emit(f"  Request error {operator} {date}: {e}", level="WARNING")
            return None

    emit(f"  Gave up polling {operator} {date} {feed}", level="WARNING")
    return None


def _fetch_static(static_key, emit):
    cache_key = "sweden.zip"
    cached = cache_get(cache_key)
    if cached:
        emit("Using cached static GTFS.")
        return zipfile.ZipFile(io.BytesIO(cached))

    emit("Downloading static GTFS Sweden 3 (this may take a few minutes) …")
    r = requests.get(STATIC_URL.format(key=static_key), timeout=300, stream=True)
    r.raise_for_status()
    buf = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1 << 20):
        buf.write(chunk)
    raw = buf.getvalue()
    cache_put(cache_key, raw)
    emit("Static GTFS downloaded and cached.")
    return zipfile.ZipFile(io.BytesIO(raw))


def _extract_pbs(raw_bytes):
    results = []
    try:
        with py7zr.SevenZipFile(io.BytesIO(raw_bytes), mode="r") as arc:
            for fname, data in arc.read().items():
                if fname.endswith(".pb"):
                    results.append(data.read())
    except Exception:
        pass
    return results


def _parse_feeds(raw):
    feeds = []
    if not raw:
        return feeds
    for pb in _extract_pbs(raw):
        msg = gtfs_realtime_pb2.FeedMessage()
        try:
            msg.ParseFromString(pb)
            feeds.append(msg)
        except Exception:
            pass
    return feeds


def _occupancy_index(vp_feeds):
    occ = {}
    for feed in vp_feeds:
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            v = entity.vehicle
            if v.HasField("trip") and v.trip.trip_id:
                occ[v.trip.trip_id] = OCCUPANCY_LABELS.get(
                    v.occupancy_status, f"UNKNOWN({v.occupancy_status})"
                )
    return occ


def _analyse(feeds, planned_stops, bus_trip_ids, occupancy):
    SKIPPED = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
    seen = {}
    for feed in feeds:
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update
            tid = tu.trip.trip_id
            if tid not in bus_trip_ids or tid in seen:
                continue
            planned = planned_stops.get(tid)
            if not planned:
                continue
            skipped = [s.stop_sequence for s in tu.stop_time_update if s.schedule_relationship == SKIPPED]
            if skipped:
                seen[tid] = {
                    "trip_id":          tid,
                    "planned_stops":    len(planned),
                    "skipped_stops":    len(skipped),
                    "skipped_seqs":     ";".join(map(str, sorted(skipped))),
                    "occupancy_status": (occupancy or {}).get(tid, "N/A"),
                }
    return list(seen.values())


def _date_range(start, end):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def _run_analysis(static_key, koda_key, start_date, end_date, operators, log_q):
    def emit(msg, level="INFO"):
        log_q.put({"type": "log", "level": level, "msg": msg})

    def progress(pct, msg):
        log_q.put({"type": "progress", "pct": pct, "msg": msg})

    try:
        zf = _fetch_static(static_key, emit)
        bus_trip_ids, planned_stops, trip_to_route, route_to_agency, agency_names = (
            _build_static_index(zf, emit)
        )
        progress(5, "Static GTFS indexed.")

        all_days      = list(_date_range(start_date, end_date))
        total_steps   = len(operators) * len(all_days)
        step          = 0
        all_incidents = []

        for operator in operators:
            emit(f"── Operator: {OPERATOR_NAMES.get(operator, operator)} ({operator}) ──")
            has_occ = operator in OCCUPANCY_OPERATORS

            for date in all_days:
                step += 1
                pct   = 5 + int(step / total_steps * 93)
                progress(pct, f"{OPERATOR_NAMES.get(operator, operator)} — {date}")

                raw_tu   = _fetch_archive(operator, date, "TripUpdates", static_key, koda_key, emit)
                tu_feeds = _parse_feeds(raw_tu)

                occupancy = None
                if has_occ:
                    raw_vp   = _fetch_archive(operator, date, "VehiclePositions", static_key, koda_key, emit)
                    occupancy = _occupancy_index(_parse_feeds(raw_vp))

                incidents = _analyse(tu_feeds, planned_stops, bus_trip_ids, occupancy)
                for inc in incidents:
                    inc["operator"] = operator
                    inc["date"]     = date.isoformat()
                all_incidents.extend(incidents)

        progress(99, "Building results …")

        if not all_incidents:
            log_q.put({"type": "done", "error": "No incomplete trips found. Check your API keys."})
            return

        df = pd.DataFrame(all_incidents)
        df["route_id"]    = df["trip_id"].map(trip_to_route)
        df["agency_id"]   = df["route_id"].map(route_to_agency)
        df["agency_name"] = df["agency_id"].map(agency_names).fillna("Unknown")

        detail_cols = ["date","operator","agency_name","trip_id","route_id",
                       "planned_stops","skipped_stops","skipped_seqs","occupancy_status"]
        _results["detail"]  = df[detail_cols].sort_values(["date","agency_name","trip_id"])

        summary = (
            df.groupby(["operator","agency_name"])
            .agg(
                total_incomplete_trips=("trip_id",      "count"),
                total_skipped_stops   =("skipped_stops", "sum"),
                avg_skipped_per_trip  =("skipped_stops", "mean"),
                days_with_issues      =("date",          "nunique"),
            )
            .reset_index()
            .sort_values("total_incomplete_trips", ascending=False)
        )
        _results["summary"] = summary

        log_q.put({
            "type":            "done",
            "total_incidents":  len(df),
            "unique_trips":     df["trip_id"].nunique(),
            "summary":          summary.to_dict(orient="records"),
        })

    except Exception as e:
        import traceback
        emit(traceback.format_exc(), level="ERROR")
        log_q.put({"type": "done", "error": str(e)})
    finally:
        global _job_running
        with _job_lock:
            _job_running = False


# ─────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────

@app.route("/")
def index():
    ops_json = json.dumps([[c, OPERATOR_NAMES[c]] for c in REALTIME_OPERATORS])
    return render_template_string(HTML, operators_json=ops_json)


@app.route("/run")
def run():
    global _job_running, _log_queue
    with _job_lock:
        if _job_running:
            return Response("Job already running", status=409)
        _job_running = True
        _log_queue   = queue.Queue()

    static_key = request.args.get("static_key", "")
    koda_key   = request.args.get("koda_key",   "")
    start_date = datetime.date.fromisoformat(request.args.get("start_date", "2026-03-01"))
    end_date   = datetime.date.fromisoformat(request.args.get("end_date",   "2026-03-31"))
    operators  = [o for o in request.args.get("operators", "").split(",") if o in REALTIME_OPERATORS]

    threading.Thread(
        target=_run_analysis,
        args=(static_key, koda_key, start_date, end_date, operators, _log_queue),
        daemon=True,
    ).start()

    def stream():
        while True:
            try:
                item = _log_queue.get(timeout=60)
            except queue.Empty:
                yield "event: ping\ndata: {}\n\n"
                continue
            if item["type"] == "log":
                yield f"event: log\ndata: {json.dumps({'msg': item['msg'], 'level': item['level']})}\n\n"
            elif item["type"] == "progress":
                yield f"event: progress\ndata: {json.dumps({'pct': item['pct'], 'msg': item['msg']})}\n\n"
            elif item["type"] == "done":
                yield f"event: done\ndata: {json.dumps(item)}\n\n"
                break

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/download/<kind>")
def download(kind):
    if kind not in _results:
        return "No results yet", 404
    buf = io.StringIO()
    _results[kind].to_csv(buf, index=False)
    buf.seek(0)
    fname = f"{'summary_by_operator' if kind == 'summary' else 'incomplete_bus_trips'}_march_2026.csv"
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


# ─────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    LOCAL_CACHE_DIR.mkdir(exist_ok=True)
    print("\n  GTFS Incomplete Trips Analyser")
    print("  Open http://localhost:5000 in your browser\n")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
