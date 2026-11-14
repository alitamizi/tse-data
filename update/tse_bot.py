# -*- coding: utf-8 -*-
"""
ربات داده‌های لحظه‌ای بورس تهران (tsetmc.com) — نسخهٔ 3

مسیرهای انتقال داده (همه هم‌زمان فعال‌اند؛ اگر یکی قطع شد بقیه کار می‌کنند):
  1) تونل Cloudflare         (اتصال زنده، تأخیر چندثانیه‌ای)
  2) تونل localhost.run      (اتصال زنده، پشتیبان)
  3) تونل Pinggy             (اتصال زنده، پشتیبان)
  4) مخزن GitHub             (همیشه؛ هر ~۳۰ ثانیه + دفترچهٔ آدرس تونل‌ها)

همهٔ زمان‌ها با استاندارد ISO 8601: «...Z» یعنی UTC، و «+03:30» یعنی وقت تهران.
ساعت سیستم با ساعت سرور GitHub هم‌تراز می‌شود تا اگر ساعت کامپیوتر غلط بود، زمان‌ها غلط نشوند.

تنظیمات در config.json کنار برنامه:
  {"github_repo": "user/repo", "github_token": "github_pat_...", "interval_seconds": 30,
   "tunnels": ["cloudflared", "localhost.run", "pinggy"]}
"""

import argparse
import bisect
import lzma
import base64
import gzip
import atexit
import csv
import email.utils
import hashlib
import io
import json
import logging
import os
import platform
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests

VERSION = "4.8.3"
BASE = "https://cdn.tsetmc.com/api"
GH_API = "https://api.github.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.tsetmc.com/",
    "Origin": "https://www.tsetmc.com",
}
TEHRAN = timezone(timedelta(hours=3, minutes=30))   # ایران از ۱۴۰۱ تغییر ساعت تابستانی ندارد
LIVE_PORT = 47821            # پورت محلی سرور زنده (همچنین جلوی اجرای هم‌زمان دو نسخه را می‌گیرد)
LIVE_MAX_AGE = 5             # در اتصال زنده، داده‌ی قدیمی‌تر از این (ثانیه) دوباره از بورس خوانده می‌شود
ARCHIVE_STEP = 60            # فاصلهٔ ثبت در آرشیو روزانه (ثانیه)؛ در config.json: archive_seconds
ARCHIVE_DAYS_ON_GITHUB = 120 # چند روز آخر روی GitHub نگه داشته شود؛ در config.json: archive_days_on_github
HISTORY_STEP = 120           # فاصلهٔ نقاط تاریخچهٔ درون‌روز (ثانیه)
DEFAULT_TUNNELS = ["localhost.run", "pinggy", "serveo", "cloudflared"]
REF_SYMBOL = "35425587644337450"   # فملی؛ برای تشخیص تاریخ آخرین روز معاملاتی

log = logging.getLogger("tse_bot")


# ============================================================================
# زمان استاندارد (هم‌تراز با ساعت سرور)
# ============================================================================
_clock = {"offset": 0.0, "synced": False}


def now_ts():
    return time.time() + _clock["offset"]


def _sync_clock(date_header):
    """اختلاف ساعت این کامپیوتر با سرور را از هدر Date پاسخ‌ها حساب می‌کند."""
    try:
        server = email.utils.parsedate_to_datetime(date_header).timestamp()
    except (TypeError, ValueError):
        return
    off = server - time.time()
    if not _clock["synced"]:
        _clock["offset"], _clock["synced"] = off, True
        if abs(off) > 5:
            log.warning("ساعت این کامپیوتر %.0f ثانیه با ساعت سرور اختلاف دارد؛ زمان‌ها اصلاح می‌شوند", off)
    else:
        _clock["offset"] = 0.9 * _clock["offset"] + 0.1 * off


def initial_clock_sync():
    """هم‌ترازی اولیهٔ ساعت با سرور GitHub (پاسخ‌هایش کش نمی‌شوند)."""
    for url in ("https://api.github.com/zen", "https://www.cloudflare.com/cdn-cgi/trace"):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "tse-bot"})
            if r.headers.get("Date"):
                _sync_clock(r.headers["Date"])
                log.info("ساعت با سرور هم‌تراز شد (اختلاف ساعت این کامپیوتر: %+.0f ثانیه)", _clock["offset"])
                return
        except requests.RequestException:
            pass
    log.warning("هم‌ترازی ساعت ممکن نشد؛ از ساعت همین کامپیوتر استفاده می‌شود")


def iso_utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None


def iso_teh(ts):
    return datetime.fromtimestamp(ts, TEHRAN).isoformat(timespec="seconds") if ts else None


def time_meta(fetched_ts, served_ts=None):
    """بلوک زمانی استاندارد که همراه همهٔ داده‌ها فرستاده می‌شود."""
    m = {
        "fetched_at_utc": iso_utc(fetched_ts),
        "fetched_at_tehran": iso_teh(fetched_ts),
        "fetched_at_unix": int(fetched_ts) if fetched_ts else None,
    }
    if served_ts:
        m.update({
            "served_at_utc": iso_utc(served_ts),
            "served_at_tehran": iso_teh(served_ts),
            "age_seconds_when_served": round(served_ts - fetched_ts, 1) if fetched_ts else None,
        })
    m["clock_synced_with_server"] = _clock["synced"]
    return m


# ============================================================================
# لاگ و مسیرها
# ============================================================================
def app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def setup_logging(log_dir="logs", verbose=False):
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, f"tse_bot_{datetime.now(TEHRAN):%Y%m%d}.log")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s: %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    log.handlers.clear()
    log.addHandler(fh)
    log.addHandler(ch)
    log.setLevel(logging.DEBUG)

    def _excepthook(exc_type, exc, tb):
        if not issubclass(exc_type, KeyboardInterrupt):
            log.critical("خطای پیش‌بینی‌نشده", exc_info=(exc_type, exc, tb))
    sys.excepthook = _excepthook
    log.info("=" * 70)
    log.info("tse_bot %s | Python %s | requests %s | %s",
             VERSION, platform.python_version(), requests.__version__, platform.platform())
    log.info("فایل لاگ: %s", os.path.abspath(path))


# ============================================================================
# HTTP به tsetmc
# ============================================================================
_session = requests.Session()
_session.headers.update(HEADERS)

# --- محافظت در برابر مسدود شدن (ban) ---
MIN_GAP = 0.4          # حداقل فاصله بین دو درخواست به tsetmc (ثانیه)
_guard = {"lock": threading.Lock(), "last": 0.0, "cool_until": 0.0, "strikes": 0,
          "count": 0, "count_since": time.time()}


def _wait_turn(low_priority):
    """صف‌بندی درخواست‌ها: هیچ‌وقت دو درخواست پشت‌سرهم بدون فاصله فرستاده نمی‌شود."""
    gap = MIN_GAP + (1.0 if low_priority else 0) + random.random() * 0.2
    with _guard["lock"]:
        wait = max(0.0, _guard["last"] + gap - time.time())
        _guard["last"] = time.time() + wait
        _guard["count"] += 1
    if wait:
        time.sleep(wait)


def _penalize(reason):
    """وقتی سرور نشانهٔ محدودسازی داد، همهٔ درخواست‌ها برای مدتی (هر بار طولانی‌تر) متوقف می‌شوند."""
    with _guard["lock"]:
        if time.time() < _guard["cool_until"]:
            return  # همین الان در دورهٔ توقف هستیم؛ دوباره افزایش نده
        _guard["strikes"] += 1
        cool = min(60 * 2 ** (_guard["strikes"] - 1), 1800)
        _guard["cool_until"] = time.time() + cool
    log.warning("سرور بورس محدودیت اعمال کرد (%s)؛ همهٔ درخواست‌ها %d ثانیه متوقف می‌شوند "
                "(در این مدت آخرین داده‌ی سالم با زمانش نمایش داده می‌شود)", reason, cool)


def _reward():
    if _guard["strikes"]:
        with _guard["lock"]:
            _guard["strikes"] = max(0, _guard["strikes"] - 1)


def blocked():
    return time.time() < _guard["cool_until"]


def request_stats():
    with _guard["lock"]:
        mins = max((time.time() - _guard["count_since"]) / 60, 1e-9)
        n = _guard["count"]
        _guard["count"], _guard["count_since"] = 0, time.time()
    return n, n / mins


def get_json(path, retries=3, timeout=20, low_priority=False):
    url = path if path.startswith("http") else f"{BASE}/{path.lstrip('/')}"
    last_err = None
    for attempt in range(1, retries + 1):
        if blocked():
            return None  # در دورهٔ توقف هیچ درخواستی فرستاده نمی‌شود
        _wait_turn(low_priority)
        try:
            t0 = time.time()
            r = _session.get(url, timeout=timeout)
            ms = (time.time() - t0) * 1000
            if r.status_code == 200 and r.text.strip():
                try:
                    data = r.json()
                    _reward()
                    return data
                except ValueError:
                    last_err = f"پاسخ JSON نیست: {r.text[:300]!r}"
                    if "<html" in r.text[:500].lower():   # صفحهٔ خطا/فایروال به‌جای داده
                        _penalize("پاسخ HTML به‌جای داده")
            else:
                last_err = f"HTTP {r.status_code} ({ms:.0f}ms) body={r.text[:300]!r}"
                _guard["last_raw"] = {"url": url.replace(BASE, ""), "status": r.status_code,
                                      "headers": {k: v for k, v in r.headers.items()
                                                  if k.lower() in ("content-type", "server", "date", "retry-after",
                                                                   "cf-ray", "x-cache", "content-length")},
                                      "body_start": r.text[:500], "at_utc": iso_utc(now_ts())}
                log.warning("پاسخ خام بورس: %s", json.dumps(_guard["last_raw"], ensure_ascii=False))
                if r.status_code in (403, 429, 503):
                    _penalize(f"HTTP {r.status_code}")
            log.debug("تلاش %d/%d ناموفق %s → %s", attempt, retries, url, last_err)
            time.sleep(attempt * 2)
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"
            log.debug("تلاش %d/%d ناموفق %s → %s", attempt, retries, url, last_err)
            time.sleep(attempt * 3)
    log.warning("درخواست بعد از %d تلاش ناموفق بود: %s | آخرین خطا: %s", retries, url, last_err)
    if not low_priority:
        _guard["last_err"] = f"{url.replace(BASE, '')}: {last_err}"
    return None


# ============================================================================
# کمکی‌ها
# ============================================================================
def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def safe_div(a, b):
    a, b = num(a), num(b)
    return a / b if a is not None and b else None


def pct(x, base):
    x, base = num(x), num(base)
    return round((x - base) / base * 100, 2) if x is not None and base else None


def norm_fa(s):
    s = str(s or "").replace("ي", "ی").replace("ك", "ک").replace("‌", "").replace("‏", "")
    return re.sub(r"\s+", " ", s).strip()


# نام‌هایی که خود بورس چسبیده می‌فرستد؛ اینجا به شکل درست اصلاح می‌شوند
NAME_FIXES = {
    "بانک اقتصادنوین": "بانک اقتصاد نوین",
    "تولیدمحورخودرو": "تولید محور خودرو",
    "تایدواترخاورمیانه": "تاید واتر خاورمیانه",
}


def norm_name(s):
    """نام کامل نماد: نیم‌فاصله‌ها (که بورس به‌جای فاصله می‌گذارد) به فاصله تبدیل می‌شوند تا کلمات نچسبند."""
    out = norm_fa(str(s or "").replace("\u200c", " "))
    for bad, good in NAME_FIXES.items():
        out = out.replace(bad, good)
    return out


# ============================================================================
# اطلاعات ثابت نمادها (روزی یک بار، در پس‌زمینه)
# ============================================================================
class StaticInfo:
    def __init__(self, out_dir):
        self.path = os.path.join(out_dir, "static_info.json")
        self.data = {}
        self.fresh = set()          # نمادهایی که امروز به‌روز شده‌اند
        self.lock = threading.Lock()
        self.running = False
        try:
            with open(self.path, encoding="utf-8") as f:
                saved = json.load(f)
            # دادهٔ روزهای قبل هم فوراً استفاده می‌شود و در پس‌زمینه به‌روز می‌شود
            self.data = saved.get("data", {})
            if saved.get("day") == datetime.now(TEHRAN).strftime("%Y%m%d"):
                self.fresh = set(saved.get("fresh") or self.data)
        except (OSError, ValueError):
            pass

    def get(self, code):
        return self.data.get(code, {})

    def day_changed(self):
        d = datetime.now(TEHRAN).strftime("%Y%m%d")
        if getattr(self, "_day", None) != d:
            changed = getattr(self, "_day", None) is not None
            self._day = d
            return changed
        return False

    def refresh_async(self, codes, workers=1):
        if self.day_changed():
            self.fresh = set()
        missing = [c for c in codes if c not in self.fresh]
        if self.running or not missing:
            return
        self.running = True
        threading.Thread(target=self._refresh, args=(missing, workers), name="static", daemon=True).start()

    def _fetch(self, code):
        info = (get_json(f"Instrument/GetInstrumentInfo/{code}", retries=2, low_priority=True) or {}).get(
            "instrumentInfo") or {}
        if not info:
            return code, None
        eps = info.get("eps") or {}
        return code, {
            "گروه": norm_fa((info.get("sector") or {}).get("lSecVal")) or None,
            "بازار": info.get("flowTitle"),
            "تابلو": info.get("cgrValCotTitle"),
            "P/E گروه": eps.get("sectorPE"),
            "EPS پیش‌بینی": num(eps.get("estimatedEPS")),
            "P/S": eps.get("psr"),
            "تعداد سهام": info.get("zTitad"),
            "حجم مبنا": info.get("baseVol"),
            "سهام شناور (درصد)": num(info.get("kAjCapValCpsIdx")),
            "میانگین حجم ماه": info.get("qTotTran5JAvg"),
            "کمترین قیمت هفته": info.get("minWeek"),
            "بیشترین قیمت هفته": info.get("maxWeek"),
            "کمترین قیمت سال": info.get("minYear"),
            "بیشترین قیمت سال": info.get("maxYear"),
            "اطلاعات ثابت به‌روز شده در": datetime.now(TEHRAN).strftime("%Y-%m-%d"),
        }

    def _refresh(self, codes, workers):
        t0 = time.time()
        log.info("خواندن اطلاعات ثابت %d نماد در پس‌زمینه ...", len(codes))
        ok = 0
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                for code, val in pool.map(self._fetch, codes):
                    if val:
                        with self.lock:
                            self.data[code] = val
                            self.fresh.add(code)
                        ok += 1
                        if ok % 200 == 0:
                            self._save()
            self._save()
        except Exception:  # noqa: BLE001
            log.exception("خطا در خواندن اطلاعات ثابت")
        finally:
            self.running = False
        log.info("اطلاعات ثابت: %d از %d نماد، %.0f ثانیه", ok, len(codes), time.time() - t0)

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with self.lock:
                snap = {"day": datetime.now(TEHRAN).strftime("%Y%m%d"), "data": dict(self.data),
                        "fresh": sorted(self.fresh)}
            with open(self.path + ".tmp", "w", encoding="utf-8") as f:
                json.dump(snap, f, ensure_ascii=False)
            os.replace(self.path + ".tmp", self.path)
        except OSError:
            log.exception("ذخیرهٔ اطلاعات ثابت ناموفق بود")


# ============================================================================
# NAV صندوق‌ها (در پس‌زمینه، چرخشی، با اولویت پایین)
# ============================================================================
NAV_CYCLE = 900   # هر ۱۵ دقیقه یک دور کامل


class NavInfo:
    def __init__(self, out_dir=None):
        self.data = {}
        self.codes = []
        self.lock = threading.Lock()
        self.path = os.path.join(out_dir, "nav_info.json") if out_dir else None
        self.last_pass_utc, self.last_pass_ok, self.last_error = None, None, None
        self._load()
        threading.Thread(target=self._loop, name="nav", daemon=True).start()

    def _load(self):
        """NAV دور قبل از دیسک خوانده می‌شود تا با هر بار اجرای دوباره از دست نرود."""
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, encoding="utf-8") as f:
                self.data = json.load(f).get("data", {})
            log.info("NAV %d صندوق از دور قبل خوانده شد", len(self.data))
        except (OSError, ValueError):
            log.exception("خواندن nav_info.json ناموفق بود")

    def _save(self):
        if not self.path:
            return
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with self.lock:
                snap = {"saved_at_utc": iso_utc(time.time()), "data": dict(self.data)}
            with open(self.path + ".tmp", "w", encoding="utf-8") as f:
                json.dump(snap, f, ensure_ascii=False)
            os.replace(self.path + ".tmp", self.path)
        except OSError:
            log.exception("ذخیرهٔ NAV ناموفق بود")

    def stats(self):
        return {"funds": len(self.codes), "with_nav": len(self.data), "last_pass_utc": self.last_pass_utc,
                "last_pass_ok": self.last_pass_ok, "last_error": self.last_error}

    def set_funds(self, codes):
        self.codes = list(codes)

    def get(self, code):
        return self.data.get(code, {})

    def _loop(self):
        time.sleep(20)
        while True:
            start = time.time()
            if self.codes and (market_open() or not self.data):
                n = 0
                for code in list(self.codes):
                    etf = (get_json(f"Fund/GetETFByInsCode/{code}", retries=1, low_priority=True) or {}).get("etf")
                    if etf and (num(etf.get("pRedTran")) or num(etf.get("pSubTran"))):
                        d = str(etf.get("deven") or etf.get("dEven") or "")
                        with self.lock:
                            self.data[code] = {
                                "NAV ابطال": num(etf.get("pRedTran")),
                                "NAV صدور": num(etf.get("pSubTran")),
                                "زمان NAV": trade_time_iso(etf.get("hEven"), d) if len(d) == 8 else None,
                            }
                        n += 1
                log.info("NAV صندوق‌ها: %d از %d صندوق در %.0f ثانیه", n, len(self.codes), time.time() - start)
                self.last_pass_utc, self.last_pass_ok = iso_utc(time.time()), n
                self.last_error = None if n else str(_guard.get("last_raw"))[:160]
                if n:
                    self._save()
            time.sleep(max(60, NAV_CYCLE - (time.time() - start)))


# ============================================================================
# خواندن کل بازار (۲ درخواست)
# ============================================================================
MW_URL = ("ClosingPrice/GetMarketWatch?market=0&industrialGroup="
          "&paperTypes%5B0%5D=1&paperTypes%5B1%5D=2&paperTypes%5B2%5D=3"
          "&paperTypes%5B3%5D=4&paperTypes%5B4%5D=5&paperTypes%5B5%5D=6"
          "&paperTypes%5B6%5D=7&paperTypes%5B7%5D=8&paperTypes%5B8%5D=9"
          "&showTraded=false&withBestLimits=true&hEven=0&RefID=0")

_market_date = {"value": None, "checked": 0}


def market_date():
    """تاریخ آخرین روز معاملاتی (YYYYMMDD) از سرور بورس؛ هر ۵ دقیقه بررسی می‌شود."""
    if time.time() - _market_date["checked"] > 300 or not _market_date["value"]:
        _market_date["checked"] = time.time()
        info = (get_json(f"ClosingPrice/GetClosingPriceInfo/{REF_SYMBOL}", retries=2) or {}).get(
            "closingPriceInfo") or {}
        d = info.get("finalLastDate") or info.get("dEven")
        if d and len(str(int(d))) == 8:
            _market_date["value"] = str(int(d))
    return _market_date["value"] or datetime.now(TEHRAN).strftime("%Y%m%d")


def trade_time_iso(heven, day):
    h = int(num(heven) or 0)
    if not h or not day:
        return None
    try:
        dt = datetime(int(day[:4]), int(day[4:6]), int(day[6:]), h // 10000, h // 100 % 100, h % 100, tzinfo=TEHRAN)
    except ValueError:
        return None
    return dt.isoformat(timespec="seconds")


def paper_kind(r):
    g, sym = r.get("کد گروه"), r.get("نماد") or ""
    if g == "69":
        return "اوراق بدهی"
    if g == "68":
        return "صندوق"
    if re.match(r"^[ضطه][^\d]*\d{3,}", sym):
        return "اختیار معامله"
    return "سهم"


def queue_state(r):
    last, hi, lo = num(r.get("آخرین معامله")), num(r.get("سقف مجاز")), num(r.get("کف مجاز"))
    if last and hi and last >= hi and num(r.get("تقاضا1 قیمت")) == hi and not num(r.get("عرضه1 حجم")):
        return "صف خرید"
    if last and lo and last <= lo and num(r.get("عرضه1 قیمت")) == lo and not num(r.get("تقاضا1 حجم")):
        return "صف فروش"
    return ""


def _pick(d, *keys):
    for k in keys:
        if d.get(k) not in (None, ""):
            return d[k]
    return None


_logged_index_keys = {"done": False}


def fetch_indices():
    """شاخص‌ها و وضعیت کلی بورس و فرابورس."""
    out = {"بازارها": [], "شاخص‌ها": []}
    for flow, title in ((1, "بورس"), (2, "فرابورس")):
        mo = (get_json(f"MarketData/GetMarketOverview/{flow}", retries=2) or {}).get("marketOverview") or {}
        if mo:
            out["بازارها"].append({
                "بازار": title,
                "شاخص کل": mo.get("indexLastValue"),
                "تغییر شاخص کل": mo.get("indexChange"),
                "درصد تغییر شاخص کل": pct(num(mo.get("indexLastValue")),
                                          (num(mo.get("indexLastValue")) or 0) - (num(mo.get("indexChange")) or 0)),
                "شاخص هم‌وزن": mo.get("indexEqualWeightedLastValue"),
                "تغییر شاخص هم‌وزن": mo.get("indexEqualWeightedChange"),
                "تعداد معاملات": mo.get("marketActivityZTotTran"),
                "حجم معاملات": mo.get("marketActivityQTotTran"),
                "ارزش معاملات": mo.get("marketActivityQTotCap"),
                "ارزش بازار": mo.get("marketValue"),
                "وضعیت بازار": mo.get("marketStateTitle"),
                "زمان": trade_time_iso(mo.get("marketActivityHEven"), str(mo.get("marketActivityDEven") or "")),
            })
        rows = (get_json(f"Index/GetIndexB1LastAll/All/{flow}", retries=2) or {}).get("indexB1") or []
        if rows and not _logged_index_keys["done"]:
            _logged_index_keys["done"] = True
            log.info("ساختار شاخص‌ها: %s", json.dumps(rows[0], ensure_ascii=False)[:600])
        for r in rows:
            val = num(_pick(r, "xDrNivJIdx004", "indexLastValue", "xNivInuClMresIbs"))
            chg_pct = num(_pick(r, "xVarIdxJRfV", "indexChangePercent"))
            out["شاخص‌ها"].append({
                "بازار": title,
                "شاخص": norm_fa(_pick(r, "lVal30", "lVal18AFC")),
                "insCode": str(r.get("insCode") or ""),
                "مقدار": val,
                "تغییر": num(_pick(r, "indexChange")),
                "درصد تغییر": chg_pct,
                "کمترین": num(_pick(r, "xPbNivJIdx004", "min")),
                "بیشترین": num(_pick(r, "xPhNivJIdx004", "max")),
                "زمان": trade_time_iso(r.get("hEven"), str(r.get("dEven") or "")),
                "_خام": r,
            })
    return out


# ---------- اختیار معامله ----------
def jalali_to_gregorian(jy, jm, jd):
    jy += 1595
    days = -355668 + 365 * jy + (jy // 33) * 8 + ((jy % 33) + 3) // 4 + jd
    days += (jm - 1) * 31 if jm < 7 else ((jm - 7) * 30) + 186
    gy = 400 * (days // 146097)
    days %= 146097
    if days > 36524:
        days -= 1
        gy += 100 * (days // 36524)
        days %= 36524
        if days >= 365:
            days += 1
    gy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        gy += (days - 1) // 365
        days = (days - 1) % 365
    gd = days + 1
    leap = (gy % 4 == 0 and gy % 100 != 0) or gy % 400 == 0
    for gm, ml in enumerate([31, 29 if leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31], 1):
        if gd <= ml:
            return datetime(gy, gm, gd).date()
        gd -= ml
    return None


def gregorian_to_jalali(gy, gm, gd):
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    gy2 = gy + 1 if gm > 2 else gy
    days = 355666 + (365 * gy) + ((gy2 + 3) // 4) - ((gy2 + 99) // 100) + ((gy2 + 399) // 400) + gd + g_d_m[gm - 1]
    jy = -1595 + (33 * (days // 12053))
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461
    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365
    jm = 1 + days // 31 if days < 186 else 7 + (days - 186) // 30
    jd = 1 + (days % 31 if days < 186 else (days - 186) % 30)
    return jy, jm, jd


def norm_jdate(s):
    """تاریخ شمسی با قالب‌های مختلف (14050806، 1405/08/06، 05/09/04) → «1405/08/06»."""
    if not s:
        return None
    s = str(s).translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789"))
    m = re.search(r"(\d{4})[/\-]?(\d{1,2})[/\-]?(\d{1,2})", s) if re.search(r"\d{8}|\d{4}[/\-]", s) else None
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.search(r"(\d{2})[/\-](\d{1,2})[/\-](\d{1,2})", s)
        if not m:
            return None
        y, mo, d = 1400 + int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1900 < y < 2200 and 1 <= mo <= 12 and 1 <= d <= 31:     # تاریخ میلادی → شمسی
        y, mo, d = gregorian_to_jalali(y, mo, d)
    if not (1300 < y < 1500 and 1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}/{mo:02d}/{d:02d}"


def days_to(jdate):
    try:
        y, m, d = map(int, jdate.split("/"))
        return (jalali_to_gregorian(y, m, d) - datetime.fromtimestamp(now_ts(), TEHRAN).date()).days
    except (ValueError, AttributeError, TypeError):
        return None


def parse_option_name(name):
    """«اختیارخ خودرو-800-1405/08/06» → نوع، دارایی پایه، قیمت اعمال، سررسید (اگر دیده‌بان اختیار در دسترس نبود)."""
    n = norm_fa(name)
    m = re.match(r"^اختیار\s*([خف])\s*(.+?)\s*-\s*([\d,]+)\s*-\s*(.+)$", n)
    if not m:
        return {}
    return {"نوع اختیار": "خرید" if m.group(1) == "خ" else "فروش", "نماد پایه": m.group(2).strip(),
            "قیمت اعمال": num(m.group(3).replace(",", "")), "تاریخ سررسید": norm_jdate(m.group(4))}


def fetch_options():
    """دیده‌بان اختیار معامله: قیمت اعمال، سررسید، موقعیت‌های باز، دارایی پایه. خروجی: {insCode: {...}}"""
    out = {}
    for market in (1, 2):
        data = (get_json(f"Instrument/GetInstrumentOptionMarketWatch/{market}", retries=2) or {}).get(
            "instrumentOptMarketWatch") or []
        for o in data:
            common = {
                "نماد پایه": norm_fa(o.get("lval30_UA") or o.get("lVal30_UA")),
                "insCode پایه": str(o.get("uaInsCode") or ""),
                "قیمت پایانی پایه": o.get("pClosing_UA"),
                "قیمت اعمال": o.get("strikePrice"),
                "اندازه قرارداد": o.get("contractSize"),
                "تاریخ شروع": norm_jdate(o.get("beginDate")),
                "تاریخ سررسید": norm_jdate(o.get("endDate")),
                "روز تا سررسید": o.get("remainedDay"),
            }
            for side, kind in (("C", "خرید"), ("P", "فروش")):
                code = str(o.get(f"insCode_{side}") or "")
                if code:
                    out[code] = {**common, "نوع اختیار": kind, "موقعیت‌های باز": o.get(f"oP_{side}"),
                                 "ارزش اسمی": o.get(f"notionalValue_{side}")}
    return out


def fetch_market(static, nav=None, options=None):
    fetch_market.premarket = 0
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_mw = pool.submit(get_json, MW_URL)
        f_ct = pool.submit(get_json, "ClientType/GetClientTypeAll")
        f_day = pool.submit(market_date)
        mw, ct, day = f_mw.result(), f_ct.result(), f_day.result()

    mw_rows = (mw or {}).get("marketwatch")
    if not isinstance(mw_rows, list):
        log.error("ساختار دیده‌بان بازار ناشناخته است: %s", str(mw)[:500])
        return []
    ct_rows = (ct or {}).get("clientTypeAllDto")
    if not isinstance(ct_rows, list):
        log.warning("حقیقی/حقوقی دریافت نشد یا ساختارش عوض شده: %s", str(ct)[:300])
        ct_rows = []
    clients = {str(c.get("insCode")): c for c in ct_rows}

    rows = []
    if mw_rows and not any(num(m.get("qtj")) or num(m.get("ztt")) for m in mw_rows):
        # پاسخ سالم است ولی هنوز هیچ نمادی معامله نشده (پیش از بازار / بعد از صفر شدن دیده‌بان)
        fetch_market.premarket = len(mw_rows)
        return []
    fetch_market.premarket = 0
    for m in mw_rows:
        if not (num(m.get("qtj")) or num(m.get("ztt"))):
            continue
        code = str(m.get("insCode"))
        close, last, yday = m.get("pcl"), m.get("pdv"), m.get("py")
        r = {
            "insCode": code,
            "نماد": norm_fa(m.get("lva")),
            "نام": norm_fa(m.get("lvc")) if norm_fa(m.get("lvc")).startswith("اختیار") else norm_name(m.get("lvc")),
            "زمان آخرین معامله": trade_time_iso(m.get("hEven"), day),
            "آخرین معامله": last,
            "درصد تغییر آخرین": pct(last, yday),
            "قیمت پایانی": close,
            "درصد تغییر پایانی": pct(close, yday),
            "قیمت دیروز": yday,
            "اولین قیمت": m.get("pf"),
            "کمترین": m.get("pmn"),
            "بیشترین": m.get("pmx"),
            "تعداد معاملات": m.get("ztt"),
            "حجم معاملات": m.get("qtj"),
            "ارزش معاملات": m.get("qtc"),
            "سقف مجاز": m.get("pMax"),
            "کف مجاز": m.get("pMin"),
            "EPS": m.get("eps"),
            "P/E": num(m.get("pe")) if m.get("pe") not in (None, "-", "") else safe_div(close, m.get("eps")),
            "حجم مبنا": m.get("bv"),
            "کد گروه": str(m.get("csv") or "").strip(),
        }
        r["نوع"] = paper_kind(r)
        if r["نوع"] == "اختیار معامله" or r["نام"].startswith("اختیار"):
            opt = dict((options or {}).get(code) or {})
            from_name = parse_option_name(r["نام"])
            for k, v in from_name.items():          # هر چه دیده‌بان اختیار نداشت، از نام نماد
                if opt.get(k) in (None, ""):
                    opt[k] = v
            if opt:
                r["نوع"] = "اختیار معامله"
                r.update(opt)
                if r.get("تاریخ سررسید") and r.get("روز تا سررسید") is None:
                    r["روز تا سررسید"] = days_to(r["تاریخ سررسید"])
        r.update(static.get(code))
        if nav:
            r.update(nav.get(code))
            if num(r.get("NAV ابطال")) and num(close):
                r["حباب نسبت به NAV ابطال (درصد)"] = round((num(close) / num(r["NAV ابطال"]) - 1) * 100, 2)
        if num(close) and num(r.get("تعداد سهام")):
            r["ارزش بازار"] = num(close) * num(r["تعداد سهام"])

        c = clients.get(code)
        if c:
            price = num(close) or num(last) or 0
            for fa, side, kind in [("خرید حقیقی", "buy", "I"), ("خرید حقوقی", "buy", "N"),
                                   ("فروش حقیقی", "sell", "I"), ("فروش حقوقی", "sell", "N")]:
                vol = num(c.get(f"{side}_{kind}_Volume"))
                r[f"حجم {fa}"] = vol
                r[f"تعداد {fa}"] = c.get(f"{side}_Count{kind}")
                r[f"ارزش {fa}"] = vol * price if vol is not None else None
            pb = safe_div(c.get("buy_I_Volume"), c.get("buy_CountI"))
            ps = safe_div(c.get("sell_I_Volume"), c.get("sell_CountI"))
            r["سرانه خرید حقیقی"] = pb * price if pb else None
            r["سرانه فروش حقیقی"] = ps * price if ps else None
            r["قدرت خریدار حقیقی"] = round(pb / ps, 3) if pb and ps else None
            if r["ارزش خرید حقیقی"] is not None and r["ارزش فروش حقیقی"] is not None:
                r["ورود پول حقیقی"] = r["ارزش خرید حقیقی"] - r["ارزش فروش حقیقی"]

        for lvl in sorted(m.get("blDs") or [], key=lambda x: x.get("n", 0))[:5]:
            n = lvl.get("n")
            r[f"تقاضا{n} تعداد"] = lvl.get("zmd")
            r[f"تقاضا{n} حجم"] = lvl.get("qmd")
            r[f"تقاضا{n} قیمت"] = lvl.get("pmd")
            r[f"عرضه{n} قیمت"] = lvl.get("pmo")
            r[f"عرضه{n} حجم"] = lvl.get("qmo")
            r[f"عرضه{n} تعداد"] = lvl.get("zmo")
        r["صف"] = queue_state(r)
        rows.append(r)

    # اطلاعات ثابت فقط برای سهام و صندوق‌ها، آرام و با اولویت پایین
    by_sym = {r["نماد"]: r for r in rows}
    for r in rows:
        if r.get("نوع") == "اختیار معامله" and not r.get("insCode پایه") and r.get("نماد پایه") in by_sym:
            ua = by_sym[r["نماد پایه"]]
            r["insCode پایه"] = ua["insCode"]
            r.setdefault("قیمت پایانی پایه", ua.get("قیمت پایانی"))
    static.refresh_async([r["insCode"] for r in rows if r.get("نوع") in ("سهم", "صندوق")])
    if nav:
        nav.set_funds([r["insCode"] for r in rows if r.get("نوع") == "صندوق"])
    return rows


# ============================================================================
# تاریخچهٔ درون‌روز
# ============================================================================
HISTORY_FIELDS = ["آخرین معامله", "قیمت پایانی", "درصد تغییر پایانی", "حجم معاملات", "ارزش معاملات",
                  "قدرت خریدار حقیقی", "ورود پول حقیقی", "تقاضا1 حجم", "عرضه1 حجم"]


class History:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.day = None
        self.data = {}
        self.last_t = 0
        self.lock = threading.Lock()

    def update(self, rows, ts):
        day = datetime.fromtimestamp(ts, TEHRAN).strftime("%Y%m%d")
        path = os.path.join(self.out_dir, f"history_{day}.json")
        with self.lock:
            if self.day != day:
                self.day, self.data, self.last_t = day, {}, 0
                try:
                    with open(path, encoding="utf-8") as f:
                        self.data = json.load(f)
                except (OSError, ValueError):
                    pass
            if ts - self.last_t < HISTORY_STEP:
                return False
            self.last_t = ts
            t = iso_teh(ts)
            for r in rows:
                point = {f: r.get(f) for f in HISTORY_FIELDS}
                h = self.data.setdefault(r["نماد"], [])
                if not h or {k: v for k, v in h[-1].items() if k != "زمان"} != point:
                    h.append({"زمان": t, **point})
            try:
                os.makedirs(self.out_dir, exist_ok=True)
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, default=str)
            except OSError:
                log.exception("ذخیرهٔ تاریخچه ناموفق بود")
        return True

    def get(self, sym):
        with self.lock:
            return list(self.data.get(sym, []))


# ============================================================================
# حافظهٔ مشترک داده‌ها
# ============================================================================
def to_csv(rows):
    cols = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


# ============================================================================
# آرشیو روزانه (فشرده، برای تحلیل بعدی)
# ============================================================================
ARCHIVE_COLS = (["زمان دریافت", "نماد", "insCode", "زمان آخرین معامله", "آخرین معامله", "قیمت پایانی",
                 "قیمت دیروز", "اولین قیمت", "کمترین", "بیشترین", "تعداد معاملات", "حجم معاملات", "ارزش معاملات"]
                + [f"{k} {fa}" for fa in ("خرید حقیقی", "خرید حقوقی", "فروش حقیقی", "فروش حقوقی")
                   for k in ("حجم", "تعداد")]
                + [f"{side}{n} {k}" for n in range(1, 6) for side in ("تقاضا", "عرضه")
                   for k in ("قیمت", "حجم", "تعداد")]
                + ["NAV ابطال", "NAV صدور", "موقعیت‌های باز", "قیمت پایانی پایه"])
INDEX_ARCHIVE_COLS = ["زمان دریافت", "بازار", "شاخص", "مقدار", "تغییر", "درصد تغییر"]
STATIC_ARCHIVE_COLS = ["نماد", "insCode", "نام", "نوع", "گروه", "بازار", "تابلو", "تعداد سهام", "حجم مبنا",
                       "سهام شناور (درصد)", "میانگین حجم ماه", "EPS", "EPS پیش‌بینی", "P/E گروه", "P/S",
                       "سقف مجاز", "کف مجاز", "کمترین قیمت هفته", "بیشترین قیمت هفته", "کمترین قیمت سال",
                       "بیشترین قیمت سال", "نوع اختیار", "نماد پایه", "insCode پایه", "قیمت اعمال",
                       "اندازه قرارداد", "تاریخ شروع", "تاریخ سررسید", "روز تا سررسید"]


def _compact(v):
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else f"{v:.6g}"
    return "" if v is None else str(v)


class Archive:
    """هر N ثانیه در ساعات بازار، فقط نمادهایی که تغییر کرده‌اند به فایل روز (csv.gz) اضافه می‌شوند."""

    def __init__(self, out_dir, step):
        self.dir = os.path.join(out_dir, "archive")
        self.step = step
        self.last_t = 0
        self.last = {}
        self.day = None
        self.lock = threading.Lock()

    def path(self, day, kind=""):
        return os.path.join(self.dir, f"{day}{kind}.csv.gz")

    def final_path(self, day, kind=""):
        return os.path.join(self.dir, f"{day}{kind}.csv.xz")

    def existing(self, day, kind=""):
        """مسیر فایل موجود آن روز (نسخهٔ نهایی xz یا نسخهٔ در حال تکمیل gz)."""
        for p in (self.final_path(day, kind), self.path(day, kind)):
            if os.path.exists(p):
                return p
        return None

    def add(self, rows, indices, ts):
        if not self.step or not market_open() or ts - self.last_t < self.step - 15:
            return
        self.last_t = ts
        day = datetime.fromtimestamp(ts, TEHRAN).strftime("%Y-%m-%d")
        t = datetime.fromtimestamp(ts, TEHRAN).strftime("%H:%M:%S")
        with self.lock:
            if day != self.day:
                self.day, self.last = day, {}
            os.makedirs(self.dir, exist_ok=True)
            buf, w = io.StringIO(), None
            path = self.path(day)
            new_file = not os.path.exists(path)
            w = csv.writer(buf)
            if new_file:
                w.writerow(ARCHIVE_COLS)
            n = 0
            for r in rows:
                vals = [_compact((r.get("زمان آخرین معامله") or "")[11:19] if c == "زمان آخرین معامله" else r.get(c))
                        for c in ARCHIVE_COLS[1:]]
                key = r.get("insCode")
                if self.last.get(key) == vals:
                    continue
                self.last[key] = vals
                w.writerow([t] + vals)
                n += 1
            if n or new_file:
                with gzip.open(path, "ab", compresslevel=9) as f:   # چند بخش gzip پشت‌سرهم؛ خواندنش عادی است
                    f.write(buf.getvalue().encode("utf-8"))
            # شاخص‌ها
            ipath = self.path(day, "_indices")
            ibuf = io.StringIO()
            iw = csv.writer(ibuf)
            if not os.path.exists(ipath):
                iw.writerow(INDEX_ARCHIVE_COLS)
            for x in (indices or {}).get("شاخص‌ها", []):
                iw.writerow([t] + [_compact(x.get(c)) for c in INDEX_ARCHIVE_COLS[1:]])
            for x in (indices or {}).get("بازارها", []):
                iw.writerow([t, x.get("بازار"), "شاخص کل (نمای کلی)", _compact(x.get("شاخص کل")),
                             _compact(x.get("تغییر شاخص کل")), _compact(x.get("درصد تغییر شاخص کل"))])
            with gzip.open(ipath, "ab", compresslevel=9) as f:
                f.write(ibuf.getvalue().encode("utf-8"))
        log.debug("آرشیو: %d ردیف تغییرکرده ثبت شد", n)

    def write_static(self, day, rows):
        os.makedirs(self.dir, exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(STATIC_ARCHIVE_COLS)
        for r in rows:
            w.writerow([_compact(r.get(c)) for c in STATIC_ARCHIVE_COLS])
        with open(self.final_path(day, "_static"), "wb") as f:
            f.write(lzma.compress(buf.getvalue().encode("utf-8"), preset=9))

    def kinds(self, day):
        try:
            names = os.listdir(self.dir)
        except OSError:
            return []
        out = set()
        for n in names:
            m = re.match(rf"^{day}(_[a-z]+)?\.csv\.(gz|xz)$", n)
            if m:
                out.add(m.group(1) or "")
        return sorted(out)

    def recompress(self, day):
        """در پایان روز، فایل‌های gzip روز با xz دوباره فشرده می‌شوند (حدود ۴ برابر کم‌حجم‌تر)."""
        for kind in self.kinds(day):
            path = self.path(day, kind)
            if not os.path.exists(path):
                continue
            try:
                with gzip.open(path, "rb") as f:
                    data = f.read()
                before = os.path.getsize(path)
                xz = self.final_path(day, kind)
                with open(xz + ".tmp", "wb") as f:
                    f.write(lzma.compress(data, preset=9 | lzma.PRESET_EXTREME))
                os.replace(xz + ".tmp", xz)
                os.remove(path)
                log.info("آرشیو %s%s فشرده شد: %.2f MB (gz) → %.2f MB (xz)", day, kind, before / 1048576,
                         os.path.getsize(xz) / 1048576)
            except (OSError, EOFError):
                log.exception("فشرده‌سازی آرشیو %s ناموفق بود", day)

    def days(self):
        try:
            return sorted({f[:10] for f in os.listdir(self.dir) if re.match(r"\d{4}-\d{2}-\d{2}.*\.csv\.(gz|xz)$", f)})
        except OSError:
            return []


class Market:
    """آخرین داده‌ها؛ هم سرور زنده و هم انتشار GitHub از همین استفاده می‌کنند."""

    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.static = StaticInfo(out_dir)
        self.history = History(out_dir)
        self.nav = NavInfo(out_dir)
        self.archive = Archive(out_dir, ARCHIVE_STEP)
        self.indices, self.indices_fetched = {}, 0.0
        self.options, self.options_fetched = {}, 0.0
        self.rows, self.by_sym, self.fetched = [], {}, 0.0
        self.hist_changed = False
        self.lock = threading.Lock()
        self.last_error = None
        self.status_note = None
        self.last_success = 0.0
        self.snap_path = os.path.join(out_dir, "last_session.json.gz")
        self._snap_saved = 0.0
        try:   # آخرین دادهٔ سالم جلسهٔ قبل (برای پیش از بازار و بعد از ری‌استارت)
            with gzip.open(self.snap_path, "rt", encoding="utf-8") as f:
                snap = json.load(f)
            self.rows, self.fetched = snap["rows"], float(snap["fetched"])
            self.by_sym = {r["نماد"]: r for r in self.rows}
            self.indices, self.indices_fetched = snap.get("indices") or {}, float(snap.get("indices_fetched") or 0)
            log.info("دادهٔ آخرین جلسه بارگذاری شد: %d نماد از %s", len(self.rows), iso_teh(self.fetched))
        except (OSError, ValueError, KeyError, EOFError):
            pass

    def get(self, max_age):
        with self.lock:
            if now_ts() - self.fetched > max_age:
                self._refresh()
            if now_ts() - self.indices_fetched > max(max_age, 30) and not blocked():
                try:
                    ind = fetch_indices()
                    if ind.get("شاخص‌ها") or ind.get("بازارها"):
                        self.indices, self.indices_fetched = ind, now_ts()
                except Exception:  # noqa: BLE001
                    log.exception("خطا در خواندن شاخص‌ها")
            return self.rows, self.fetched

    def _refresh(self):
        if blocked():
            self.last_error = {"at_utc": iso_utc(now_ts()), "reason": "درخواست‌ها به‌خاطر محدودیت سرور بورس موقتاً "
                               f"متوقف است تا {iso_utc(_guard['cool_until'] + _clock['offset'])}"}
            return  # در دورهٔ توقف، آخرین داده‌ی سالم با زمانش برگردانده می‌شود
        t0 = time.time()
        try:
            if now_ts() - self.options_fetched > 60 and not blocked():
                try:
                    opts = fetch_options()
                    if opts:
                        self.options, self.options_fetched = opts, now_ts()
                except Exception:  # noqa: BLE001
                    log.exception("خطا در خواندن دیده‌بان اختیار معامله")
            rows = fetch_market(self.static, self.nav, self.options)
        except Exception:  # noqa: BLE001
            log.exception("خطا در خواندن بازار")
            rows = []
        if not rows and getattr(fetch_market, "premarket", 0):
            self.status_note = (f"پیش از بازار: دیده‌بان {fetch_market.premarket} نماد برگرداند ولی هنوز معامله‌ای ثبت نشده؛ "
                                + (f"دادهٔ آخرین جلسه ({iso_teh(self.fetched)}) نمایش داده می‌شود" if self.rows
                                   else "دادهٔ جلسهٔ قبل هنوز ذخیره نشده است"))
            self.last_error = None
            return
        if not rows:
            self.last_error = {"at_utc": iso_utc(now_ts()), "raw_response": _guard.get("last_raw"),
                               "reason": ("درخواست‌ها به‌خاطر محدودیت سرور بورس موقتاً متوقف است" if blocked()
                                          else _guard.get("last_err") or "پاسخ خالی یا ساختار ناشناخته")}
            log.error("داده‌ای از tsetmc دریافت نشد؛ آخرین داده‌ی سالم (%s) نگه داشته می‌شود — "
                      "اتصال اینترنت/VPN را بررسی کنید", iso_teh(self.fetched) or "ندارد")
            return
        ts = now_ts()
        self.rows, self.fetched = rows, ts
        self.by_sym = {r["نماد"]: r for r in rows}
        if self.history.update(rows, ts):
            self.hist_changed = True
        try:
            self.archive.add(rows, self.indices, ts)
        except Exception:  # noqa: BLE001
            log.exception("خطا در نوشتن آرشیو")
        if getattr(self, "alerts", None):
            try:
                self.alerts.evaluate(rows, ts)
            except Exception:  # noqa: BLE001
                log.exception("خطا در ارزیابی هشدارها")
        self.last_error = None
        self.status_note = None
        self.last_success = ts
        if time.time() - self._snap_saved > 60:
            self._snap_saved = time.time()
            try:
                with gzip.open(self.snap_path + ".tmp", "wt", encoding="utf-8") as f:
                    json.dump({"rows": rows, "fetched": ts, "indices": self.indices,
                               "indices_fetched": self.indices_fetched}, f, ensure_ascii=False, default=str)
                os.replace(self.snap_path + ".tmp", self.snap_path)
            except OSError:
                log.exception("ذخیرهٔ دادهٔ آخرین جلسه ناموفق بود")
        try:
            os.makedirs(self.out_dir, exist_ok=True)
            with open(os.path.join(self.out_dir, "latest.csv"), "w", newline="", encoding="utf-8-sig") as f:
                f.write(to_csv(rows))
        except OSError:
            log.warning("ذخیرهٔ latest.csv ناموفق بود (اگر در اکسل باز است، ببندید)")
        log.debug("%d نماد خوانده شد در %.1f ثانیه", len(rows), time.time() - t0)

    def find(self, sym):
        s = norm_fa(sym)
        if s in self.by_sym:
            return self.by_sym[s]
        for r in self.rows:
            if r.get("insCode") == s or norm_fa(r.get("نام")) == s:
                return r
        return None


# ============================================================================
# قالب‌بندی خروجی (مشترک بین اتصال زنده و GitHub)
# ============================================================================
def _fmt(v):
    if v is None or v == "":
        return "-"
    if isinstance(v, (int, float)):
        if abs(v) >= 1e9:
            return f"{v / 1e9:,.1f} میلیارد"
        if isinstance(v, float) and not v.is_integer():
            return f"{v:,.2f}"
        return f"{int(v):,}"
    return str(v)


def _table(rows, cols, link=None):
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        cells = [_fmt(r.get(c)) for c in cols]
        if link and cols[0] == "نماد" and r.get("نماد"):
            cells[0] = f"[{r['نماد']}]({link(r['نماد'])})"
        out.append("| " + " | ".join(cells) + " |")
    return "\n".join(out)


def time_block_md(meta):
    lines = [
        f"- زمان دریافت از بورس (UTC): `{meta['fetched_at_utc']}`",
        f"- زمان دریافت از بورس (تهران): `{meta['fetched_at_tehran']}`",
    ]
    if meta.get("served_at_utc"):
        lines += [f"- زمان ارسال این پاسخ (UTC): `{meta['served_at_utc']}`",
                  f"- عمر داده هنگام ارسال: {meta['age_seconds_when_served']} ثانیه"]
    return "\n".join(lines)


EQ_KINDS = ("سهم", "صندوق")
BASE_COLS = ["نماد", "قیمت پایانی", "درصد تغییر پایانی", "ارزش معاملات", "قدرت خریدار حقیقی", "ورود پول حقیقی"]
TOP_LISTS = [  # (عنوان، ستون، صعودی؟، حداقل ارزش معاملات)
    ("بیشترین ارزش معاملات", "ارزش معاملات", False, 0),
    ("بیشترین رشد", "درصد تغییر پایانی", False, 0),
    ("بیشترین افت", "درصد تغییر پایانی", True, 0),
    ("بیشترین ورود پول حقیقی", "ورود پول حقیقی", False, 0),
    ("بیشترین خروج پول حقیقی", "ورود پول حقیقی", True, 0),
    ("بیشترین قدرت خریدار حقیقی (ارزش بالای ۱۰ میلیارد ریال)", "قدرت خریدار حقیقی", False, 1e10),
]


def top_rows(rows, field, asc=False, n=20, min_value=0, kinds=EQ_KINDS):
    rs = [r for r in rows if (not kinds or r.get("نوع") in kinds) and num(r.get(field)) is not None
          and (num(r.get("ارزش معاملات")) or 0) >= min_value]
    return sorted(rs, key=lambda r: num(r.get(field)), reverse=not asc)[:n]


def md_indices(ind, meta=None, limit=None):
    parts = ["# شاخص‌های بازار", ""] if limit is None else ["## شاخص‌ها و وضعیت بازار", ""]
    if meta and limit is None:
        parts += [time_block_md(meta), ""]
    if not ind:
        return "\n".join(parts + ["(هنوز دریافت نشده)"])
    for m in ind.get("بازارها", []):
        parts.append(f"- **{m['بازار']}**: شاخص کل {_fmt(m.get('شاخص کل'))} ({_fmt(m.get('تغییر شاخص کل'))}، "
                     f"{_fmt(m.get('درصد تغییر شاخص کل'))}٪) | شاخص هم‌وزن {_fmt(m.get('شاخص هم‌وزن'))} "
                     f"({_fmt(m.get('تغییر شاخص هم‌وزن'))}) | ارزش معاملات {_fmt(m.get('ارزش معاملات'))} | "
                     f"ارزش بازار {_fmt(m.get('ارزش بازار'))} | وضعیت: {m.get('وضعیت بازار') or '-'}")
    idx = ind.get("شاخص‌ها", [])
    if limit:
        idx = idx[:limit]
    if idx:
        parts += ["", _table(idx, ["شاخص", "بازار", "مقدار", "تغییر", "درصد تغییر", "کمترین", "بیشترین", "زمان"])]
    return "\n".join(parts)


def md_summary(rows, meta, link=None, n=20, indices=None):
    eq = [r for r in rows if r.get("نوع") in EQ_KINDS]
    up = sum(1 for r in eq if (num(r.get("درصد تغییر پایانی")) or 0) > 0)
    down = sum(1 for r in eq if (num(r.get("درصد تغییر پایانی")) or 0) < 0)
    buy_q = [r for r in eq if r.get("صف") == "صف خرید"]
    sell_q = [r for r in eq if r.get("صف") == "صف فروش"]
    parts = [
        "# خلاصهٔ بازار بورس تهران", "", time_block_md(meta), "",
        "جدول‌ها فقط سهام و صندوق‌ها را شامل می‌شوند (اوراق بدهی و اختیار معامله در جدول همهٔ نمادها هستند).", "",
        f"- سهام و صندوق‌های معامله‌شده: {len(eq)} | مثبت: {up} | منفی: {down}",
        f"- ارزش معاملات سهام و صندوق‌ها: {_fmt(sum(num(r.get('ارزش معاملات')) or 0 for r in eq))} ریال",
        f"- خالص ورود پول حقیقی: {_fmt(sum(num(r.get('ورود پول حقیقی')) or 0 for r in eq))} ریال",
        f"- صف خرید: {len(buy_q)} | صف فروش: {len(sell_q)}", "",
        md_indices(indices, limit=12), "",
    ]
    for title, field, asc, mv in TOP_LISTS:
        parts += [f"## {title}", _table(top_rows(rows, field, asc, n, mv), BASE_COLS, link), ""]
    parts += ["## صف‌های خرید", _table(buy_q[:50], BASE_COLS, link), "",
              "## صف‌های فروش", _table(sell_q[:50], BASE_COLS, link), ""]
    return "\n".join(parts)


ALL_COLS = ["نماد", "نوع", "گروه", "قیمت پایانی", "درصد تغییر پایانی", "درصد تغییر آخرین", "ارزش معاملات",
            "قدرت خریدار حقیقی", "ورود پول حقیقی", "P/E", "صف", "زمان آخرین معامله"]


def md_all(rows, meta, link=None):
    return ("# همهٔ نمادهای معامله‌شده\n\n" + time_block_md(meta) + "\n\n"
            "روی نام هر نماد بزنید تا همهٔ اطلاعاتش را ببینید.\n\n"
            + _table(sorted(rows, key=lambda r: -(num(r.get("ارزش معاملات")) or 0)), ALL_COLS, link))


def symbol_doc(r, meta, history=None):
    d = {"_زمان": meta, **{k: v for k, v in r.items() if v not in (None, "")}}
    if history is not None:
        d["تاریخچه امروز"] = history
    return d


COLUMNS_HELP = """## ستون‌ها
- قیمت و ارزش به ریال.
- «ارزش خرید/فروش حقیقی/حقوقی» و «ورود پول حقیقی» تقریبی‌اند (حجم × قیمت پایانی).
- «قدرت خریدار حقیقی» = سرانهٔ خرید حقیقی ÷ سرانهٔ فروش حقیقی (دقیق).
- «تقاضاN/عرضهN» = ردیف N دفتر سفارش. «صف» = صف خرید/فروش.
- «زمان آخرین معامله» = زمان آخرین معاملهٔ همان نماد، به وقت تهران.

## زمان‌ها (استاندارد ISO 8601)
- `...Z` یعنی UTC و `...+03:30` یعنی وقت تهران (ایران تغییر ساعت تابستانی ندارد).
- `fetched_at_utc` = لحظه‌ای که ربات این داده را از سرور بورس گرفت.
- `served_at_utc` = لحظه‌ای که این پاسخ فرستاده شد (فقط در اتصال زنده).
- تأخیر داده = (زمان فعلی UTC) − `fetched_at_utc`.
- ساعت ربات با ساعت سرور GitHub هم‌تراز می‌شود تا ساعت غلط کامپیوتر اثری نداشته باشد (`clock_synced_with_server`).
- `زمان آخرین معامله` هر نماد نشان می‌دهد داده‌ی بورس تا چه لحظه‌ای را شامل می‌شود.
"""


# ============================================================================
# سرور زنده (از طریق تونل‌ها در دسترس است)
# ============================================================================
# ============================================================================
# تحلیل بازه‌ای (بین هر دو زمان، روی هر نماد/صنعت/نوع/کل بازار) — از روی آرشیو روزانه
# ============================================================================
RANGE_COLS = ["آخرین معامله", "قیمت پایانی", "قیمت دیروز", "تعداد معاملات", "حجم معاملات", "ارزش معاملات",
              "حجم خرید حقیقی", "تعداد خرید حقیقی", "حجم خرید حقوقی", "تعداد خرید حقوقی",
              "حجم فروش حقیقی", "تعداد فروش حقیقی", "حجم فروش حقوقی", "تعداد فروش حقوقی",
              "تقاضا1 قیمت", "تقاضا1 حجم", "عرضه1 قیمت", "عرضه1 حجم", "موقعیت‌های باز", "NAV ابطال"]
CUM = ["تعداد معاملات", "حجم معاملات", "ارزش معاملات", "حجم خرید حقیقی", "تعداد خرید حقیقی", "حجم خرید حقوقی",
       "تعداد خرید حقوقی", "حجم فروش حقیقی", "تعداد فروش حقیقی", "حجم فروش حقوقی", "تعداد فروش حقوقی"]


def _secs(hms):
    try:
        p = [int(x) for x in str(hms).strip().split(":")]
        return p[0] * 3600 + p[1] * 60 + (p[2] if len(p) > 2 else 0)
    except (ValueError, IndexError):
        return None


def _hms(s):
    return f"{s // 3600:02d}:{s // 60 % 60:02d}:{s % 60:02d}" if s is not None else None


class RangeStore:
    """آرشیو هر روز را یک بار می‌خواند و در حافظه نگه می‌دارد (چند روز آخر)."""

    def __init__(self, market):
        self.m = market
        self.cache = {}           # day -> (mtime, data)
        self.lock = threading.Lock()

    def days(self):
        return sorted(self.m.archive.days(), reverse=True)

    def _open(self, path):
        if path.endswith(".xz"):
            return io.TextIOWrapper(lzma.open(path, "rb"), encoding="utf-8")
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")

    def load(self, day):
        path = self.m.archive.existing(day)
        if not path:
            return None
        mt = os.path.getmtime(path)
        with self.lock:
            hit = self.cache.get(day)
            if hit and hit[0] == mt:
                return hit[1]
        data, idx = {}, None
        try:
            with self._open(path) as f:
                rd = csv.reader(f)
                header = None
                for row in rd:
                    if header is None or (row and row[0] == "زمان دریافت"):
                        header = row
                        idx = {c: header.index(c) for c in RANGE_COLS + ["نماد", "insCode"] if c in header}
                        continue
                    t = _secs(row[0])
                    if t is None:
                        continue
                    code = row[idx["insCode"]]
                    d = data.get(code)
                    if d is None:
                        d = data[code] = {"sym": norm_fa(row[idx["نماد"]]), "t": [],
                                          **{c: [] for c in RANGE_COLS if c in idx}}
                    d["t"].append(t)
                    for c in RANGE_COLS:
                        if c in idx:
                            v = row[idx[c]]
                            d[c].append(float(v) if v not in ("", None) else None)
        except EOFError:
            pass        # فایل روز جاری در حال نوشتن است؛ هر چه خوانده شد کافی است
        except Exception:  # noqa: BLE001
            log.exception("خواندن آرشیو %s برای تحلیل بازه‌ای ناموفق بود", day)
        info = self._static(day)
        out = {"symbols": data, "static": info,
               "first": min((d["t"][0] for d in data.values() if d["t"]), default=None),
               "last": max((d["t"][-1] for d in data.values() if d["t"]), default=None)}
        with self.lock:
            self.cache[day] = (mt, out)
            if len(self.cache) > 4:
                self.cache.pop(sorted(self.cache)[0])
        return out

    def _static(self, day):
        """نام، نوع و گروه هر نماد: از فایل اطلاعات ثابت همان روز، وگرنه از دادهٔ فعلی ربات."""
        info = {}
        p = self.m.archive.existing(day, "_static")
        if p:
            try:
                with self._open(p) as f:
                    for r in csv.DictReader(f):
                        info[r.get("insCode")] = {"نام": r.get("نام"), "نوع": r.get("نوع"), "گروه": r.get("گروه")}
            except Exception:  # noqa: BLE001
                pass
        for r in self.m.rows:
            info.setdefault(r.get("insCode"), {"نام": r.get("نام"), "نوع": r.get("نوع"), "گروه": r.get("گروه")})
        for code, d in self.m.static.data.items():
            info.setdefault(code, {}).setdefault("گروه", d.get("گروه"))
        return info

    @staticmethod
    def _at(d, t):
        """وضعیت نماد در لحظهٔ t = آخرین ثبت تا آن لحظه (قبل از اولین معامله = صفر)."""
        i = bisect.bisect_right(d["t"], t) - 1
        if i < 0:
            return None, {c: 0.0 for c in CUM}
        return d["t"][i], {c: d[c][i] for c in RANGE_COLS if c in d}

    def query(self, day, t1, t2, scope="all", symbols=None, group=None, kind=None, group_by="symbol",
              sort="ارزش معاملات", desc=True, limit=200):
        data = self.load(day)
        if not data:
            return {"error": f"آرشیو روز {day} پیدا نشد", "days": self.days()}
        t1 = t1 if t1 is not None else (data["first"] or 0)
        t2 = t2 if t2 is not None else (data["last"] or 86399)
        if t2 < t1:
            t1, t2 = t2, t1
        syms = {norm_fa(s) for s in (symbols or []) if s.strip()}
        info = data["static"]
        rows = []
        for code, d in data["symbols"].items():
            meta = info.get(code, {})
            if syms and d["sym"] not in syms:
                continue
            if group and norm_fa(meta.get("گروه")) != norm_fa(group):
                continue
            if kind and meta.get("نوع") != kind:
                continue
            ta, a = self._at(d, t1)
            tb, b = self._at(d, t2)
            if tb is None:
                continue                                  # تا پایان بازه معامله‌ای نداشته
            dl = {c: (b.get(c) or 0) - (a.get(c) or 0) for c in CUM}
            if not dl["تعداد معاملات"] and scope != "all_symbols":
                continue                                  # در این بازه معامله‌ای نداشته
            vwap = dl["ارزش معاملات"] / dl["حجم معاملات"] if dl["حجم معاملات"] else None
            seg = [i for i, t in enumerate(d["t"]) if t1 <= t <= t2]
            lasts = [d["آخرین معامله"][i] for i in seg if d["آخرین معامله"][i]] if "آخرین معامله" in d else []
            y = b.get("قیمت دیروز")
            p0 = a.get("آخرین معامله") or y      # قبل از اولین معامله، مبنا قیمت دیروز است
            r = {
                "نماد": d["sym"], "insCode": code, "نام": meta.get("نام"), "نوع": meta.get("نوع"),
                "گروه": meta.get("گروه"),
                "ثبت شروع": _hms(ta) if ta is not None else "قبل از اولین معامله", "ثبت پایان": _hms(tb),
                "تعداد معاملات": dl["تعداد معاملات"], "حجم معاملات": dl["حجم معاملات"],
                "ارزش معاملات": dl["ارزش معاملات"], "میانگین قیمت معاملات (VWAP)": round(vwap, 1) if vwap else None,
                "قیمت شروع": p0, "قیمت پایان": b.get("آخرین معامله"),
                "تغییر قیمت در بازه (درصد)": pct(b.get("آخرین معامله"), p0) if p0 else None,
                "درصد نسبت به دیروز (پایان بازه)": pct(b.get("آخرین معامله"), y),
                "قیمت پایانی (پایان بازه)": b.get("قیمت پایانی"),
                "بیشترین قیمت دیده‌شده": max(lasts) if lasts else None,
                "کمترین قیمت دیده‌شده": min(lasts) if lasts else None,
                "حجم خرید حقیقی": dl["حجم خرید حقیقی"], "حجم فروش حقیقی": dl["حجم فروش حقیقی"],
                "حجم خرید حقوقی": dl["حجم خرید حقوقی"], "حجم فروش حقوقی": dl["حجم فروش حقوقی"],
                "خریداران حقیقی جدید": dl["تعداد خرید حقیقی"], "فروشندگان حقیقی جدید": dl["تعداد فروش حقیقی"],
                "_bI": dl["حجم خرید حقیقی"] * (vwap or 0), "_sI": dl["حجم فروش حقیقی"] * (vwap or 0),
                "_bIc": dl["تعداد خرید حقیقی"], "_sIc": dl["تعداد فروش حقیقی"],
            }
            r["ورود پول حقیقی"] = r["_bI"] - r["_sI"] if vwap else None
            r["ورود پول حقوقی"] = (dl["حجم خرید حقوقی"] - dl["حجم فروش حقوقی"]) * vwap if vwap else None
            r["قدرت خریدار حقیقی (بازه)"] = self._power(r["_bI"], r["_bIc"], r["_sI"], r["_sIc"])
            bc = {c: b.get(c) or 0 for c in CUM}
            r["قدرت خریدار حقیقی (از اول روز)"] = self._power(bc["حجم خرید حقیقی"], bc["تعداد خرید حقیقی"],
                                                              bc["حجم فروش حقیقی"], bc["تعداد فروش حقیقی"])
            r["سهم حقیقی از خرید (درصد)"] = round(100 * dl["حجم خرید حقیقی"] / dl["حجم معاملات"], 1) if dl["حجم معاملات"] else None
            if b.get("موقعیت‌های باز") is not None and a.get("موقعیت‌های باز") is not None:
                r["تغییر موقعیت‌های باز"] = b["موقعیت‌های باز"] - a["موقعیت‌های باز"]
            r["تقاضا1 حجم (پایان)"], r["عرضه1 حجم (پایان)"] = b.get("تقاضا1 حجم"), b.get("عرضه1 حجم")
            rows.append(r)

        if group_by in ("گروه", "نوع", "total"):
            agg = {}
            for r in rows:
                k = "کل" if group_by == "total" else (r.get(group_by) or "نامشخص")
                g = agg.setdefault(k, {"گروه/دسته": k, "تعداد نماد": 0, **{c: 0.0 for c in (
                    "تعداد معاملات", "حجم معاملات", "ارزش معاملات", "_bI", "_sI", "_bIc", "_sIc", "_net", "_netN")},
                    "_up": 0, "_dn": 0})
                g["تعداد نماد"] += 1
                for c in ("تعداد معاملات", "حجم معاملات", "ارزش معاملات", "_bI", "_sI", "_bIc", "_sIc"):
                    g[c] += r[c] or 0
                g["_net"] += r["ورود پول حقیقی"] or 0
                g["_netN"] += r["ورود پول حقوقی"] or 0
                ch = r["تغییر قیمت در بازه (درصد)"]
                g["_up"] += 1 if ch and ch > 0 else 0
                g["_dn"] += 1 if ch and ch < 0 else 0
            rows = []
            for g in agg.values():
                g["ورود پول حقیقی"], g["ورود پول حقوقی"] = g.pop("_net"), g.pop("_netN")
                g["قدرت خریدار حقیقی (بازه)"] = self._power(g["_bI"], g["_bIc"], g["_sI"], g["_sIc"])
                g["مثبت در بازه"], g["منفی در بازه"] = g.pop("_up"), g.pop("_dn")
                rows.append(g)
        for r in rows:
            for k in [k for k in r if k.startswith("_")]:
                r.pop(k)
        key = sort if rows and sort in rows[0] else "ارزش معاملات"
        rows.sort(key=lambda r: (num(r.get(key)) is None, -(num(r.get(key)) or 0) if desc else (num(r.get(key)) or 0)))
        total_val = sum(num(r.get("ارزش معاملات")) or 0 for r in rows)
        return {"day": day, "from": _hms(t1), "to": _hms(t2), "group_by": group_by,
                "archive_span": [_hms(data["first"]), _hms(data["last"])], "count": len(rows),
                "total_value": total_val, "rows": rows[:max(1, min(int(limit or 200), 3000))],
                "notes": ["هر نماد حدوداً هر ۶۰ تا ۹۰ ثانیه یک ثبت دارد؛ «ثبت شروع/پایان» زمان واقعی ثبت‌های استفاده‌شده است.",
                          "قدرت خریدار بازه‌ای تقریبی است: «خریداران جدید» تعداد افراد تازه از اول روز است، نه تعداد معامله.",
                          "ورود پول در بازه = (خرید − فروش) × میانگین قیمت معاملات بازه."]}

    @staticmethod
    def _power(bv, bc, sv, sc):
        try:
            return round((bv / bc) / (sv / sc), 3) if bv and bc and sv and sc else None
        except ZeroDivisionError:
            return None


APP_HTML = r"""<!doctype html><html lang="fa" dir="rtl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>تحلیل بازه‌ای بورس تهران</title>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;700&display=swap">
<style>
/* layout: نوار فرمان بالای صفحه، جدول نتایج زیر آن؛ اعداد با ارقام هم‌عرض */
:root{--bg:#f6f7f5;--panel:#ffffff;--fg:#1d2321;--muted:#5d6864;--line:#dfe3e0;--accent:#0f766e;--accent-2:#e6f2f0;
--up:#15803d;--down:#b91c1c;--font:"Vazirmatn",Tahoma,"Segoe UI",sans-serif;color-scheme:light}
@media (prefers-color-scheme:dark){:root{--bg:#121615;--panel:#1a201e;--fg:#e6ebe9;--muted:#9aa6a1;--line:#2c3532;
--accent:#3cc4b4;--accent-2:#173330;--up:#4ade80;--down:#f87171;color-scheme:dark}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.6 var(--font)}
.wrap{max-width:1400px;margin:0 auto;padding-block:20px 40px;padding-inline:16px;display:grid;gap:16px}
h1{font-size:20px;margin:0;font-weight:700}.sub{color:var(--muted);font-size:13px;margin:2px 0 0}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;align-items:end}
label{display:grid;gap:4px;font-size:12px;color:var(--muted)}
input,select,button{font:inherit;color:var(--fg);background:var(--bg);border:1px solid var(--line);border-radius:8px;padding:7px 9px;min-width:0}
input:focus,select:focus,button:focus{outline:2px solid var(--accent);outline-offset:1px}
button{background:var(--accent);color:#fff;border-color:var(--accent);cursor:pointer;font-weight:500}
button.ghost{background:transparent;color:var(--accent)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
.chips{display:flex;gap:6px;flex-wrap:wrap}.chip{border:1px solid var(--line);border-radius:999px;padding:3px 10px;font-size:12px;cursor:pointer;user-select:none}
.chip[aria-pressed=true]{background:var(--accent-2);border-color:var(--accent);color:var(--accent)}
.summary{display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:var(--muted)}.summary b{color:var(--fg);font-variant-numeric:tabular-nums}
.tablebox{overflow-x:auto;border:1px solid var(--line);border-radius:10px;background:var(--panel)}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;font-size:13px}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);white-space:nowrap;text-align:left}
th{position:sticky;top:0;background:var(--panel);color:var(--muted);font-weight:500;cursor:pointer;text-align:right}
td:first-child,th:first-child{text-align:right;font-weight:500}
tr:hover td{background:var(--accent-2)}.pos{color:var(--up)}.neg{color:var(--down)}
.note{color:var(--muted);font-size:12px}.err{color:var(--down)}
</style></head><body><div class="wrap">
<div><h1>تحلیل بازه‌ای بورس تهران</h1><p class="sub" id="sub">از روی آرشیو دقیقه‌ای ربات؛ بین هر دو زمان دلخواه</p></div>
<div class="panel grid">
<label for="day">روز<select id="day"></select></label>
<label for="from">از ساعت<input id="from" type="time" step="60" value="09:00"></label>
<label for="to">تا ساعت<input id="to" type="time" step="60" value="12:30"></label>
<label for="group_by">نمایش بر اساس<select id="group_by"><option value="symbol">هر نماد</option><option value="گروه">صنعت</option><option value="نوع">نوع اوراق</option><option value="total">کل (یک ردیف)</option></select></label>
<label for="kind">نوع<select id="kind"><option value="">همه</option><option>سهم</option><option>صندوق</option><option>اختیار معامله</option><option>اوراق بدهی</option></select></label>
<label for="group">صنعت<select id="group"><option value="">همهٔ صنایع</option></select></label>
<label for="symbols">نمادها (با ویرگول جدا کنید)<input id="symbols" placeholder="مثلاً خودرو، فملی، شپنا"></label>
<label for="sort">مرتب‌سازی<select id="sort"></select></label>
<div class="row"><button id="run">نمایش</button><button class="ghost" id="copy">کپی جدول</button></div>
</div>
<div class="panel"><div class="row" style="justify-content:space-between"><span class="note">ستون‌ها:</span></div><div class="chips" id="cols"></div></div>
<div class="summary" id="summary"></div>
<div class="tablebox"><table id="tbl"><thead></thead><tbody></tbody></table></div>
<p class="note" id="notes"></p>
</div>
<script>
const SORTS=["ارزش معاملات","تعداد معاملات","حجم معاملات","ورود پول حقیقی","قدرت خریدار حقیقی (بازه)","قدرت خریدار حقیقی (از اول روز)","تغییر قیمت در بازه (درصد)","درصد نسبت به دیروز (پایان بازه)","تغییر موقعیت‌های باز"];
const ALL=["نماد","گروه/دسته","نام","گروه","نوع","تعداد نماد","ثبت شروع","ثبت پایان","تعداد معاملات","حجم معاملات","ارزش معاملات","میانگین قیمت معاملات (VWAP)","قیمت شروع","قیمت پایان","تغییر قیمت در بازه (درصد)","درصد نسبت به دیروز (پایان بازه)","بیشترین قیمت دیده‌شده","کمترین قیمت دیده‌شده","قدرت خریدار حقیقی (بازه)","قدرت خریدار حقیقی (از اول روز)","ورود پول حقیقی","ورود پول حقوقی","حجم خرید حقیقی","حجم فروش حقیقی","خریداران حقیقی جدید","فروشندگان حقیقی جدید","سهم حقیقی از خرید (درصد)","مثبت در بازه","منفی در بازه","تغییر موقعیت‌های باز","تقاضا1 حجم (پایان)","عرضه1 حجم (پایان)"];
const DEF=new Set(["نماد","گروه/دسته","گروه","تعداد نماد","تعداد معاملات","ارزش معاملات","تغییر قیمت در بازه (درصد)","قدرت خریدار حقیقی (بازه)","قدرت خریدار حقیقی (از اول روز)","ورود پول حقیقی","مثبت در بازه","منفی در بازه"]);
let shown=new Set(DEF);try{const s=JSON.parse(localStorage.getItem("cols")||"null");if(s)shown=new Set(s)}catch(e){}
const $=id=>document.getElementById(id);let last=null,desc=true;
SORTS.forEach(s=>$("sort").add(new Option(s,s)));
function chips(){const c=$("cols");c.innerHTML="";ALL.forEach(k=>{const b=document.createElement("span");b.className="chip";b.textContent=k;b.setAttribute("role","button");b.tabIndex=0;b.setAttribute("aria-pressed",shown.has(k));b.onclick=()=>{shown.has(k)?shown.delete(k):shown.add(k);try{localStorage.setItem("cols",JSON.stringify([...shown]))}catch(e){};chips();render()};c.appendChild(b)})}
const fa=new Intl.NumberFormat("fa-IR",{maximumFractionDigits:2});
function fmt(k,v){if(v===null||v===undefined||v==="")return"–";if(typeof v!=="number")return v;
 if(/ارزش|ورود پول/.test(k)&&Math.abs(v)>=1e9)return fa.format(v/1e9)+" میلیارد";
 if(/حجم/.test(k)&&Math.abs(v)>=1e6)return fa.format(v/1e6)+" میلیون";return fa.format(v)}
function cls(k,v){if(typeof v!=="number")return"";if(/درصد|ورود پول|تغییر/.test(k))return v>0?"pos":v<0?"neg":"";if(/قدرت/.test(k))return v>1?"pos":v<1?"neg":"";return""}
function render(){if(!last||!last.rows)return;const cols=ALL.filter(k=>shown.has(k)&&last.rows.some(r=>k in r));
 $("tbl").tHead.innerHTML="<tr>"+cols.map(k=>`<th data-k="${k}">${k}</th>`).join("")+"</tr>";
 $("tbl").tBodies[0].innerHTML=last.rows.map(r=>"<tr>"+cols.map(k=>`<td class="${cls(k,r[k])}">${fmt(k,r[k])}</td>`).join("")+"</tr>").join("");
 [...$("tbl").tHead.querySelectorAll("th")].forEach(th=>th.onclick=()=>{const k=th.dataset.k;if($("sort").value===k)desc=!desc;else{if(![...$("sort").options].some(o=>o.value===k))$("sort").add(new Option(k,k));$("sort").value=k;desc=true}run()})}
async function run(){const p=new URLSearchParams({day:$("day").value,from:$("from").value,to:$("to").value,group_by:$("group_by").value,kind:$("kind").value,group:$("group").value,symbols:$("symbols").value,sort:$("sort").value,desc:desc?"1":"0",limit:"1000"});
 $("summary").innerHTML="در حال محاسبه…";try{const r=await fetch("api/range?"+p);last=await r.json()}catch(e){$("summary").innerHTML='<span class="err">ارتباط با ربات برقرار نشد. آدرس را از README مخزن دوباره بگیرید.</span>';return}
 if(last.error){$("summary").innerHTML=`<span class="err">${last.error}</span>`;return}
 $("summary").innerHTML=`<span>روز <b>${last.day}</b></span><span>بازه <b>${last.from}</b> تا <b>${last.to}</b></span><span>ردیف‌ها <b>${fa.format(last.count)}</b></span><span>ارزش کل معاملات بازه <b>${fmt("ارزش",last.total_value)}</b> ریال</span><span>آرشیو این روز <b>${last.archive_span[0]}</b> تا <b>${last.archive_span[1]}</b></span>`;
 $("notes").textContent=(last.notes||[]).join(" ");render()}
async function init(){chips();try{const m=await (await fetch("api/meta")).json();
 m.days.forEach(d=>$("day").add(new Option((m.jalali&&m.jalali[d]?m.jalali[d]+"  ("+d+")":d),d)));m.groups.forEach(g=>$("group").add(new Option(g,g)));
 $("sub").textContent=`از روی آرشیو دقیقه‌ای ربات (${m.days.length} روز). زمان‌ها به وقت تهران.`}catch(e){$("summary").innerHTML='<span class="err">ارتباط با ربات برقرار نشد.</span>'}
 const now=new Date();run()}
$("run").onclick=run;["day","group_by","kind","group","sort"].forEach(id=>$(id).onchange=run);$("symbols").onkeydown=e=>{if(e.key==="Enter")run()};
$("copy").onclick=async()=>{if(!last||!last.rows)return;const cols=ALL.filter(k=>shown.has(k)&&last.rows.some(r=>k in r));
 const t=[cols.join("\t"),...last.rows.map(r=>cols.map(k=>r[k]??"").join("\t"))].join("\n");try{await navigator.clipboard.writeText(t);$("copy").textContent="کپی شد"}catch(e){$("copy").textContent="کپی نشد"}setTimeout(()=>$("copy").textContent="کپی جدول",1500)};
init();
</script></body></html>"""


class LiveHandler(BaseHTTPRequestHandler):
    market = None
    gh_repo = None
    ranges = None
    server_version = "tse-bot"

    def log_message(self, fmt, *args):
        log.debug("live %s - %s", self.headers.get("X-Forwarded-For", self.client_address[0]), fmt % args)

    def _base(self):
        host = self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or f"127.0.0.1:{LIVE_PORT}"
        scheme = "http" if host.startswith(("127.", "localhost")) else "https"
        return f"{scheme}://{host}"

    def _send(self, code, body, ctype):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", f"{ctype}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):  # noqa: N802
        try:
            self._route()
        except Exception:  # noqa: BLE001
            log.exception("خطا در پاسخ به %s", self.path)
            self._send(500, "internal error", "text/plain")

    def _route(self):
        u = urlparse(self.path)
        parts = [unquote(p) for p in u.path.split("/") if p]
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        base = self._base()

        def link(sym):
            return f"{base}/symbol/{quote(sym)}"

        if parts[:1] == ["robots.txt"]:
            return self._send(200, "User-agent: *\nAllow: /\n", "text/plain")
        if parts[:1] == ["health"]:
            m = self.market
            return self._send(200, json.dumps({
                "ok": bool(m.rows) and not m.last_error, "version": VERSION, "symbols": len(m.rows),
                "data_is_from_previous_session": bool(m.status_note and m.rows),
                **time_meta(m.fetched, now_ts()), "last_error": m.last_error, "status_note": m.status_note,
                "last_success_utc": iso_utc(m.last_success) if m.last_success else None,
                "alerts": m.alerts.status() if getattr(m, "alerts", None) else None,
                "feeds": m.feeds.status() if getattr(m, "feeds", None) else None,
                "tsetmc_blocked_until_utc": iso_utc(_guard["cool_until"] + _clock["offset"]) if blocked() else None,
            }, ensure_ascii=False, default=str), "application/json")
        if parts[:1] == ["app"]:
            data = APP_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
            return
        if parts[:2] == ["app", "api"] or parts[:1] == ["api"]:
            sub = parts[2] if parts[:1] == ["app"] else (parts[1] if len(parts) > 1 else "")
            return self._api(sub, q)
        rows, fetched = self.market.get(LIVE_MAX_AGE)
        meta = time_meta(fetched, now_ts())
        if not rows:
            return self._send(503, "# داده‌ای در دسترس نیست\n\nربات هنوز داده‌ای از بورس نگرفته است.",
                              "text/markdown")

        if not parts:
            return self._send(200, self._index(base, meta), "text/markdown")
        if parts[0] == "summary":
            return self._send(200, md_summary(rows, meta, link, indices=self.market.indices), "text/markdown")
        if parts[0] in ("codal", "news", "macro", "alerts") and getattr(self.market, "feeds", None) is not None:
            if parts[0] == "alerts":
                return self._send(200, self.market.alerts.alerts_csv() if self.market.alerts else "", "text/csv")
            body = self.market.feeds.files().get(f"data/{parts[0]}.json", "{}")
            return self._send(200, body, "application/json")
        if parts[0] == "indices":
            ind_meta = time_meta(self.market.indices_fetched, now_ts())
            return self._send(200, md_indices(self.market.indices, ind_meta), "text/markdown")
        if parts[0] == "all":
            return self._send(200, md_all(rows, meta, link), "text/markdown")
        if parts[0] == "csv":
            return self._send(200, to_csv(rows), "text/csv")
        if parts[0] == "top" and len(parts) > 1:
            field, asc = parts[1], q.get("asc") == "1"
            n = int(q.get("n", 30))
            kinds = None if q.get("all") == "1" else EQ_KINDS
            body = (f"# {'کمترین' if asc else 'بیشترین'} «{field}»\n\n" + time_block_md(meta) + "\n\n"
                    + _table(top_rows(rows, field, asc, n, num(q.get("min_value")) or 0, kinds),
                             BASE_COLS + ([field] if field not in BASE_COLS else []), link))
            return self._send(200, body, "text/markdown")
        if parts[0] in ("symbol", "history") and len(parts) > 1:
            r = self.market.find(parts[1])
            if not r:
                hits = [x for x in rows if norm_fa(parts[1]) in x["نماد"] or norm_fa(parts[1]) in norm_fa(x.get("نام"))]
                body = (f"# نماد «{parts[1]}» پیدا نشد\n\n" + time_block_md(meta) + "\n\n"
                        + ("نمادهای مشابه:\n\n" + _table(hits[:30], BASE_COLS, link) if hits else
                           f"فهرست همهٔ نمادها: {base}/all"))
                return self._send(404, body, "text/markdown")
            hist = self.market.history.get(r["نماد"])
            if parts[0] == "history":
                return self._send(200, json.dumps({"_زمان": meta, "نماد": r["نماد"], "تاریخچه امروز": hist},
                                                  ensure_ascii=False, indent=1, default=str), "application/json")
            return self._send(200, json.dumps(symbol_doc(r, meta, hist), ensure_ascii=False, indent=1,
                                              default=str), "application/json")
        return self._send(404, f"# مسیر نامعتبر\n\nراهنما: {base}/", "text/markdown")

    def _api(self, sub, q):
        rs = self.ranges
        if sub == "meta":
            days = rs.days()
            day_static = rs._static(days[0]) if days else {}
            groups = sorted({norm_fa(v.get("گروه")) for v in self.market.static.data.values() if v.get("گروه")} |
                            {norm_fa(r.get("گروه")) for r in self.market.rows if r.get("گروه")} |
                            {norm_fa(v.get("گروه")) for v in day_static.values() if v.get("گروه")})
            jal = {}
            for d in days:
                try:
                    jy, jm, jd = gregorian_to_jalali(int(d[:4]), int(d[5:7]), int(d[8:10]))
                    jal[d] = f"{jy}/{jm:02d}/{jd:02d}"
                except ValueError:
                    pass
            return self._send(200, json.dumps({"days": days, "jalali": jal, "groups": groups, "version": VERSION},
                                              ensure_ascii=False), "application/json")
        if sub == "range":
            days = rs.days()
            day = q.get("day") or (days[0] if days else None)
            if not day:
                return self._send(404, json.dumps({"error": "هنوز آرشیوی ثبت نشده است"}, ensure_ascii=False),
                                  "application/json")
            res = rs.query(day, _secs(q.get("from")) if q.get("from") else None,
                           _secs(q.get("to")) if q.get("to") else None,
                           symbols=[x for x in re.split(r"[,،\s]+", q.get("symbols", "")) if x],
                           group=q.get("group") or None, kind=q.get("kind") or None,
                           group_by=q.get("group_by") or "symbol", sort=q.get("sort") or "ارزش معاملات",
                           desc=q.get("desc", "1") != "0", limit=q.get("limit") or 200)
            return self._send(200, json.dumps(res, ensure_ascii=False, default=str), "application/json")
        return self._send(404, json.dumps({"error": "مسیر نامعتبر", "routes": ["api/meta", "api/range"]},
                                          ensure_ascii=False), "application/json")

    def _index(self, base, meta):
        gh = f"https://github.com/{self.gh_repo}" if self.gh_repo else None
        tops = "\n".join(f"- {t}: {base}/top/{quote(f)}{'?asc=1' if a else ''}"
                         f"{('&' if a else '?') + 'min_value=' + str(int(mv)) if mv else ''}"
                         for t, f, a, mv in TOP_LISTS)
        return f"""# داده‌های زندهٔ بورس تهران (اتصال مستقیم به ربات)

{time_block_md(meta)}

هر درخواست، اگر داده بیش از {LIVE_MAX_AGE} ثانیه عمر داشته باشد، همان لحظه از سرور بورس تازه می‌شود.
اگر `served_at_utc` یک پاسخ قدیمی بود، ابزار خواندن صفحه نسخهٔ کش‌شده را برگردانده است؛
همان آدرس را با یک پسوند تازه دوباره بخوان، مثلاً `?r=2`، `?r=3` و ... (سرور پسوند را نادیده می‌گیرد).

## صفحه‌ها
- 📊 برنامهٔ تحلیل بازه‌ای (بین هر دو زمان، هر نماد/صنعت/کل بازار): {base}/app
- همان تحلیل به‌صورت API برای Claude: {base}/api/range?day=YYYY-MM-DD&from=10:00&to=10:10&symbols=خودرو
  (پارامترها: day، from، to، symbols با ویرگول، group=نام صنعت، kind=سهم|صندوق|اختیار معامله، group_by=symbol|گروه|نوع|total، sort=نام ستون، limit) — فهرست روزها و صنایع: {base}/api/meta
- خلاصهٔ بازار و برترین‌ها: {base}/summary
- همهٔ نمادها (با پیوند به صفحهٔ هر نماد): {base}/all
- شاخص‌های بازار (کل، هم‌وزن، صنایع و ...): {base}/indices
- اطلاعیه‌های کدال: {base}/codal | خبرها: {base}/news | دلار، سکه، طلا: {base}/macro | هشدارهای امروز: {base}/alerts
- اطلاعات کامل یک نماد + روند امروز: {base}/symbol/{quote('فملی')}  (به‌جای «فملی» نام نماد دیگر)
- همهٔ ستون‌های همهٔ نمادها (CSV): {base}/csv
- وضعیت ربات: {base}/health

## فهرست‌های آماده
{tops}

{"اگر این آدرس کار نکرد، آدرس‌های جایگزین و داده‌ی پشتیبان در " + gh + " است." if gh else ""}

{COLUMNS_HELP}"""


def start_live_server(market, repo):
    LiveHandler.market, LiveHandler.gh_repo = market, repo
    LiveHandler.ranges = RangeStore(market)
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", LIVE_PORT), LiveHandler)
    except OSError:
        return None
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, name="live", daemon=True).start()
    log.info("سرور زنده روی http://127.0.0.1:%d فعال شد", LIVE_PORT)
    return srv


# ============================================================================
# تونل‌ها (هر کدام جدا؛ اگر قطع شود خودکار دوباره وصل می‌شود)
# ============================================================================
_children = []


@atexit.register
def _kill_children():
    for p in list(_children):
        try:
            p.kill()
        except Exception:  # noqa: BLE001
            pass


def _no_window():
    return 0x08000000 if os.name == "nt" else 0  # CREATE_NO_WINDOW


def cloudflared_path():
    tools = os.path.join(app_dir(), "tools")
    if os.name == "nt":
        names, asset = ["cloudflared.exe"], "cloudflared-windows-amd64.exe"
    elif sys.platform == "darwin":
        return shutil.which("cloudflared")
    else:
        names, asset = ["cloudflared"], "cloudflared-linux-amd64"
    for n in names:
        p = os.path.join(tools, n)
        if os.path.exists(p):
            return p
    if shutil.which("cloudflared"):
        return shutil.which("cloudflared")
    # دانلود (فقط اگر همراه برنامه نبود)
    url = f"https://github.com/cloudflare/cloudflared/releases/latest/download/{asset}"
    dest = os.path.join(tools, names[0])
    try:
        log.info("دانلود cloudflared ...")
        os.makedirs(tools, exist_ok=True)
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(dest + ".part", "wb") as f:
                for chunk in r.iter_content(1 << 16):
                    f.write(chunk)
        os.replace(dest + ".part", dest)
        if os.name != "nt":
            os.chmod(dest, 0o755)
        return dest
    except Exception as e:  # noqa: BLE001
        log.warning("دانلود cloudflared ناموفق بود: %s", e)
        return None


class Tunnel(threading.Thread):
    BAD_URLS = ("https://api.trycloudflare.com", "https://admin.localhost.run", "https://localhost.run",
                "https://console.serveo.net", "https://serveo.net", "https://pinggy.io", "https://dashboard.pinggy.io")

    def __init__(self, name, cmd_fn, url_re, priority, url_poller=None):
        super().__init__(name=f"tunnel-{name}", daemon=True)
        self.tname, self.cmd_fn, self.url_re, self.priority = name, cmd_fn, re.compile(url_re), priority
        self.url_poller = url_poller
        self.url = None
        self.since = None
        self.last_ok = None
        self.check_ok = None
        self.restarts = 0
        self.proc = None

    def run(self):
        backoff = 10
        while True:
            cmd = self.cmd_fn()
            if not cmd:
                log.warning("تونل %s در دسترس نیست (ابزار لازم پیدا نشد)", self.tname)
                return
            started = time.time()
            try:
                log.info("راه‌اندازی تونل %s ...", self.tname)
                self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                             stdin=subprocess.DEVNULL, text=True, encoding="utf-8",
                                             errors="replace", creationflags=_no_window())
                _children.append(self.proc)
                if self.url_poller:
                    threading.Thread(target=self._poll_url, args=(self.proc,), daemon=True,
                                     name=f"poll-{self.tname}").start()
                for line in self.proc.stdout:
                    line = line.strip()
                    if line and "\x1b[" not in line:      # کدهای QR و رنگی را ثبت نکن
                        log.debug("[%s] %s", self.tname, line[:300])
                    for m in self.url_re.finditer(line):
                        self._set_url(m.group(0))
                self.proc.wait()
            except Exception:  # noqa: BLE001
                log.exception("خطا در تونل %s", self.tname)
            finally:
                if self.proc in _children:
                    _children.remove(self.proc)
            self.url, self.check_ok = None, None
            self.restarts += 1
            backoff = 10 if time.time() - started > 300 else min(backoff * 2, 600)
            log.warning("تونل %s قطع شد؛ %d ثانیه دیگر دوباره وصل می‌شود", self.tname, backoff)
            time.sleep(backoff)

    def _set_url(self, url):
        url = url.rstrip("/.,")
        if url in self.BAD_URLS or url.startswith(("https://console.", "https://admin.", "https://dashboard.")) \
                or url == self.url:
            return
        self.url, self.since, self.check_ok = url, now_ts(), None
        log.info("تونل %s فعال شد: %s", self.tname, self.url)

    def _poll_url(self, proc):
        """برای تونل‌هایی که آدرس را چاپ نمی‌کنند (Pinggy)، آدرس از صفحهٔ وضعیت محلی‌شان خوانده می‌شود."""
        while proc.poll() is None:
            try:
                u = self.url_poller()
                if u:
                    self._set_url(u)
            except Exception:  # noqa: BLE001
                pass
            time.sleep(5 if not self.url else 60)

    def restart(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()

    def info(self):
        verified = bool(self.check_ok and self.last_ok and now_ts() - self.last_ok < 180)
        return {"name": self.tname, "url": self.url, "priority": self.priority, "verified_recently": verified,
                "connected_since_utc": iso_utc(self.since), "last_check_ok_utc": iso_utc(self.last_ok),
                "last_check_passed": self.check_ok}


def make_tunnels(names):
    ssh = shutil.which("ssh")
    known = os.path.join(app_dir(), "tools", "known_hosts")
    os.makedirs(os.path.dirname(known), exist_ok=True)
    ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", f"UserKnownHostsFile={known}",
                "-o", "ServerAliveInterval=20", "-o", "ServerAliveCountMax=3", "-o", "ExitOnForwardFailure=yes",
                "-o", "ConnectTimeout=20", "-T"]
    pinggy_web = LIVE_PORT + 1

    def pinggy_url():
        r = requests.get(f"http://127.0.0.1:{pinggy_web}/urls", timeout=5)
        urls = (r.json() or {}).get("urls") or []
        https = [u for u in urls if u.startswith("https://")]
        return (https or [None])[0]

    defs = {
        "localhost.run": dict(cmd_fn=lambda: [ssh, *ssh_opts, "-R", f"80:127.0.0.1:{LIVE_PORT}", "nokey@localhost.run"]
                              if ssh else None, url_re=r"https://[a-z0-9]+\.lhr\.life"),
        "pinggy": dict(cmd_fn=lambda: [ssh, "-p", "443", *[o for o in ssh_opts if o != "-T"], "-tt",
                                       "-R", f"0:localhost:{LIVE_PORT}", "-L", f"{pinggy_web}:localhost:4300",
                                       "a.pinggy.io"] if ssh else None,
                       url_re=r"https://[a-z0-9-]+\.[a-z.]*pinggy\.(?:link|online)", url_poller=pinggy_url),
        "serveo": dict(cmd_fn=lambda: [ssh, *ssh_opts, "-R", f"80:127.0.0.1:{LIVE_PORT}", "serveo.net"]
                       if ssh else None, url_re=r"https://[a-z0-9-]+\.serveo(?:usercontent)?\.(?:net|com)"),
        "cloudflared": dict(cmd_fn=lambda: (lambda p: [p, "tunnel", "--url", f"http://127.0.0.1:{LIVE_PORT}",
                                                       "--no-autoupdate", "--protocol", "http2"] if p else None)(
            cloudflared_path()), url_re=r"https://[a-z0-9-]+\.trycloudflare\.com"),
    }
    return [Tunnel(n, priority=i + 1, **defs[n]) for i, n in enumerate(names) if n in defs]


def health_loop(tunnels):
    """هر ۶۰ ثانیه هر تونل را از مسیر عمومی‌اش امتحان می‌کند."""
    s = requests.Session()
    fails = {}
    while True:
        time.sleep(60)
        for t in tunnels:
            if not t.url:
                continue
            try:
                r = s.get(t.url + "/health", timeout=20)
                ok = r.status_code == 200 and (r.json() or {}).get("version") == VERSION
            except (requests.RequestException, ValueError):
                ok = False
            t.check_ok = ok
            if ok:
                t.last_ok, fails[t.tname] = now_ts(), 0
                t.ever_ok_url = t.url
            else:
                fails[t.tname] = fails.get(t.tname, 0) + 1
                # بعضی تونل‌ها از داخل ایران قابل بررسی نیستند ولی از بیرون کار می‌کنند؛
                # پس بررسی ناموفق فقط ثبت می‌شود و تونل قطع نمی‌شود
                if getattr(t, "ever_ok_url", None) == t.url and fails[t.tname] >= 3:
                    # این تونل قبلاً از داخل قابل بررسی بود و حالا جواب نمی‌دهد → واقعاً خراب است
                    fails[t.tname] = 0
                    log.warning("تونل %s دیگر جواب نمی‌دهد؛ دوباره وصل می‌شود", t.tname)
                    t.url, t.check_ok = None, None
                    t.restart()
                elif fails[t.tname] in (1, 10) or fails[t.tname] % 60 == 0:
                    log.info("بررسی تونل %s از داخل ایران ناموفق (%d بار)؛ تونل فعال می‌ماند",
                             t.tname, fails[t.tname])


# ============================================================================
# انتشار روی GitHub (پشتیبان + دفترچهٔ آدرس)
# ============================================================================
class GitHubPublisher:
    def __init__(self, repo, token, branch="main"):
        self.repo, self.branch = repo, branch
        self.base = f"{GH_API}/repos/{repo}"
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                               "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "tse-bot"})
        self.tree_sha = None
        self.head_sha = None
        self.hashes = {}
        self.disabled_until = 0

    def _call(self, method, path, **kw):
        url = f"{self.base}{path}"
        for attempt in range(1, 4):
            try:
                r = self.s.request(method, url, timeout=120, **kw)
            except requests.RequestException as e:
                log.warning("GitHub %s %s ناموفق (تلاش %d): %s", method, path, attempt, e)
                time.sleep(5 * attempt)
                continue
            if r.headers.get("Date"):
                _sync_clock(r.headers["Date"])
            if r.status_code < 300:
                return r.json()
            msg = r.text[:400]
            if r.status_code == 401:
                log.error("GitHub: توکن نامعتبر یا منقضی است (401). %s", msg)
                self.disabled_until = time.time() + 600
                return None
            if r.status_code in (403, 429) and ("rate limit" in msg.lower() or "abuse" in msg.lower()):
                wait = int(r.headers.get("retry-after") or 60)
                log.warning("GitHub: محدودیت تعداد درخواست؛ %d ثانیه صبر می‌شود. %s", wait, msg)
                self.disabled_until = time.time() + wait
                return None
            if r.status_code in (403, 404):
                log.error("GitHub: دسترسی ندارد (%s). توکن باید به مخزن %s با دسترسی "
                          "Contents: Read and write داده شده باشد. %s", r.status_code, self.repo, msg)
                self.disabled_until = time.time() + 600
                return None
            log.warning("GitHub %s %s → HTTP %s (تلاش %d): %s", method, path, r.status_code, attempt, msg)
            time.sleep(5 * attempt)
        return None

    def _load_base(self):
        """درخت فعلی مخزن را می‌گیرد تا فایل‌های قبلی (مثل آرشیو روزهای گذشته) پاک نشوند."""
        ref = self._call("GET", f"/git/ref/heads/{self.branch}")
        if not ref:
            return False
        commit = self._call("GET", f"/git/commits/{ref['object']['sha']}")
        if not commit:
            return False
        self.tree_sha, self.hashes = commit["tree"]["sha"], {}
        self.head_sha = ref["object"]["sha"]
        log.info("درخت فعلی مخزن GitHub دریافت شد")
        return True

    def upload_release(self, tag, title, assets):
        """آرشیو بلندمدت: فایل‌ها به‌عنوان asset در Release ماهانه (بدون محدودیت حجم کل مخزن)."""
        try:
            r = self.s.get(f"{self.base}/releases/tags/{tag}", timeout=60)
            if r.status_code == 404:
                r = self.s.post(f"{self.base}/releases", timeout=60, json={
                    "tag_name": tag, "name": title, "target_commitish": self.branch,
                    "body": "آرشیو روزانهٔ داده‌های بورس تهران. هر فایل یک روز معاملاتی است (CSV فشرده با xz)."})
            if r.status_code >= 300:
                log.warning("GitHub Release %s: HTTP %s %s", tag, r.status_code, r.text[:300])
                return False
            rel = r.json()
            existing = {a["name"] for a in rel.get("assets", [])}
            upload = rel["upload_url"].split("{")[0]
            for name, data in assets.items():
                if name in existing:
                    continue
                u = self.s.post(upload, params={"name": name}, data=data, timeout=300,
                                headers={"Content-Type": "application/x-xz"})
                if u.status_code >= 300:
                    log.warning("آپلود %s در Release ناموفق: HTTP %s %s", name, u.status_code, u.text[:300])
                    return False
            log.info("آرشیو در Release %s ذخیره شد (%s)", tag, ", ".join(assets))
            return True
        except requests.RequestException as e:
            log.warning("GitHub Release %s ناموفق: %s", tag, e)
            return False

    @staticmethod
    def _bytes(v):
        return v if isinstance(v, bytes) else v.encode("utf-8")

    def publish(self, files):
        """files: {مسیر: متن | bytes | None(=حذف)}"""
        if time.time() < self.disabled_until:
            return False
        if self.tree_sha and self.head_sha:
            # اگر کس دیگری (مثلاً فایل‌های به‌روزرسانی) در مخزن commit کرده باشد، درخت از نو خوانده می‌شود تا پاک نشود
            ref = self._call("GET", f"/git/ref/heads/{self.branch}")
            if ref and ref["object"]["sha"] != self.head_sha:
                log.info("مخزن از بیرون تغییر کرده؛ درخت دوباره خوانده می‌شود")
                self.tree_sha = None
        if not self.tree_sha and not self._load_base():
            log.warning("GitHub: درخت فعلی مخزن دریافت نشد؛ انتشار به بعد موکول شد")
            return False
        t0 = time.time()
        new_hashes = {p: ("DEL" if c is None else hashlib.sha1(self._bytes(c)).hexdigest()) for p, c in files.items()}
        changed = [p for p in files if self.hashes.get(p) != new_hashes[p]]
        if not changed:
            return True
        size_kb = sum(len(self._bytes(files[p])) for p in changed if files[p] is not None) / 1024
        entries = []
        for p in changed:
            c = files[p]
            if c is None:
                entries.append(({"path": p, "mode": "100644", "type": "blob", "sha": None}, 0))
            elif isinstance(c, bytes):   # فایل فشرده: اول blob ساخته می‌شود
                b = self._call("POST", "/git/blobs", json={"content": base64.b64encode(c).decode(),
                                                           "encoding": "base64"})
                if not b:
                    self.tree_sha = None
                    return False
                entries.append(({"path": p, "mode": "100644", "type": "blob", "sha": b["sha"]}, 0))
            else:
                entries.append(({"path": p, "mode": "100644", "type": "blob", "content": c}, len(c.encode("utf-8"))))
        # درخت‌های بزرگ در چند بخش ساخته می‌شوند (GitHub درخواست‌های خیلی بزرگ را رد می‌کند)
        batches, chunk, budget = [], [], 0
        for e, size in entries:
            chunk.append(e)
            budget += size
            if len(chunk) >= 400 or budget > 1_500_000:
                batches.append(chunk)
                chunk, budget = [], 0
        if chunk:
            batches.append(chunk)
        base, t = self.tree_sha, None
        for batch in batches:
            t = self._call("POST", "/git/trees", json={"tree": batch, "base_tree": base})
            if not t:
                self.tree_sha = None
                return False
            base = t["sha"]
        c = self._call("POST", "/git/commits", json={"message": f"بازار {iso_teh(now_ts())}", "tree": t["sha"],
                                                     "parents": []})
        ref = self._call("GET", f"/git/ref/heads/{self.branch}")
        if ref and self.head_sha and ref["object"]["sha"] != self.head_sha:
            log.info("مخزن هم‌زمان از بیرون تغییر کرد؛ این انتشار در دور بعد انجام می‌شود")
            self.tree_sha = None
            return False
        if not c or self._call("PATCH", f"/git/refs/heads/{self.branch}",
                               json={"sha": c["sha"], "force": True}) is None:
            self.tree_sha = None
            return False
        self.hashes.update(new_hashes)
        self.tree_sha = t["sha"]
        self.head_sha = c["sha"]
        log.info("GitHub: %d فایل (%.0f KB) منتشر شد در %.1f ثانیه", len(changed), size_kb, time.time() - t0)
        return True


class ArchiveSync:
    """آپلود آرشیو روزانه روی GitHub: ساعتی یک بار در طول روز و نسخهٔ نهایی بعد از بسته شدن بازار."""

    def __init__(self, market, pub, repo, keep_days):
        self.m, self.pub, self.repo, self.keep = market, pub, repo, keep_days
        self.state_path = os.path.join(market.archive.dir, "published.json")
        try:
            with open(self.state_path, encoding="utf-8") as f:
                self.state = json.load(f)
        except (OSError, ValueError):
            self.state = {}
        self.last_partial = 0.0

    def _save(self):
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=1)

    def tick(self):
        arch = self.m.archive
        today = datetime.fromtimestamp(now_ts(), TEHRAN).strftime("%Y-%m-%d")
        after_close = not market_open() and datetime.fromtimestamp(now_ts(), TEHRAN).hour >= 13
        files, finals, partials = {}, [], []
        for day in arch.days():
            st = self.state.get(day, {})
            if st.get("final"):
                continue
            final = day < today or after_close
            if not final and time.time() - self.last_partial < 3600:
                continue
            if final and day == today and self.m.rows:
                arch.write_static(day, self.m.rows)
            if final:
                arch.recompress(day)
            for kind in arch.kinds(day):
                path = arch.existing(day, kind)
                if path:
                    with open(path, "rb") as f:
                        files[f"archive/{os.path.basename(path)}"] = f.read()
                if final and st.get("partial") and kind != "_static" and path and path.endswith(".xz"):
                    files[f"archive/{day}{kind}.csv.gz"] = None   # نسخهٔ ناقص قبلی حذف شود
            if final:
                finals.append(day)
            else:
                partials.append(day)
        # روزهای کامل در Release ماهانه هم ذخیره می‌شوند (نگهداری دائمی)
        need_index = False
        for day, st in sorted(self.state.items()):
            if st.get("final") and not st.get("released"):
                assets = {}
                for kind in arch.kinds(day):
                    p = arch.existing(day, kind)
                    if p and p.endswith(".xz"):
                        with open(p, "rb") as f:
                            assets[os.path.basename(p)] = f.read()
                if assets and self.pub.upload_release(f"archive-{day[:7]}", f"آرشیو {day[:7]}", assets):
                    st["released"] = True
                    st["kinds"] = [n[10:-7] for n in assets]
                    need_index = True
                    self._save()
        # فقط N روز آخر در خود مخزن می‌ماند؛ قدیمی‌ترها فقط اگر در Release ذخیره شده باشند حذف می‌شوند
        published_days = sorted(d for d, v in self.state.items() if v.get("final") and not v.get("removed"))
        old = [d for d in (published_days[:-self.keep] if self.keep and len(published_days) > self.keep else [])
               if self.state[d].get("released")]
        for day in old:
            for kind in self.state[day].get("kinds") or ["", "_indices", "_static"]:
                files[f"archive/{day}{kind}.csv.xz"] = None
        if not files and not need_index:
            return
        files["archive/index.md"] = self._index(files, old, finals)
        if self.pub.publish(files):
            for day in finals:
                self.state[day] = {"final": True, "uploaded_at_utc": iso_utc(now_ts())}
            for day in partials:
                self.state[day] = {"partial": True, "uploaded_at_utc": iso_utc(now_ts())}
            for day in old:
                self.state[day]["removed"] = True
            self.last_partial = time.time()
            self._save()
            log.info("آرشیو روی GitHub به‌روز شد (%s)", ", ".join(sorted({p[8:18] for p in files if p[8:18][:2] == "20"})))

    def _index(self, files, removed, finals):
        raw = f"https://raw.githubusercontent.com/{self.repo}/main/archive"
        days = sorted(set(d for d, v in self.state.items() if v.get("final") and not v.get("removed")) |
                      {p[8:18] for p in files if p.startswith("archive/20")} - set(removed))
        rel_days = sorted(d for d, v in self.state.items() if v.get("released"))
        rel = f"https://github.com/{self.repo}/releases/download"
        lines = []
        for d in reversed(days):
            sizes = []
            for kind, label in (("", "معاملات"), ("_indices", "شاخص‌ها"), ("_static", "اطلاعات ثابت"), ("_alerts", "هشدارها"),
                                ("_codal", "کدال"), ("_news", "خبرها"), ("_macro", "دلار و طلا"), ("_verdicts", "حکم‌های ارسال‌شده")):
                lp = self.m.archive.existing(d, kind)
                if lp:
                    sizes.append(f"[{label}]({raw}/{os.path.basename(lp)}) ({os.path.getsize(lp) / 1048576:.2f} MB)")
            status = "کامل" if self.state.get(d, {}).get("final") or d in finals else "در حال تکمیل"
            lines.append(f"- {d}: " + " | ".join(sizes) + f" — {status}")
        stored = len(days)
        if self.keep and stored >= self.keep - 25:
            notice = (f"> **هشدار حذف آرشیو:** الان {stored} روز در این مخزن است و سقف نگهداری {self.keep} روز است. "
                      f"وقتی به سقف برسد، قدیمی‌ترین روزها از این مخزن حذف می‌شوند. این روزها قبلاً در Releases "
                      f"به‌صورت دائمی ذخیره شده‌اند، ولی اگر نسخهٔ کامل می‌خواهید پیش از آن بک‌آپ بگیرید.\n\n")
            log.warning("نزدیک شدن به سقف نگهداری آرشیو در مخزن: %d از %d روز", stored, self.keep)
        else:
            notice = ""
        return notice + f"""# آرشیو روزانهٔ بورس تهران

هر روز معاملاتی چند فایل فشرده دارد (CSV با UTF-8؛ روزهای کامل `.csv.xz`، روز جاری `.csv.gz`):
- **معاملات**: هر {self.m.archive.step} ثانیه در ساعات بازار، فقط نمادهایی که نسبت به ثبت قبلی تغییر کرده‌اند.
  آخرین ردیف هر نماد تا یک زمان مشخص = وضعیت آن نماد در آن لحظه. ستون «زمان دریافت» به وقت تهران است.
- **شاخص‌ها**: مقدار همهٔ شاخص‌ها در همان زمان‌ها.
- **اطلاعات ثابت**: گروه، تعداد سهام، شناوری، میانگین حجم ماه، مشخصات اختیارها و ... در پایان روز.
- **هشدارها، کدال، خبرها، دلار و طلا** (`_alerts`، `_codal`، `_news`، `_macro`): هر مورد با `fetched_at_utc`.

خواندن با Python:
```python
import pandas as pd
df = pd.read_csv("{raw}/2026-10-01.csv.xz")      # pandas نوع فشرده‌سازی را از پسوند تشخیص می‌دهد
```

## روزهای موجود در همین مخزن ({len(days)} روز؛ {self.keep} روز آخر نگه داشته می‌شود)

""" + "\n".join(lines) + f"""

## آرشیو دائمی (GitHub Releases)
همهٔ روزهای کامل، از جمله روزهای قدیمی‌تر از {self.keep} روز، به‌صورت ماهانه در Releases نگه داشته می‌شوند:
https://github.com/{self.repo}/releases

آدرس دانلود مستقیم: `{rel}/archive-YYYY-MM/YYYY-MM-DD.csv.xz` (و `_indices`، `_static`)

""" + "\n".join(f"- {d}: " + " | ".join(
            f"[{label}]({rel}/archive-{d[:7]}/{d}{kind}.csv.xz)"
            for kind, label in (("", "معاملات"), ("_indices", "شاخص‌ها"), ("_static", "اطلاعات ثابت"), ("_alerts", "هشدارها"),
                                ("_codal", "کدال"), ("_news", "خبرها"), ("_macro", "دلار و طلا"), ("_verdicts", "حکم‌های ارسال‌شده"))
            if kind in self.state[d].get("kinds", ("", "_indices", "_static"))) for d in reversed(rel_days)) + """

## نسخهٔ کامل روی کامپیوتر ربات
همهٔ روزها بدون حذف در پوشهٔ `output/archive` کنار برنامه هم ذخیره می‌شوند.
"""


def build_github_files(market, rows, fetched, repo, tunnels, full):
    gh = f"https://github.com/{repo}/blob/main"
    meta = time_meta(fetched)
    published = now_ts()

    def link(sym):
        return f"{gh}/data/symbols/{quote(sym)}.json"

    files = {}
    live = sorted([t.info() for t in tunnels],
                  key=lambda x: (x["url"] is None, x["last_check_passed"] is False, not x["verified_recently"],
                                 x["priority"]))
    files["data/live.json"] = json.dumps({"published_at_utc": iso_utc(published), "endpoints": live,
                                          "bot_version": VERSION}, ensure_ascii=False, indent=1)
    if getattr(market, "alerts", None):
        files["data/alerts.csv"] = market.alerts.alerts_csv()
        files["data/alerts_status.json"] = json.dumps({"published_at_utc": iso_utc(published), **market.alerts.status()},
                                                      ensure_ascii=False, indent=1, default=str)
    if getattr(market, "feeds", None):
        files.update(market.feeds.files())
    files["data/meta.json"] = json.dumps({**meta, "bot_version": VERSION, "instance": instance_info(), "nav": market.nav.stats(), "published_at_utc": iso_utc(published), "last_error": market.last_error,
                                          "status_note": market.status_note,
                                          "symbols": len(rows)}, ensure_ascii=False, indent=1)
    if rows:
        files["data/summary.md"] = md_summary(rows, meta, link, indices=market.indices)
        if market.indices:
            files["data/indices.md"] = md_indices(market.indices, time_meta(market.indices_fetched))
            files["data/indices.json"] = json.dumps(
                {"_زمان": time_meta(market.indices_fetched),
                 **{k: [{kk: vv for kk, vv in x.items() if kk != "_خام"} for x in v] for k, v in market.indices.items()}},
                ensure_ascii=False, indent=1, default=str)
        for r in rows:
            if r["نماد"] and "/" not in r["نماد"] and r.get("نوع") != "اوراق بدهی":
                # بدون زمان دریافت تا فقط نمادهای تغییرکرده آپلود شوند؛ زمان در data/meta.json است
                files[f"data/symbols/{r['نماد']}.json"] = json.dumps(
                    {k: v for k, v in r.items() if v not in (None, "")}, ensure_ascii=False,
                    separators=(",", ":"), default=str)
        if full:
            files["data/all.md"] = md_all(rows, meta, link)
            files["data/latest.csv"] = to_csv(rows)
            for sym in list(market.by_sym):
                if "/" not in sym:
                    files[f"data/history/{sym}.json"] = json.dumps(market.history.get(sym), ensure_ascii=False,
                                                                   default=str)

    def status(x):
        if not x["url"]:
            return "قطع"
        if x["verified_recently"]:
            return f"فعال (آخرین بررسی موفق: {x['last_check_ok_utc']})"
        if x["last_check_passed"] is False:
            return "متصل، ولی از داخل ایران قابل بررسی نیست؛ ممکن است از بیرون کار کند"
        return "متصل، هنوز بررسی نشده"

    live_lines = "\n".join(f"   - اولویت {i}: {x['url']}  — {status(x)}" for i, x in
                           enumerate([x for x in live if x["url"]], 1)) or "(در حال حاضر هیچ اتصال زنده‌ای برقرار نیست)"
    files["README.md"] = f"""# داده‌های لحظه‌ای بورس تهران (TSETMC)

- **زمان انتشار این صفحه (UTC): `{iso_utc(published)}`** — تهران: `{iso_teh(published)}`
- زمان دریافت داده‌های این مخزن از بورس (UTC): `{meta['fetched_at_utc']}` — تهران: `{meta['fetched_at_tehran']}`
- تعداد نمادهای معامله‌شده: {len(rows)}

## راهنما برای Claude
1. **اول اتصال زنده** (تأخیر چندثانیه‌ای). آدرس‌ها به ترتیب اولویت:

{live_lines}

   صفحهٔ اصلی هر آدرس راهنما و پیوند همهٔ بخش‌ها را دارد. اگر اولی جواب نداد یا خطا داد، بعدی را امتحان کن.
   📊 **برنامهٔ تحلیل بازه‌ای** (در مرورگر): `<آدرس زنده>/app` — API آن برای Claude: `<آدرس زنده>/api/range?day=...&from=10:00&to=10:10&symbols=خودرو`
   هر پاسخ زمان دریافت از بورس (`fetched_at_utc`) و زمان ارسال (`served_at_utc`) را دارد.
   اگر `served_at_utc` قدیمی بود، ابزار خواندن صفحه نسخهٔ کش‌شده را داده است؛ همان آدرس را با پسوند تازه
   (`?r=2`، `?r=3`، ...) دوباره بخوان.
2. **اگر هیچ اتصال زنده‌ای کار نکرد**، از فایل‌های همین مخزن بخوان (حدود هر ۳۰ ثانیه در ساعات بازار به‌روز می‌شوند):
   - خلاصهٔ بازار و برترین‌ها: {gh}/data/summary.md
   - همهٔ نمادها با پیوند به هر نماد (هر ۲ دقیقه): {gh}/data/all.md
   - یک نماد: {gh}/data/symbols/<نماد>.json — زمان دریافتش همان `fetched_at_utc` در {gh}/data/meta.json است
   - روند امروز یک نماد (هر ۲ دقیقه): {gh}/data/history/<نماد>.json
   - شاخص‌ها: {gh}/data/indices.md
   - هشدارهای امروز: {gh}/data/alerts.csv (وضعیت موتور هشدار: data/alerts_status.json)
   - اطلاعیه‌های کدال: {gh}/data/codal.json | خبرها: {gh}/data/news.json | دلار، سکه، طلا: {gh}/data/macro.json
3. **آرشیو روزهای قبل** (CSV فشرده با xz/gz، برای تحلیل با کد): {gh}/archive/index.md
   - همهٔ ستون‌ها (CSV، تا ۵ دقیقه کش می‌شود): https://raw.githubusercontent.com/{repo}/main/data/latest.csv
4. همیشه تأخیر را گزارش کن: (زمان فعلی UTC) − `fetched_at_utc`. اگر «زمان انتشار این صفحه» خیلی قدیمی است،
   ربات خاموش است یا بازار بسته است (شنبه تا چهارشنبه ۹:۰۰ تا ۱۲:۳۰ تهران).
5. در نام نمادها «ی» و «ک» فارسی‌اند و نیم‌فاصله حذف شده است.

{COLUMNS_HELP}"""
    return files


# ============================================================================
# موتور هشدار لحظه‌ای
# ============================================================================
MODEL_REPO = "alitamizi/tse-model"
RULES_URL = f"https://raw.githubusercontent.com/{MODEL_REPO}/main/rules/rules.json"
CONTEXT_URL = f"https://raw.githubusercontent.com/{MODEL_REPO}/main/rules/context.csv"
VERDICTS_URL = f"https://raw.githubusercontent.com/{MODEL_REPO}/main/verdicts/verdicts.json"
VERDICT_COLS = ["sent_at_utc", "verdict_id", "alert_id", "sym", "decision", "entry", "target", "stop", "reason",
                "decided_at_utc", "data_fetched_at_utc", "data_age_s", "channel_ok"]
ALERT_COLS = ["alert_id", "fetched_at_utc", "alerted_at_utc", "tehran_time", "delay_s", "kind", "rule_id", "rule_name",
              "نماد", "insCode", "side", "last", "lpct", "bp", "pace", "nrp", "note"]
OPS_ALERT = {"<": lambda a, b: a < b, "<=": lambda a, b: a <= b, ">": lambda a, b: a > b,
             ">=": lambda a, b: a >= b, "==": lambda a, b: a == b, "!=": lambda a, b: a != b}


def _ratio(a, b):
    a, b = num(a), num(b)
    return a / b - 1 if a is not None and b else None


def session_fraction(ts):
    t = datetime.fromtimestamp(ts, TEHRAN)
    mins = (t.hour * 60 + t.minute + t.second / 60) - 9 * 60
    return min(1.0, max(0.05, mins / 210))


class AlertEngine:
    def __init__(self, cfg, out_dir):
        self.cfg = cfg
        self.out_dir = out_dir
        self.rules, self.watch, self.rules_meta = [], [], {}
        self.context = {}
        self.rules_fetched = self.context_fetched = self.verdicts_fetched = 0.0
        self.verdicts_meta = {}
        self.last_ok = {}
        self.send_raw = bool(cfg.get("send_raw_alerts", False))
        self.errors = {}
        self.prev_queue = {}
        self.state_path = os.path.join(out_dir, "alerts_state.json")
        try:
            with open(self.state_path, encoding="utf-8") as f:
                self.state = json.load(f)
        except (OSError, ValueError):
            self.state = {"cooldown": {}, "watch_done": [], "verdicts_sent": []}
        self.today_alerts = []
        self.day = None
        self.queue = []
        self.sent_times = []
        self.qlock = threading.Lock()
        self.changed = False
        self.ch = self._channel()
        threading.Thread(target=self._loader, name="rules", daemon=True).start()
        threading.Thread(target=self._sender, name="alert-send", daemon=True).start()

    # ---- کانال ارسال
    def _channel(self):
        """بله یا تلگرام. اگر chat_id خالی باشد، از اولین پیامی که کاربر به ربات بدهد پیدا می‌شود."""
        c = self.cfg
        saved = os.path.join(self.out_dir, "alert_chat_id.txt")
        try:
            saved_id = open(saved, encoding="utf-8").read().strip()
        except OSError:
            saved_id = ""
        for name, tok_key, chat_key, base in (("بله", "bale_token", "bale_chat_id", "https://tapi.bale.ai/bot"),
                                               ("تلگرام", "telegram_token", "telegram_chat_id", "https://api.telegram.org/bot")):
            tok = c.get(tok_key)
            if not tok:
                continue
            proxy = c.get("telegram_proxy") if name == "تلگرام" else None
            proxies = {"https": proxy, "http": proxy} if proxy else None
            chat = str(c.get(chat_key) or saved_id or "")
            if not chat:
                threading.Thread(target=self._discover_chat, args=(name, base + tok, proxies, saved),
                                 name="chat-id", daemon=True).start()
                self.errors["send"] = (f"chat_id {name} هنوز معلوم نیست: یک پیام (مثلاً /start) به ربات {name} "
                                       "بفرستید تا خودکار پیدا شود")
            return (name, f"{base}{tok}/sendMessage", chat, proxies)
        return None

    def _discover_chat(self, name, api, proxies, saved):
        while True:
            try:
                r = requests.get(f"{api}/getUpdates", timeout=30, proxies=proxies)
                for u in reversed((r.json() or {}).get("result") or []):
                    msg = u.get("message") or u.get("channel_post") or {}
                    chat = (msg.get("chat") or {}).get("id")
                    if chat:
                        with open(saved, "w", encoding="utf-8") as f:
                            f.write(str(chat))
                        self.ch = (self.ch[0], self.ch[1], str(chat), self.ch[3])
                        self.errors.pop("send", None)
                        log.info("chat_id %s پیدا شد: %s", name, chat)
                        self._send(f"✅ ربات هشدار بورس وصل شد. هشدارها از این به بعد اینجا ارسال می‌شوند.")
                        return
            except (requests.RequestException, ValueError) as e:
                self.errors["send"] = f"{iso_utc(now_ts())} {name} getUpdates: {type(e).__name__}: {str(e)[:150]}"
            time.sleep(30)

    # ---- خواندن قاعده‌ها و context
    def _loader(self):
        last_morning = None
        while True:
            now = datetime.fromtimestamp(now_ts(), TEHRAN)
            morning = now.strftime("%Y-%m-%d") if now.hour * 60 + now.minute >= 8 * 60 + 45 else None
            if time.time() - self.rules_fetched > 300 or (morning and morning != last_morning):
                self._load_rules()
                self._load_context()
                last_morning = morning or last_morning
            if time.time() - self.verdicts_fetched >= 60:
                self._poll_verdicts()
            time.sleep(10)

    def _get(self, url):
        """فایل‌های مخزن tse-model (خصوصی) با توکن خوانده می‌شوند؛ اول از API (بدون کش)، بعد از raw."""
        token = self.cfg.get("model_token") or self.cfg.get("github_token")
        headers = {"User-Agent": "tse-bot", "Cache-Control": "no-cache"}
        m = re.match(r"https://raw\.githubusercontent\.com/([^/]+/[^/]+)/([^/]+)/(.+)$", url)
        if token and m:
            api = f"https://api.github.com/repos/{m.group(1)}/contents/{m.group(3)}"
            try:
                r = requests.get(api, timeout=30, params={"ref": m.group(2)},
                                 headers={**headers, "Authorization": f"Bearer {token}",
                                          "Accept": "application/vnd.github.raw+json",
                                          "X-GitHub-Api-Version": "2022-11-28"})
                if r.status_code == 200:
                    self.errors.pop("token", None)
                    return r
                if r.status_code in (401, 403, 404):
                    self.errors["token"] = (f"{iso_utc(now_ts())} دسترسی به {m.group(1)}/{m.group(3)}: HTTP {r.status_code} — "
                                            "توکن باید به مخزن tse-model دسترسی Contents: Read داشته باشد")
            except requests.RequestException:
                pass
        if token:
            headers["Authorization"] = f"Bearer {token}"
        r = requests.get(url, timeout=30, headers=headers, params={"t": int(time.time())})
        r.raise_for_status()
        self.errors.pop("token", None)
        return r

    def _load_rules(self):
        try:
            d = self._get(RULES_URL).json()
            self.rules = [x for x in d.get("rules", []) if x.get("id") and x.get("when")]
            self.watch = d.get("watch", []) or []
            self.rules_meta = {"version": d.get("version"), "updated_at_utc": d.get("updated_at_utc"),
                               "loaded_at_utc": iso_utc(now_ts()), "rules": len(self.rules), "watch": len(self.watch)}
            self.errors.pop("rules", None)
            self.last_ok["rules"] = iso_utc(now_ts())
        except Exception as e:  # noqa: BLE001
            self.errors["rules"] = f"{iso_utc(now_ts())} {type(e).__name__}: {str(e)[:200]} (آخرین نسخهٔ سالم نگه داشته شد)"
            log.warning("خواندن rules.json ناموفق: %s", e)
        self.rules_fetched = time.time()

    def _load_context(self):
        try:
            text = self._get(CONTEXT_URL).content.decode("utf-8-sig")
            ctx = {}
            for row in csv.DictReader(io.StringIO(text)):
                vals = {}
                for k, v in row.items():
                    if k is None:
                        continue
                    k = k.strip()
                    if k in ("نماد", "insCode"):
                        vals[k] = norm_fa(v) if k == "نماد" else str(v).strip()
                    else:
                        vals[k] = num(v) if num(v) is not None else v
                if vals.get("insCode"):
                    ctx[vals["insCode"]] = vals
                if vals.get("نماد"):
                    ctx[vals["نماد"]] = vals
            self.context = ctx
            self.errors.pop("context", None)
            self.last_ok["context"] = iso_utc(now_ts())
        except Exception as e:  # noqa: BLE001
            self.errors["context"] = f"{iso_utc(now_ts())} {type(e).__name__}: {str(e)[:200]} (آخرین نسخهٔ سالم نگه داشته شد)"
            log.warning("خواندن context.csv ناموفق: %s", e)
        self.context_fetched = time.time()

    # ---- حکم‌های Claude
    def _poll_verdicts(self):
        self.verdicts_fetched = time.time()
        try:
            d = self._get(VERDICTS_URL).json()
        except Exception as e:  # noqa: BLE001
            self.errors["verdicts"] = f"{iso_utc(now_ts())} {type(e).__name__}: {str(e)[:200]}"
            return
        self.errors.pop("verdicts", None)
        self.last_ok["verdicts"] = iso_utc(now_ts())
        sent = set(self.state.setdefault("verdicts_sent", []))
        items = d.get("verdicts") or []
        self.verdicts_meta = {"updated_at_utc": d.get("updated_at_utc"), "polled_at_utc": iso_utc(now_ts()),
                              "count": len(items)}
        first_run = not sent and not self.state.get("verdicts_seen_once")
        for v in items:
            vid = str(v.get("verdict_id") or "")
            if not vid or vid in sent:
                continue
            sent.add(vid)
            if v.get("decision") not in ("confirm_buy", "confirm_sell"):
                continue                      # reject یا نامعتبر: فرستاده نمی‌شود
            if first_run and self._older_than(v.get("decided_at_utc"), 3600):
                continue                      # اولین اجرا: حکم‌های قدیمی دوباره فرستاده نشوند
            self._send_verdict(v)
        self.state["verdicts_sent"] = sorted(sent)[-5000:]
        self.state["verdicts_seen_once"] = True
        self._save_state()

    @staticmethod
    def _older_than(iso, secs):
        try:
            return now_ts() - datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp() > secs
        except (ValueError, TypeError):
            return False

    def _send_verdict(self, v):
        dec = {"confirm_buy": "✅ تأیید خرید", "confirm_sell": "🔻 تأیید فروش"}[v["decision"]]
        age = None
        try:
            age = round(now_ts() - datetime.fromisoformat(str(v.get("data_fetched_at_utc")).replace("Z", "+00:00")).timestamp())
        except (ValueError, TypeError):
            pass
        try:
            dt = datetime.fromisoformat(str(v.get("decided_at_utc")).replace("Z", "+00:00")).astimezone(TEHRAN)
            decided = dt.strftime("%H:%M:%S")
        except (ValueError, TypeError):
            decided = v.get("decided_at_utc") or "-"
        text = (f"{dec} — {norm_fa(v.get('sym'))}\n"
                f"قیمت ورود: {_fmt(num(v.get('entry')))} | هدف: {_fmt(num(v.get('target')))} | "
                f"حد ضرر: {_fmt(num(v.get('stop')))}\n"
                f"دلیل: {v.get('reason') or '-'}\n"
                f"زمان حکم: {decided} تهران | عمر داده: {age if age is not None else '-'} ثانیه")
        ok = self._send(text) if self.ch and self.ch[2] else False
        if not (self.ch and self.ch[2]):
            self.errors["send"] = self.errors.get("send") or "کانال ارسال (بله/تلگرام) تنظیم نشده؛ حکم فقط ثبت شد"
        log.info("حکم %s برای %s %s", v.get("decision"), v.get("sym"), "ارسال شد" if ok else "ثبت شد (ارسال نشد)")
        row = {**v, "sent_at_utc": iso_utc(now_ts()), "data_age_s": age, "channel_ok": ok}
        try:
            day = datetime.fromtimestamp(now_ts(), TEHRAN).strftime("%Y-%m-%d")
            path = os.path.join(self.out_dir, "archive", f"{day}_verdicts.csv.gz")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            buf = io.StringIO()
            w = csv.writer(buf)
            if not os.path.exists(path):
                w.writerow(VERDICT_COLS)
            w.writerow([_compact(row.get(c)) for c in VERDICT_COLS])
            with gzip.open(path, "ab") as fh:
                fh.write(buf.getvalue().encode("utf-8"))
        except OSError:
            log.exception("ثبت حکم در آرشیو ناموفق بود")
        self.verdicts_today = getattr(self, "verdicts_today", []) + [row]

    # ---- فیلدهای محاسبه‌شده
    def features(self, r, ts):
        y = num(r.get("قیمت دیروز"))
        last, close = num(r.get("آخرین معامله")), num(r.get("قیمت پایانی"))
        lo, hi = num(r.get("کمترین")), num(r.get("بیشترین"))
        up, dn = num(r.get("سقف مجاز")), num(r.get("کف مجاز"))
        f = {
            "lpct": _ratio(last, y), "cpct": _ratio(close, y), "lo_pct": _ratio(lo, y), "hi_pct": _ratio(hi, y),
            "lim_up": _ratio(up, y), "lim_dn": -_ratio(dn, y) if _ratio(dn, y) is not None else None,
            "at_up": bool(last and up and last >= up), "at_dn": bool(last and dn and last <= dn),
            "touched_dn": bool(lo and dn and lo <= dn), "touched_up": bool(hi and up and hi >= up),
            "bp": num(r.get("قدرت خریدار حقیقی")),
        }
        f["locked_up"] = f["at_up"] and bool(lo and up and lo >= up)
        f["locked_dn"] = f["at_dn"] and bool(hi and dn and hi <= dn)
        val = num(r.get("ارزش معاملات"))
        f["nrp"] = num(r.get("ورود پول حقیقی")) / val if val and num(r.get("ورود پول حقیقی")) is not None else None
        ctx = self.context.get(r.get("insCode")) or self.context.get(r.get("نماد")) or {}
        v20 = num(ctx.get("v20"))
        f["pace"] = val / (v20 * session_fraction(ts)) if val is not None and v20 else None
        q = r.get("صف") or ""
        prev = self.prev_queue.get(r.get("insCode"))
        f["q_new_buy"] = q == "صف خرید" and prev is not None and prev != "صف خرید"
        f["q_new_sell"] = q == "صف فروش" and prev is not None and prev != "صف فروش"
        lim_up, lim_dn = f["lim_up"], f["lim_dn"]
        f["dn_to_up"] = bool(f["touched_dn"] and f["lpct"] is not None and lim_up and f["lpct"] >= lim_up / 2)
        f["up_to_dn"] = bool(f["touched_up"] and f["lpct"] is not None and lim_dn and f["lpct"] <= -lim_dn / 2)
        for k, v in ctx.items():
            f.setdefault(k, v)
        return f, ctx

    # ---- ارزیابی (با هر بار دریافت داده)
    def evaluate(self, rows, fetched):
        day = datetime.fromtimestamp(fetched, TEHRAN).strftime("%Y-%m-%d")
        if day != self.day:
            self.day, self.today_alerts = day, []
        if not market_open():
            self.prev_queue = {r.get("insCode"): r.get("صف") for r in rows}
            return
        cd = self.state.setdefault("cooldown", {})
        for r in rows:
            if not self.rules and not self.watch:
                break
            f, ctx = self.features(r, fetched)
            for rule in self.rules:
                uni = rule.get("universe", "all")
                if uni == "top100" and num(ctx.get("top100")) != 1:
                    continue
                if uni == "stocks" and r.get("نوع") != "سهم":
                    continue
                if uni == "funds" and r.get("نوع") != "صندوق":
                    continue
                syms = [norm_fa(s) for s in (rule.get("symbols") or [])]
                if syms and r["نماد"] not in syms and r.get("insCode") not in syms:
                    continue
                if not all(self._cond(f, c) for c in rule["when"]):
                    continue
                key = f"{rule['id']}|{r.get('insCode')}"
                if fetched - cd.get(key, 0) < 60 * float(rule.get("cooldown_min") or 0):
                    continue
                cd[key] = fetched
                self._alert("rule", rule.get("id"), rule.get("name"), r, f, rule.get("side"), rule.get("note"),
                            fetched)
        self._check_watch(rows, fetched)
        self.prev_queue = {r.get("insCode"): r.get("صف") for r in rows}
        if self.changed:
            self._save_state()

    @staticmethod
    def _cond(f, c):
        try:
            field, op, val = c
            v = f.get(field)
            if v is None or op not in OPS_ALERT:
                return False
            if isinstance(val, bool) or isinstance(v, bool):
                return OPS_ALERT[op](bool(v), bool(val)) if op in ("==", "!=") else False
            nv, nval = num(v), num(val)
            if nv is not None and nval is not None:
                return OPS_ALERT[op](nv, nval)
            return OPS_ALERT[op](str(v), str(val)) if op in ("==", "!=") else False
        except (ValueError, TypeError):
            return False

    def _check_watch(self, rows, fetched):
        by = {r["نماد"]: r for r in rows}
        done = set(self.state.setdefault("watch_done", []))
        today = datetime.fromtimestamp(fetched, TEHRAN).strftime("%Y-%m-%d")
        for w in self.watch:
            sym = norm_fa(w.get("sym"))
            r = by.get(sym)
            if not r or (w.get("until") and today > str(w["until"])[:10]):
                continue
            last = num(r.get("آخرین معامله"))
            if last is None:
                continue
            buy = (w.get("side") or "buy") == "buy"
            for kind, level in (("target", num(w.get("target"))), ("stop", num(w.get("stop")))):
                if level is None:
                    continue
                hit = (last >= level if (kind == "target") == buy else last <= level)
                key = f"{sym}|{w.get('entry')}|{kind}|{level}"
                if hit and key not in done:
                    done.add(key)
                    f, _ = self.features(r, fetched)
                    note = f"{'هدف' if kind == 'target' else 'حد ضرر'} {_fmt(level)} (ورود {_fmt(num(w.get('entry')))})" \
                           + (f" — {w.get('note')}" if w.get("note") else "")
                    self._alert("watch", f"watch-{kind}", f"دیده‌بانی: {'رسیدن به هدف' if kind == 'target' else 'رسیدن به حد ضرر'}",
                                r, f, w.get("side"), note, fetched)
        self.state["watch_done"] = sorted(done)

    def _alert(self, kind, rid, name, r, f, side, note, fetched):
        now = now_ts()
        aid = f"{datetime.fromtimestamp(fetched, TEHRAN):%Y%m%d-%H%M%S}-{rid}-{r.get('insCode')}"
        a = {"alert_id": aid, "fetched_at_utc": iso_utc(fetched), "alerted_at_utc": iso_utc(now),
             "tehran_time": datetime.fromtimestamp(fetched, TEHRAN).strftime("%H:%M:%S"),
             "delay_s": round(now - fetched, 1), "kind": kind, "rule_id": rid, "rule_name": name,
             "نماد": r["نماد"], "insCode": r.get("insCode"), "side": side, "last": num(r.get("آخرین معامله")),
             "lpct": round(f["lpct"] * 100, 2) if f.get("lpct") is not None else None,
             "bp": f.get("bp"), "pace": round(f["pace"], 2) if f.get("pace") is not None else None,
             "nrp": round(f["nrp"], 3) if f.get("nrp") is not None else None, "note": note}
        self.today_alerts.append(a)
        self.changed = True
        log.info("هشدار: %s — %s (%s)", name, r["نماد"], rid)
        try:
            os.makedirs(os.path.join(self.out_dir, "archive"), exist_ok=True)
            path = os.path.join(self.out_dir, "archive", f"{self.day}_alerts.csv.gz")
            buf = io.StringIO()
            w = csv.writer(buf)
            if not os.path.exists(path):
                w.writerow(ALERT_COLS)
            w.writerow([_compact(a.get(c)) for c in ALERT_COLS])
            with gzip.open(path, "ab") as fh:
                fh.write(buf.getvalue().encode("utf-8"))
        except OSError:
            log.exception("ثبت هشدار در آرشیو ناموفق بود")
        if not self.send_raw:
            return        # پیش‌فرض: هشدار خام فقط ثبت می‌شود؛ Claude قضاوت می‌کند و حکم تأییدشده فرستاده می‌شود
        side_fa = {"buy": "خرید", "sell": "فروش"}.get(side or "", side or "-")
        text = (f"⏳ تشخیص اولیه — منتظر تأیید\n🔔 {name}\nنماد: {r['نماد']} | سمت: {side_fa}\n"
                f"آخرین قیمت: {_fmt(a['last'])} ({_fmt(a['lpct'])}٪)\n"
                f"قدرت خریدار حقیقی: {_fmt(a['bp'])} | pace: {_fmt(a['pace'])}\n"
                f"ساعت داده: {a['tehran_time']} تهران | تأخیر: {a['delay_s']} ثانیه"
                + (f"\n{note}" if note else ""))
        with self.qlock:
            self.queue.append(text)

    def _sender(self):
        while True:
            time.sleep(3)
            with self.qlock:
                pending, self.queue = self.queue, []
            if not pending:
                continue
            if not self.ch or not self.ch[2]:
                continue   # کانال تنظیم نشده؛ هشدارها فقط ثبت می‌شوند
            self.sent_times = [t for t in self.sent_times if time.time() - t < 3600]
            room = max(0, 30 - len(self.sent_times))
            if len(pending) > room:
                keep = pending[:max(0, room - 1)]
                rest = pending[len(keep):]
                summary = f"📋 {len(rest)} هشدار دیگر (سقف ۳۰ پیام در ساعت):\n" + \
                          "\n".join(t.split("\n")[0] + " — " + t.split("\n")[1] for t in rest[:40])
                pending = keep + ([summary] if room else [])
                if not room:
                    with self.qlock:
                        self.queue = [summary] + self.queue
                    continue
            for text in pending:
                self._send(text)

    def _send(self, text):
        name, url, chat, proxies = self.ch
        for attempt in range(3):
            try:
                r = requests.post(url, json={"chat_id": chat, "text": text[:4000]}, timeout=20, proxies=proxies)
                if r.status_code == 200 and (r.json() or {}).get("ok"):
                    self.sent_times.append(time.time())
                    self.errors.pop("send", None)
                    self.last_ok["send"] = iso_utc(now_ts())
                    return True
                self.errors["send"] = f"{iso_utc(now_ts())} {name}: HTTP {r.status_code} {r.text[:200]}"
            except (requests.RequestException, ValueError) as e:
                self.errors["send"] = f"{iso_utc(now_ts())} {name}: {type(e).__name__}: {str(e)[:200]}"
            time.sleep(3 * (attempt + 1))
        log.warning("ارسال هشدار به %s ناموفق: %s", name, self.errors.get("send"))
        return False

    def _save_state(self):
        self.changed = False
        try:
            cut = now_ts() - 7 * 86400
            self.state["cooldown"] = {k: v for k, v in self.state.get("cooldown", {}).items() if v > cut}
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False)
        except OSError:
            pass

    def status(self):
        return {"channel": self.ch[0] if self.ch else None, "chat_id_known": bool(self.ch and self.ch[2]),
                "send_raw_alerts": self.send_raw, "rules": self.rules_meta, "verdicts": self.verdicts_meta,
                "verdicts_sent_today": len(getattr(self, "verdicts_today", [])),
                "context_symbols": len({v.get("insCode") for v in self.context.values()}),
                "alerts_today": len(self.today_alerts), "errors": dict(self.errors),
                "last_success_utc": dict(self.last_ok)}

    def alerts_csv(self):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(ALERT_COLS)
        for a in self.today_alerts:
            w.writerow([_compact(a.get(c)) for c in ALERT_COLS])
        return buf.getvalue()


# ============================================================================
# خبر، اطلاعیهٔ کدال و دادهٔ کلان
# ============================================================================
DEFAULT_NEWS_SOURCES = {
    "بورس‌نیوز": ["https://www.boursenews.ir/fa/rss/allnews", "https://www.boursenews.ir"],
    "سنا": ["https://www.sena.ir/rss", "https://www.sena.ir"],
    "دنیای اقتصاد": ["https://donya-e-eqtesad.com/fa/feeds/?p=Y2F0ZWdvcmllcz0x", "https://donya-e-eqtesad.com"],
    "اکوایران": ["https://ecoiran.com/fa/feeds/?p=Y2F0ZWdvcmllcz0x", "https://ecoiran.com"],
    "تسنیم": ["https://www.tasnimnews.com/fa/rss/feed/0/7/0/%D8%A7%D9%82%D8%AA%D8%B5%D8%A7%D8%AF%DB%8C",
              "https://www.tasnimnews.com/fa/rss/feed/0/8/0/%D8%A2%D8%AE%D8%B1%DB%8C%D9%86-%D8%A7%D8%AE%D8%A8%D8%A7%D8%B1",
              "https://tasnimnews.com/fa/rss/feed/0/7/0/%D8%A7%D9%82%D8%AA%D8%B5%D8%A7%D8%AF%DB%8C",
              "https://www.tasnimnews.ir/fa/rss/feed/0/7/0/%D8%A7%D9%82%D8%AA%D8%B5%D8%A7%D8%AF%DB%8C",
              "https://www.tasnimnews.com/fa/service/7", "https://www.tasnimnews.com"],
    "ایرنا": ["https://www.irna.ir/rss/tp/20", "https://www.irna.ir/service/economy"],
    "ایسنا": ["https://www.isna.ir/rss/tp/34", "https://www.isna.ir/service/Economy"],
    "فارس": ["https://www.farsnews.ir/rss/economy", "https://farsnews.ir/rss/economy", "https://www.farsnews.ir/rss",
             "https://farsnews.ir/rss", "https://www.farsnews.ir/economy", "https://www.farsnews.ir"],
    "اقتصادآنلاین": ["https://www.eghtesadonline.com/fa/rss/allnews", "https://www.eghtesadonline.com"],
    "تجارت‌نیوز": ["https://tejaratnews.com/feed", "https://tejaratnews.com"],
    "نبض بورس": ["https://www.nabzebourse.com/fa/rss/allnews", "https://www.nabzebourse.com"],
}
CODAL_URL = ("https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=-1&Childs=true"
             "&CompanyState=-1&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType=-1"
             "&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&TracingNo=-1"
             "&search=false")
TGJU_URL = "https://call1.tgju.org/ajax.json"
TGJU_KEYS = {"price_dollar_rl": "دلار آزاد", "price_eur": "یورو آزاد", "sekee": "سکه امامی",
             "nim": "نیم سکه", "rob": "ربع سکه", "geram18": "طلای ۱۸ عیار (گرم)", "mesghal": "مثقال طلا",
             "ons": "انس طلا (دلار)",
             "ice_currency_usd_buy": "دلار مرکز مبادله - بازار ارز (خرید)",
             "ice_currency_usd_sell": "دلار مرکز مبادله - بازار ارز (فروش)",
             "ice_transfer_usd_buy": "دلار مرکز مبادله - حواله (خرید)",
             "ice_transfer_usd_sell": "دلار مرکز مبادله - حواله (فروش)",
             "ice_average_usd_buy": "دلار مرکز مبادله - میانگین موزون (خرید)",
             "ice_average_usd_sell": "دلار مرکز مبادله - میانگین موزون (فروش)",
             "ice_usd": "دلار مرکز مبادله"}
# نام‌های جایگزین احتمالی دلار مرکز مبادله/نیمایی در tgju (هر کدام پیدا شد استفاده می‌شود)
NIMA_CANDIDATES = ["nima_sell_usd", "nima_buy_usd", "mex_usd_sell", "mex_usd_buy", "price_dollar_nima",
                   "dollar_nima", "usd_nima", "nima_usd", "exchange_center_usd", "mex_usd", "price_dollar_mex",
                   "sana_sell_usd", "sana_buy_usd", "price_dollar_sana"]
WEB_HEADERS = {"User-Agent": HEADERS["User-Agent"], "Accept-Language": "fa-IR,fa;q=0.9,en;q=0.5"}


COMMON_WORDS = {"فروردین", "اردیبهشت", "خرداد", "تیر", "مرداد", "شهریور", "مهر", "آبان", "آذر", "دی", "بهمن",
                "اسفند", "امروز", "دلار", "سکه", "طلا", "بانک", "نفت", "گاز", "برق", "آب", "مس", "روی", "ملت", "سپه",
                "رشد", "پارس", "کار", "تهران", "ایران", "خبر", "بورس", "کالا", "سهام", "سود", "اخبار", "شاخص", "نیرو"}


def _codal_type(title):
    t = norm_fa(title)
    for key, label in (("فعالیت ماهانه", "گزارش فعالیت ماهانه"), ("صورت‌های مالی", "صورت مالی"),
                       ("صورتهای مالی", "صورت مالی"), ("اطلاعات و صورت", "صورت مالی"), ("افزایش سرمایه", "افزایش سرمایه"),
                       ("مجمع", "مجمع"), ("توقف", "توقف نماد"), ("بازگشایی", "بازگشایی نماد"),
                       ("تفسیری", "گزارش تفسیری"), ("افشای اطلاعات", "افشای اطلاعات بااهمیت"),
                       ("پیش بینی", "پیش‌بینی"), ("سود", "سود")):
        if norm_fa(key) in t:
            return label
    return "سایر"


def _parse_time_any(s):
    """زمان انتشار خبر را تا جای ممکن به ISO تبدیل می‌کند؛ اگر نشد، متن خام برمی‌گردد."""
    if not s:
        return None
    try:
        return email.utils.parsedate_to_datetime(s).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        pass
    m = re.search(r"(1[34]\d\d)/(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?",
                  str(s).translate(str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")))
    if m:
        g = jalali_to_gregorian(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if g:
            dt = datetime(g.year, g.month, g.day, int(m.group(4)), int(m.group(5)), int(m.group(6) or 0), tzinfo=TEHRAN)
            return dt.isoformat(timespec="seconds")
    return str(s)


def _xml_items(text):
    """RSS یا Atom ساده → [(title, link, date)] بدون کتابخانهٔ اضافه."""
    import xml.etree.ElementTree as ET
    out = []
    try:
        root = ET.fromstring(text.encode("utf-8") if isinstance(text, str) else text)
    except ET.ParseError:
        return out
    for el in root.iter():
        tag = el.tag.split("}")[-1].lower()
        if tag not in ("item", "entry"):
            continue
        d = {}
        for ch in el:
            ct = ch.tag.split("}")[-1].lower()
            if ct == "title":
                d["title"] = (ch.text or "").strip()
            elif ct == "link":
                d["link"] = (ch.text or ch.get("href") or "").strip()
            elif ct in ("pubdate", "published", "updated", "date"):
                d.setdefault("date", (ch.text or "").strip())
        if d.get("title"):
            out.append((d["title"], d.get("link"), d.get("date")))
    return out


class Feeds:
    def __init__(self, cfg, market, out_dir):
        self.market, self.out_dir = market, out_dir
        self.sources = cfg.get("news_sources") or DEFAULT_NEWS_SOURCES
        self.s = requests.Session()
        self.s.headers.update(WEB_HEADERS)
        self.codal, self.news, self.macro = [], [], {}
        self.last_ok = {}
        self.t = {"codal": 0.0, "news": 0.0, "macro": 0.0}
        self.errors = {"codal": {}, "news": {}, "macro": {}}
        self.seen = {"codal": set(), "news": set()}
        self.discovered = {}
        self.changed = set()
        threading.Thread(target=self._loop, name="feeds", daemon=True).start()

    # ---- تطبیق نماد در عنوان
    def _symbols_in(self, title):
        t = " " + re.sub(r"[^\w\s]", " ", norm_fa(title)) + " "
        found = []
        for r in self.market.rows:
            sym = r.get("نماد")
            if sym and len(sym) >= 2 and sym not in COMMON_WORDS and r.get("نوع") in ("سهم", "صندوق") \
                    and f" {sym} " in t:
                found.append(sym)
        return found[:10]

    def _loop(self):
        time.sleep(15)
        while True:
            now = datetime.fromtimestamp(now_ts(), TEHRAN)
            codal_every = 120 if 8 <= now.hour < 18 else 900
            for kind, every, fn in (("codal", codal_every, self._fetch_codal), ("news", 300, self._fetch_news),
                                    ("macro", 300, self._fetch_macro)):
                if time.time() - self.t[kind] >= every:
                    self.t[kind] = time.time()
                    try:
                        fn()
                    except Exception as e:  # noqa: BLE001
                        self.errors[kind]["_"] = f"{iso_utc(now_ts())} {type(e).__name__}: {str(e)[:200]}"
                        log.exception("خطا در %s", kind)
            time.sleep(10)

    def _archive(self, kind, rows, cols):
        day = datetime.fromtimestamp(now_ts(), TEHRAN).strftime("%Y-%m-%d")
        path = os.path.join(self.out_dir, "archive", f"{day}_{kind}.csv.gz")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        buf = io.StringIO()
        w = csv.writer(buf)
        if not os.path.exists(path):
            w.writerow(cols)
        for r in rows:
            w.writerow([_compact(", ".join(r[c]) if isinstance(r.get(c), list) else r.get(c)) for c in cols])
        with gzip.open(path, "ab") as f:
            f.write(buf.getvalue().encode("utf-8"))

    def _fetch_codal(self):
        r = self.s.get(CODAL_URL, timeout=30, headers={"Accept": "application/json"})
        if r.status_code != 200:
            self.errors["codal"]["codal"] = f"{iso_utc(now_ts())} HTTP {r.status_code}"
            return
        letters = (r.json() or {}).get("Letters") or []
        fetched = iso_utc(now_ts())
        new = []
        for L in letters:
            lid = str(L.get("TracingNo") or L.get("LetterSerial") or L.get("Url"))
            url = L.get("Url") or ""
            if url and not url.startswith("http"):
                url = "https://www.codal.ir" + url
            item = {"id": f"codal-{lid}", "source": "کدال", "published_at": _parse_time_any(L.get("PublishDateTime") or L.get("SentDateTime")),
                    "fetched_at_utc": fetched, "نماد": norm_fa(L.get("Symbol")), "نام شرکت": norm_fa(L.get("CompanyName")),
                    "title": norm_fa(L.get("Title")), "نوع": _codal_type(L.get("Title")),
                    "شمارهٔ پیگیری": L.get("TracingNo"), "url": url,
                    "symbols": [norm_fa(L.get("Symbol"))] if L.get("Symbol") else []}
            if item["id"] not in self.seen["codal"]:
                self.seen["codal"].add(item["id"])
                new.append(item)
        if new:
            self.codal = (new + self.codal)[:300]
            self._archive("codal", new, ["id", "published_at", "fetched_at_utc", "نماد", "نام شرکت", "نوع", "title",
                                         "شمارهٔ پیگیری", "url"])
            self.changed.add("codal")
        self.errors["codal"].pop("codal", None)
        self.last_ok["codal"] = fetched

    def _feed_urls(self, name, urls):
        if name in self.discovered:
            return [self.discovered[name]]
        return urls

    def _fetch_news(self):
        fetched = iso_utc(now_ts())
        new = []
        for name, urls in self.sources.items():
            items, err = [], None
            for url in self._feed_urls(name, urls):
                try:
                    r = self.s.get(url, timeout=20)
                    if r.status_code != 200:
                        err = f"HTTP {r.status_code} {url}"
                        continue
                    text = r.text
                    items = _xml_items(text)
                    if not items and "<html" in text[:2000].lower():
                        # کشف خودکار آدرس RSS از صفحهٔ اصلی
                        m = re.search(r'<link[^>]+type="application/(?:rss|atom)\+xml"[^>]+href="([^"]+)"', text, re.I) or \
                            re.search(r'<link[^>]+href="([^"]+)"[^>]+type="application/(?:rss|atom)\+xml"', text, re.I)
                        if m:
                            feed = requests.compat.urljoin(url, m.group(1))
                            r2 = self.s.get(feed, timeout=20)
                            items = _xml_items(r2.text) if r2.status_code == 200 else []
                            if items:
                                self.discovered[name] = feed
                    if items:
                        if url != urls[0] and name not in self.discovered:
                            self.discovered[name] = url
                        break
                    err = err or f"خبری پیدا نشد در {url}"
                except requests.RequestException as e:
                    err = f"{type(e).__name__} {url}"
            if items:
                self.errors["news"].pop(name, None)
                self.last_ok[f"news:{name}"] = fetched
            else:
                self.errors["news"][name] = f"{fetched} {err}"
            for title, link, date in items[:50]:
                nid = "news-" + hashlib.sha1(f"{name}|{link or title}".encode("utf-8")).hexdigest()[:16]
                if nid in self.seen["news"]:
                    continue
                self.seen["news"].add(nid)
                new.append({"id": nid, "source": name, "published_at": _parse_time_any(date), "fetched_at_utc": fetched,
                            "title": norm_fa(title), "url": link, "symbols": self._symbols_in(title)})
        if new:
            self.news = (new + self.news)[:200]
            self._archive("news", new, ["id", "source", "published_at", "fetched_at_utc", "title", "url", "symbols"])
            self.changed.add("news")
        log.info("خبرها: %d خبر جدید؛ منابع ناموفق: %s", len(new), ", ".join(self.errors["news"]) or "ندارد")

    def _fetch_macro(self):
        fetched = iso_utc(now_ts())
        try:
            r = self.s.get(TGJU_URL, timeout=20)
            cur = (r.json() or {}).get("current") or {}
        except (requests.RequestException, ValueError) as e:
            self.errors["macro"]["tgju"] = f"{fetched} {type(e).__name__}: {str(e)[:150]}"
            return
        out = {}
        for key, label in TGJU_KEYS.items():
            v = cur.get(key)
            if not v:
                continue
            p = num(str(v.get("p", "")).replace(",", ""))
            out[key] = {"نام": label, "قیمت": p, "تغییر": v.get("d"), "درصد تغییر": v.get("dp"),
                        "کمترین": num(str(v.get("l", "")).replace(",", "")), "بیشترین": num(str(v.get("h", "")).replace(",", "")),
                        "زمان منبع": v.get("ts") or v.get("t"), "واحد": "دلار" if key == "ons" else "ریال"}
        if not any(k in out for k in ("nima_sell_usd", "nima_buy_usd")):
            for k in NIMA_CANDIDATES:
                v = cur.get(k)
                if v and num(str(v.get("p", "")).replace(",", "")):
                    out["nima_usd"] = {"نام": f"دلار مرکز مبادله/نیمایی (کلید tgju: {k})",
                                       "قیمت": num(str(v.get("p", "")).replace(",", "")), "تغییر": v.get("d"),
                                       "درصد تغییر": v.get("dp"), "زمان منبع": v.get("ts") or v.get("t"), "واحد": "ریال"}
                    break
        usd_keys = sorted(k for k in cur if any(x in k.lower() for x in ("usd", "dollar", "nima", "mex", "sana")))
        if out:
            self.macro = {"fetched_at_utc": fetched, "source": "tgju.org", "items": out,
                          "tgju_usd_keys_seen": usd_keys[:80]}
            self.errors["macro"].pop("tgju", None)
            self.last_ok["macro"] = fetched
            missing = [TGJU_KEYS[k] for k in TGJU_KEYS if k not in out]
            if missing:
                self.errors["macro"]["missing"] = "در پاسخ tgju نبود: " + "، ".join(missing)
            else:
                self.errors["macro"].pop("missing", None)
            self._archive("macro", [{"fetched_at_utc": fetched, "key": k, **v} for k, v in out.items()],
                          ["fetched_at_utc", "key", "نام", "قیمت", "تغییر", "درصد تغییر", "زمان منبع"])
            self.changed.add("macro")
        else:
            self.errors["macro"]["tgju"] = f"{fetched} پاسخ tgju قیمتی نداشت (ساختار عوض شده؟)"
        self.errors["macro"].pop("interbank", None)   # کنار گذاشته شد (Claude از نرخ اوراق خزانه حساب می‌کند)
        self.errors["macro"]["commodity"] = "شاخص‌های بورس کالا و انرژی: هنوز پیاده نشده است"

    def files(self):
        out = {}
        for kind, items in (("codal", self.codal), ("news", self.news)):
            if items or self.errors[kind]:
                out[f"data/{kind}.json"] = json.dumps({"fetched_at_utc": iso_utc(self.t[kind] + _clock["offset"]) if self.t[kind] else None,
                                                       "last_success_utc": {k: v for k, v in self.last_ok.items()
                                                                            if k == kind or k.startswith(kind + ":")},
                                                       "errors": self.errors[kind], "items": items},
                                                      ensure_ascii=False, indent=1, default=str)
        if self.macro or self.errors["macro"]:
            out["data/macro.json"] = json.dumps({**(self.macro or {"fetched_at_utc": None, "items": {}}),
                                                 "last_success_utc": self.last_ok.get("macro"),
                                                 "errors": self.errors["macro"]}, ensure_ascii=False, indent=1, default=str)
        return out

    def status(self):
        return {k: {"last_fetch_utc": iso_utc(self.t[k] + _clock["offset"]) if self.t[k] else None,
                    "last_success_utc": {kk: v for kk, v in self.last_ok.items() if kk == k or kk.startswith(k + ":")},
                    "errors": self.errors[k]} for k in self.t}


# ============================================================================
# اجرا
# ============================================================================
def market_open(now=None):
    now = now or datetime.fromtimestamp(now_ts(), TEHRAN)
    if now.weekday() not in (5, 6, 0, 1, 2):
        return False
    minutes = now.hour * 60 + now.minute
    return 8 * 60 + 45 <= minutes <= 13 * 60


# ============================================================================
# به‌روزرسانی خودکار — فقط وقتی کاربر در config.json گزینهٔ auto_update را true کرده باشد
# ============================================================================
UPDATE_CHECK_SECONDS = 3600
PROCESS_STARTED = time.time()


def instance_info():
    """برای تشخیص اینکه چند نمونه از ربات هم‌زمان اجرا می‌شود: شناسهٔ پردازش، زمان شروع و هش کوتاه مسیر پوشه (نه خود مسیر)."""
    return {"pid": os.getpid(), "started_at_utc": iso_utc(PROCESS_STARTED),
            "dir_id": hashlib.sha1(os.path.abspath(app_dir()).encode("utf-8")).hexdigest()[:8]}
DEFAULT_UPDATE_URL = "https://raw.githubusercontent.com/alitamizi/tse-data/main/update/manifest.json"


def _ver(v):
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except ValueError:
        return ()


def _update_enabled(cfg):
    """اگر در config.json مقدار auto_update آمده باشد همان؛ وگرنه پاسخی که کاربر هنگام اولین اجرا در update_choice.txt داده است."""
    v = cfg.get("auto_update")
    if v is not None:
        return bool(v)
    try:
        with open(os.path.join(app_dir(), "update_choice.txt"), encoding="utf-8") as f:
            return f.read().strip().lower().startswith("y")
    except OSError:
        return False


def check_update(cfg):
    """manifest را می‌خواند؛ اگر نسخهٔ جدیدتر بود و هش فایل درست بود، فایل را عوض و ربات را دوباره راه می‌اندازد."""
    if not _update_enabled(cfg):
        return
    try:
        r = requests.get(cfg.get("update_url") or DEFAULT_UPDATE_URL, timeout=20)
        r.raise_for_status()
        man = r.json()
        new_v = str(man.get("version", "")).strip()
        url, want = man.get("url"), str(man.get("sha256", "")).strip().lower()
        log.info("بررسی به‌روزرسانی: نسخهٔ فعلی %s، نسخهٔ منتشرشده %s", VERSION, new_v)
        if not _ver(new_v) or _ver(new_v) <= _ver(VERSION) or not url or not want:
            return
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.content
        if hashlib.sha256(data).hexdigest() != want:
            log.error("به‌روزرسانی به نسخهٔ %s رد شد: هش فایل با manifest یکی نیست", new_v)
            return
        compile(data, "tse_bot.py", "exec")  # فایل باید پایتون معتبر باشد
        path = os.path.join(app_dir(), "tse_bot.py")
        with open(path + ".new", "wb") as f:
            f.write(data)
        os.replace(path + ".new", path)
        log.warning("ربات به نسخهٔ %s به‌روز شد؛ %s. دوباره راه‌اندازی می‌شود.", new_v, man.get("notes", ""))
        logging.shutdown()
        os._exit(10)  # start.bat بلافاصله دوباره اجرا می‌کند
    except Exception:  # noqa: BLE001
        log.exception("بررسی به‌روزرسانی ناموفق بود")


def _update_loop(cfg):
    time.sleep(120)
    while True:
        check_update(cfg)
        time.sleep(UPDATE_CHECK_SECONDS)


def load_config():
    path = os.path.join(app_dir(), "config.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        log.exception("خواندن config.json ناموفق بود")
        return {}


def keep_awake():
    """جلوی رفتن ویندوز به حالت خواب را تا وقتی ربات باز است می‌گیرد."""
    if os.name == "nt":
        try:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)
            log.info("حالت خواب ویندوز تا وقتی ربات باز است غیرفعال شد")
        except Exception:  # noqa: BLE001
            log.debug("SetThreadExecutionState ناموفق")


def main():
    ap = argparse.ArgumentParser(description="ربات داده‌های لحظه‌ای بورس تهران")
    ap.add_argument("--once", action="store_true", help="فقط یک بار بخواند و منتشر کند (بدون تونل)")
    ap.add_argument("--interval", type=int, help="فاصلهٔ انتشار روی GitHub (ثانیه، پیش‌فرض ۳۰)")
    ap.add_argument("--ignore-hours", action="store_true", help="بدون توجه به ساعت بازار")
    ap.add_argument("--no-tunnel", action="store_true", help="بدون اتصال زنده")
    ap.add_argument("--out", default="output")
    ap.add_argument("--logs", default="logs")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    os.chdir(app_dir())
    setup_logging(args.logs, args.verbose)
    cfg = load_config()
    interval = max(20, args.interval or int(cfg.get("interval_seconds", 30)))
    repo, token = cfg.get("github_repo"), cfg.get("github_token")
    tunnel_names = [] if args.no_tunnel or args.once else cfg.get("tunnels", DEFAULT_TUNNELS)
    log.info("تنظیمات: interval=%ss github=%s tunnels=%s", interval, repo, tunnel_names)

    initial_clock_sync()
    global ARCHIVE_STEP
    ARCHIVE_STEP = int(cfg.get("archive_seconds", ARCHIVE_STEP))
    market = Market(args.out)
    pub = GitHubPublisher(repo, token) if repo and token else None
    if not pub:
        log.warning("GitHub تنظیم نشده (config.json)؛ داده فقط محلی و از طریق تونل در دسترس است")

    if args.once:
        rows, fetched = market.get(0)
        if pub and rows:
            pub.publish(build_github_files(market, rows, fetched, repo, [], True))
        return

    if not start_live_server(market, repo):
        log.error("یک نسخهٔ دیگر از ربات همین حالا در حال اجراست (پورت %d اشغال است). این پنجره بسته می‌شود.",
                  LIVE_PORT)
        sys.exit(3)
    keep_awake()
    if _update_enabled(cfg):
        threading.Thread(target=_update_loop, args=(cfg,), name="update", daemon=True).start()
        log.info("به‌روزرسانی خودکار فعال است (%s)", cfg.get("update_url") or DEFAULT_UPDATE_URL)
    else:
        log.info("به‌روزرسانی خودکار غیرفعال است (در config.json مقدار auto_update یا در update_choice.txt مقدار yes نیست)")

    tunnels = make_tunnels(tunnel_names)
    for t in tunnels:
        t.start()
    if tunnels:
        threading.Thread(target=health_loop, args=(tunnels,), name="health", daemon=True).start()

    market.alerts = AlertEngine(cfg, args.out)
    market.feeds = Feeds(cfg, market, args.out) if cfg.get("feeds", True) else None
    log.info("موتور هشدار: کانال=%s", market.alerts.ch[0] if market.alerts.ch else "تنظیم نشده (فقط ثبت در data/alerts.csv)")
    arch_sync = ArchiveSync(market, pub, repo, int(cfg.get("archive_days_on_github", ARCHIVE_DAYS_ON_GITHUB))) \
        if pub else None
    last_arch = 0.0
    last_pub, last_full, last_sig, last_stats = 0.0, 0.0, None, time.time()
    while True:
        if time.time() - last_stats > 600:
            n, rate = request_stats()
            log.info("آمار: %d درخواست به بورس در ۱۰ دقیقهٔ اخیر (%.1f در دقیقه)", n, rate)
            last_stats = time.time()
        try:
            is_open = args.ignore_hours or market_open()
            sig = tuple((t.tname, t.url, t.check_ok) for t in tunnels) + (len(market.alerts.today_alerts),)
            due = (time.time() - last_pub >= (interval if is_open else 300)) or sig != last_sig
            if due:
                rows, fetched = market.get(interval / 2 if is_open else 240)
                if pub:
                    full = (market.hist_changed or not last_full or pub.tree_sha is None
                            or time.time() - last_full > 600)
                    if pub.publish(build_github_files(market, rows, fetched, repo, tunnels, full)):
                        last_sig = sig
                        if full:
                            market.hist_changed, last_full = False, time.time()
                else:
                    last_sig = sig
                last_pub = time.time()
                if arch_sync and time.time() - last_arch > 120:
                    last_arch = time.time()
                    try:
                        arch_sync.tick()
                    except Exception:  # noqa: BLE001
                        log.exception("خطا در آپلود آرشیو")
                if not is_open:
                    log.info("بازار بسته است؛ اتصال‌ها باز می‌مانند و هر ۵ دقیقه وضعیت منتشر می‌شود")
        except Exception:  # noqa: BLE001
            log.exception("خطا در حلقهٔ اصلی؛ ادامه می‌دهم")
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("توقف توسط کاربر")
        sys.exit(0)
