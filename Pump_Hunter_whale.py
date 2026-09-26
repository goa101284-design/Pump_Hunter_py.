import asyncio
import aiohttp
from aiohttp import web
import os
import time
import gc
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# ПАМП-ХАНТЕР v10.3 — Grind-15m Edition
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "90"))
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "900"))
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "150"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "1.0"))
MIN_LISTING_AGE_DAYS = float(os.environ.get("MIN_LISTING_AGE_DAYS", "14"))

# === GRIND — ранний подтверждённый разгон на 15м свечах ===
GRIND_ENABLED = os.environ.get("GRIND_ENABLED", "true").lower() == "true"
MIN_GRIND_PCT = float(os.environ.get("MIN_GRIND_PCT", "4.0"))
GRIND_WINDOW_CANDLES = int(os.environ.get("GRIND_WINDOW_CANDLES", "4"))
GRIND_MIN_GREEN = int(os.environ.get("GRIND_MIN_GREEN", "3"))
GRIND_LOOKBACK_HOURS = int(os.environ.get("GRIND_LOOKBACK_HOURS", "24"))
MIN_GRIND_RVOL = float(os.environ.get("MIN_GRIND_RVOL", "2.0"))
GRIND_CANDLE_FETCH_LIMIT = int(os.environ.get("GRIND_CANDLE_FETCH_LIMIT", "120"))

# === Детектор разгона (1H) ===
ROC_WINDOW_HOURS = int(os.environ.get("ROC_WINDOW_HOURS", "12"))
BREAKOUT_LOOKBACK_HOURS = int(os.environ.get("BREAKOUT_LOOKBACK_HOURS", "72"))
MIN_PUMP_PCT = float(os.environ.get("MIN_PUMP_PCT", "14.0"))
MAX_PUMP_PCT = float(os.environ.get("MAX_PUMP_PCT", "250.0"))
MIN_RVOL_1H = float(os.environ.get("MIN_RVOL_1H", "1.8"))
BREAKOUT_TOLERANCE = float(os.environ.get("BREAKOUT_TOLERANCE", "0.985"))

# === "Быстрый" путь ===
FAST_PATH_ENABLED = os.environ.get("FAST_PATH_ENABLED", "true").lower() == "true"
MIN_PUMP_PCT_FAST = float(os.environ.get("MIN_PUMP_PCT_FAST", "12.0"))
MIN_RVOL_FAST = float(os.environ.get("MIN_RVOL_FAST", "3.0"))
MIN_INTRACANDLE_VOLUME_USD = float(os.environ.get("MIN_INTRACANDLE_VOLUME_USD", "50000"))
MIN_ELAPSED_HOURS_FAST = float(os.environ.get("MIN_ELAPSED_HOURS_FAST", "0.05"))

# === "Накопление" ===
ACCUM_ENABLED = os.environ.get("ACCUM_ENABLED", "true").lower() == "true"
ACCUM_WINDOW_HOURS = int(os.environ.get("ACCUM_WINDOW_HOURS", "6"))
ACCUM_MIN_OI_GROWTH_PCT = float(os.environ.get("ACCUM_MIN_OI_GROWTH_PCT", "15.0"))
ACCUM_MAX_PRICE_MOVE_PCT = float(os.environ.get("ACCUM_MAX_PRICE_MOVE_PCT", "6.0"))
ACCUM_MIN_SOURCES = int(os.environ.get("ACCUM_MIN_SOURCES", "2"))
ACCUM_COOLDOWN_SEC = int(os.environ.get("ACCUM_COOLDOWN_SEC", str(4 * 3600)))
ACCUM_MIN_RATIO = float(os.environ.get("ACCUM_MIN_RATIO", "2.5"))

# === Подтверждение и OI ===
CONFIRM_PCT_RATIO = float(os.environ.get("CONFIRM_PCT_RATIO", "0.5"))
MIN_CONFIRMATIONS = int(os.environ.get("MIN_CONFIRMATIONS", "1"))
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "8.0"))
OI_MIN_SOURCES = int(os.environ.get("OI_MIN_SOURCES", "1"))

COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(6 * 3600)))
REJECT_COOLDOWN_SEC = int(os.environ.get("REJECT_COOLDOWN_SEC", str(30 * 60)))
MIN_CANDLES_NEEDED = ROC_WINDOW_HOURS + BREAKOUT_LOOKBACK_HOURS + 5
CANDLE_FETCH_LIMIT = min(100, MIN_CANDLES_NEEDED + 5)

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v10.3")

SESSION = None
HTTP_SEMAPHORE = None
START_TIME = time.time()

UNIVERSE = {}
LAST_SIGNAL = {}
LAST_REJECT = {}
ACCUM_LAST_SIGNAL = {}

OI_HIST_MAXLEN = int((ROC_WINDOW_HOURS + 2) * 3600 / OI_SAMPLE_INTERVAL_SEC) + 5
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0,
    "oi_samples": 0,
    "pump_triggers": 0,
    "pump_triggers_fast": 0,
    "pump_triggers_slow": 0,
    "pump_triggers_grind": 0,
    "signals": 0,
    "signals_fast": 0,
    "signals_slow": 0,
    "signals_grind": 0,
    "accum_alerts": 0,
    "rejected_no_breakout": 0,
    "rejected_low_rvol": 0,
    "rejected_too_late": 0,
    "rejected_no_confirm": 0,
    "rejected_no_oi": 0,
}

# ============================================================
# HTTP UTILS
# ============================================================

async def http_get(url, params=None, timeout=8, retries=2):
    if SESSION is None or SESSION.closed or HTTP_SEMAPHORE is None:
        return None

    for attempt in range(retries + 1):
        try:
            async with HTTP_SEMAPHORE:
                async with SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                    if r.status == 429:
                        if attempt < retries:
                            await asyncio.sleep(1.0 * (attempt + 1))
                            continue
                        return None
                    if r.status >= 400:
                        return None
                    return await r.json(content_type=None)
        except (asyncio.TimeoutError, aiohttp.ClientError):
            if attempt < retries:
                await asyncio.sleep(0.4)
        except Exception:
            break
    return None


def num(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def norm(s):
    if not s:
        return ""
    s = str(s).upper()
    if s.startswith("XBT"):
        s = "BTC" + s[3:]
    for suf in ("USDTM", "USDT", "-USDT", "_USDT", "PERP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    return s


# ============================================================
# FETCHERS — Universe
# ============================================================

async def fetch_kucoin_contracts():
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/active")
    result = {}
    if not data or not isinstance(data.get("data"), list):
        return result
    for row in data["data"]:
        if not isinstance(row, dict):
            continue
        if str(row.get("status", "")).lower() != "open":
            continue
        if str(row.get("settleCurrency", "")).upper() != "USDT":
            continue
        symbol = str(row.get("symbol", "")).upper()
        base = norm(row.get("baseCurrency") or symbol)
        if not base:
            continue
        result[base] = {
            "symbol": symbol,
            "price": num(row.get("lastTradePrice") or row.get("markPrice")),
            "volume24": num(row.get("turnoverOf24h")),
            "change24": num(row.get("priceChgPct")) * 100,
            "first_open_ms": num(row.get("firstOpenDate")),
        }
    return result


async def fetch_bitget_tickers():
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    result = {}
    if not data or data.get("code") != "00000":
        return result
    for row in data.get("data", []):
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        if symbol.endswith("USDT"):
            base = norm(symbol)
            if base:
                result[base] = {"symbol": symbol}
    return result


# ============================================================
# FETCHERS — candles
# ============================================================

def _parse_list_candles(rows, ts_ms=True):
    candles = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                if ts_ms and ts < 10**12:
                    ts *= 1000
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_kucoin_candles_1h(symbol):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - CANDLE_FETCH_LIMIT * 3600 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "60", "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])


async def fetch_kucoin_candles_15m(symbol):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - GRIND_CANDLE_FETCH_LIMIT * 900 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "15", "from": from_ms, "to": now_ms,
    })
    if not data or not isinstance(data.get("data"), list):
        return []
    return _parse_list_candles(data["data"])


async def fetch_bitget_candles_1h(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "granularity": "1H", "limit": str(CANDLE_FETCH_LIMIT),
    })
    if not data or data.get("code") != "00000":
        return []
    return _parse_list_candles(data.get("data", []), ts_ms=False)


async def fetch_bitget_candles_15m(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "granularity": "15m", "limit": str(GRIND_CANDLE_FETCH_LIMIT),
    })
    if not data or data.get("code") != "00000":
        return []
    return _parse_list_candles(data.get("data", []), ts_ms=False)


async def fetch_bybit_candles_1h(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": "60", "limit": CANDLE_FETCH_LIMIT,
    })
    if not data or data.get("retCode") != 0:
        return []
    result = data.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("list"), list):
        return []
    return _parse_list_candles(result["list"], ts_ms=False)


async def fetch_bybit_candles_15m(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": "15", "limit": GRIND_CANDLE_FETCH_LIMIT,
    })
    if not data or data.get("retCode") != 0:
        return []
    result = data.get("result")
    if not isinstance(result, dict) or not isinstance(result.get("list"), list):
        return []
    return _parse_list_candles(result["list"], ts_ms=False)


# ============================================================
# FETCHERS — OI
# ============================================================

async def fetch_kucoin_oi(symbol):
    if not symbol:
        return 0.0
    data = await http_get(f"{KUCOIN_BASE}/api/v1/contracts/{symbol}")
    if data and isinstance(data.get("data"), dict):
        return num(data["data"].get("openInterest"))
    return 0.0


async def fetch_bitget_oi(symbol):
    if not symbol:
        return 0.0
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "USDT-FUTURES",
    })
    if not data or data.get("code") != "00000":
        return 0.0
        
    raw = data.get("data")
    if not isinstance(raw, (dict, list)):
        return 0.0

    row = {}
    if isinstance(raw, dict):
        items = raw.get("openList") or raw.get("openInterestList") or raw.get("list")
        if isinstance(items, list) and len(items) > 0:
            row = items[0]
        else:
            row = raw
    elif isinstance(raw, list) and len(raw) > 0:
        row = raw[0]

    return num(row.get("openInterest") or row.get("amount") or row.get("size") or row.get("openInterestUsd"))


async def fetch_bybit_oi(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/open-interest", {
        "category": "linear", "symbol": f"{base}USDT", "intervalTime": "1h", "limit": 1,
    })
    if not data or data.get("retCode") != 0:
        return 0.0
    result = data.get("result")
    if not isinstance(result, dict):
        return 0.0
    items = result.get("list")
    if not isinstance(items, list) or not items:
        return 0.0
    return num(items[0].get("openInterest"))


# ============================================================
# METRICS
# ============================================================

def calc_breakout_metrics(candles):
    needed = ROC_WINDOW_HOURS + BREAKOUT_LOOKBACK_HOURS + 2
    if len(candles) < needed:
        return None

    idx_now = len(candles) - 2
    idx_past = idx_now - ROC_WINDOW_HOURS
    if idx_past < 0:
        return None

    close_now = candles[idx_now]["close"]
    close_past = candles[idx_past]["close"]
    if close_past <= 0:
        return None

    pct = ((close_now / close_past) - 1) * 100

    base_start = idx_now - ROC_WINDOW_HOURS - BREAKOUT_LOOKBACK_HOURS
    base_slice = candles[max(0, base_start):idx_past]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high * BREAKOUT_TOLERANCE

    vol_now_usd = candles[idx_now]["volume"] * candles[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = vol_now_usd / avg_vol if avg_vol > 0 else 0

    return {
        "close": close_now,
        "pct": pct,
        "breakout": breakout,
        "base_high": base_high,
        "rvol": rvol,
        "volume_usd": vol_now_usd,
    }


def calc_intracandle_metrics(candles):
    needed = BREAKOUT_LOOKBACK_HOURS + 3
    if len(candles) < needed:
        return None

    idx_now = len(candles) - 1
    current = candles[idx_now]
    if current["open"] <= 0:
        return None

    elapsed_real = (time.time() * 1000 - current["ts"]) / 1000 / 3600
    if elapsed_real < MIN_ELAPSED_HOURS_FAST:
        return None
    elapsed_hours = max(elapsed_real, 0.01)

    pct = ((current["close"] / current["open"]) - 1) * 100

    base_slice = candles[max(0, idx_now - BREAKOUT_LOOKBACK_HOURS):idx_now]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = current["close"] > base_high * BREAKOUT_TOLERANCE

    vol_usd_so_far = current["volume"] * current["close"]
    normalized_vol = vol_usd_so_far / min(elapsed_hours, 1.0)
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = normalized_vol / avg_vol if avg_vol > 0 else 0

    return {
        "close": current["close"],
        "pct": pct,
        "breakout": breakout,
        "base_high": base_high,
        "rvol": rvol,
        "volume_usd": vol_usd_so_far,
        "elapsed_hours": elapsed_hours,
    }


def calc_grind_metrics(candles):
    lookback_candles = GRIND_LOOKBACK_HOURS * 4
    needed = lookback_candles + GRIND_WINDOW_CANDLES + 2
    if len(candles) < needed:
        return None

    idx_now = len(candles) - 2
    window = candles[idx_now - GRIND_WINDOW_CANDLES + 1: idx_now + 1]
    if len(window) < GRIND_WINDOW_CANDLES:
        return None

    ref_close = candles[idx_now - GRIND_WINDOW_CANDLES]["close"]
    close_now = candles[idx_now]["close"]
    if ref_close <= 0:
        return None

    pct = ((close_now / ref_close) - 1) * 100
    green = sum(1 for c in window if c["close"] >= c["open"])

    base_slice = candles[max(0, idx_now - lookback_candles): idx_now - GRIND_WINDOW_CANDLES + 1]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high * BREAKOUT_TOLERANCE

    vol_now_usd = candles[idx_now]["volume"] * candles[idx_now]["close"]
    prev_vols = [c["volume"] * c["close"] for c in candles[max(0, idx_now - 20):idx_now] if c["volume"] > 0]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    rvol = vol_now_usd / avg_vol if avg_vol > 0 else 0

    return {
        "close": close_now,
        "pct": pct,
        "breakout": breakout,
        "base_high": base_high,
        "rvol": rvol,
        "volume_usd": vol_now_usd,
        "green": green,
        "elapsed_hours": GRIND_WINDOW_CANDLES * 0.25,
    }


def oi_growth_pct(base, exchange, window_hours=None):
    if window_hours is None:
        window_hours = ROC_WINDOW_HOURS
    hist = OI_HISTORY[base][exchange]
    if len(hist) < 2:
        return None

    now_ts, now_oi = hist[-1]
    if now_ts - hist[0][0] < window_hours * 3600 * 0.8:
        return None

    target_ts = time.time() - window_hours * 3600
    past = None
    for ts, val in hist:
        if ts <= target_ts:
            past = (ts, val)
        else:
            break
    if past is None:
        past = hist[0]
    past_ts, past_oi = past
    if past_oi <= 0:
        return None
    return ((now_oi / past_oi) - 1) * 100


def calc_accumulation(base, kc_candles):
    needed = ACCUM_WINDOW_HOURS + 2
    if len(kc_candles) < needed:
        return None

    idx_start = max(0, len(kc_candles) - 1 - ACCUM_WINDOW_HOURS)
    start_close = kc_candles[idx_start]["close"]
    now_close = kc_candles[-1]["close"]
    if start_close <= 0:
        return None

    price_change_pct = abs((now_close - start_close) / start_close) * 100
    if price_change_pct > ACCUM_MAX_PRICE_MOVE_PCT:
        return None

    oi_deltas = {}
    growing_sources = 0
    max_oi_delta = 0.0

    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch, ACCUM_WINDOW_HOURS)
        oi_deltas[exch] = d if d is not None else 0.0
        if d is not None and d >= ACCUM_MIN_OI_GROWTH_PCT:
            growing_sources += 1
            if d > max_oi_delta:
                max_oi_delta = d

    if growing_sources < ACCUM_MIN_SOURCES:
        return None

    safe_price_change = max(price_change_pct, 0.5)
    oi_to_price_ratio = max_oi_delta / safe_price_change

    if oi_to_price_ratio < ACCUM_MIN_RATIO:
        return None

    return {
        "price_change_pct": price_change_pct,
        "oi_to_price_ratio": oi_to_price_ratio,
        "oi_deltas": oi_deltas,
        "growing_sources": growing_sources,
        "close": now_close,
    }


# ============================================================
# TELEGRAM MESSAGING
# ============================================================

async def send_tg(text):
    if not BOT_TOKEN or not CHAT_ID or SESSION is None:
        return False
    try:
        async with SESSION.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHAT_ID, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": True,
            },
            timeout=aiohttp.ClientTimeout(total=8),
        ) as r:
            if r.status != 200:
                return False
            payload = await r.json(content_type=None)
            return payload.get("ok", False)
    except Exception:
        return False


async def send_accumulation_alert(base, acc):
    bybit_ticker = f"<code>{escape(base)}USDT</code>"
    msg = (
        f"⚠️ <b>НАКОПЛЕНИЕ: {bybit_ticker}</b>\n\n"
        f"Цена изменилась всего на <b>{acc['price_change_pct']:.1f}%</b> за {ACCUM_WINDOW_HOURS}ч,\n"
        f"но открытый интерес вырос с коэффициентом <b>x{acc['oi_to_price_ratio']:.1f}</b> к цене.\n"
        f"Идёт закуп крупного игрока без импульса на графике.\n\n"
        f"<b>📊 OI Δ ({ACCUM_WINDOW_HOURS}ч)</b>\n"
        f"  KuCoin: {acc['oi_deltas'].get('kucoin', 0):+.1f}%\n"
        f"  Bitget: {acc['oi_deltas'].get('bitget', 0):+.1f}%\n"
        f"  Bybit:  {acc['oi_deltas'].get('bybit', 0):+.1f}%\n\n"
        f"Текущая цена: <b>{acc['close']:.6g}</b>\n\n"
        f"🔍 Ранняя стадия перед выходом из флэта."
    )
    sent = await send_tg(msg)
    if sent:
        STATS["accum_alerts"] += 1
    return sent


async def send_status_message():
    uptime_hours = (time.time() - START_TIME) / 3600
    active_cooldowns = sum(1 for ts in LAST_SIGNAL.values() if time.time() - ts < COOLDOWN_SEC)
    active_accum = sum(1 for ts in ACCUM_LAST_SIGNAL.values() if time.time() - ts < ACCUM_COOLDOWN_SEC)
    msg = (
        f"📊 <b>Статус PUMP-HUNTER v10.3</b>\n\n"
        f"⏱ Аптайм: {uptime_hours:.1f} ч\n"
        f"🌐 Юниверс: {len(UNIVERSE)} пар (Диапазон: ${MIN_PRICE_USDT}–${MAX_PRICE_USDT})\n"
        f"🔍 Сканов выполнено: {STATS['scans']}\n"
        f"🚨 Триггеров: {STATS['pump_triggers']} "
        f"(grind={STATS['pump_triggers_grind']}, fast={STATS['pump_triggers_fast']}, slow={STATS['pump_triggers_slow']})\n"
        f"✅ Сигналов отправлено: {STATS['signals']} "
        f"(grind={STATS['signals_grind']}, fast={STATS['signals_fast']}, slow={STATS['signals_slow']})\n"
        f"⚠️ Алертов накопления: {STATS['accum_alerts']}\n"
        f"🧊 Кулдауны пампа: {active_cooldowns} | Накопления: {active_accum}\n\n"
        f"<b>Статистика отказов:</b>\n"
        f"  Без пробоя: {STATS['rejected_no_breakout']}\n"
        f"  Низкий RVOL: {STATS['rejected_low_rvol']}\n"
        f"  Нет подтверждения: {STATS['rejected_no_confirm']}\n"
        f"  Нет роста OI: {STATS['rejected_no_oi']}\n"
        f"  Слишком поздно: {STATS['rejected_too_late']}"
    )
    await send_tg(msg)


async def handle_telegram_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    chat_id = str(msg.get("chat", {}).get("id", ""))
    if CHAT_ID and chat_id != str(CHAT_ID):
        return
    text = (msg.get("text") or "").strip().lower()
    if text.startswith("/start"):
        await send_tg("👋 <b>PUMP-HUNTER v10.3 запущен.</b> Используй /status для получения отчёта.")
    elif text.startswith("/status"):
        await send_status_message()


async def telegram_get_updates(offset):
    if SESSION is None or SESSION.closed or not BOT_TOKEN:
        return None
    try:
        params = {"timeout": 25}
        if offset is not None:
            params["offset"] = offset
        async with SESSION.get(
            f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
            params=params, timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                return None
            return await r.json(content_type=None)
    except Exception:
        return None


async def telegram_poll_loop():
    if not BOT_TOKEN:
        return
    offset = None
    while True:
        try:
            data = await telegram_get_updates(offset)
            if data and data.get("ok"):
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        await handle_telegram_update(update)
                    except Exception:
                        log.exception("Ошибка обработки команды TG")
            else:
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Telegram poll error")
            await asyncio.sleep(5)


async def send_pump_signal(base, kc, bg, bb, oi_deltas, change_24h, signal_type):
    conf_lines = []
    if bg:
        conf_lines.append(f"  Bitget: +{bg['pct']:.1f}% | RVOL {bg['rvol']:.1f}x")
    if bb:
        conf_lines.append(f"  Bybit:  +{bb['pct']:.1f}% | RVOL {bb['rvol']:.1f}x")

    bybit_ticker = f"<code>{escape(base)}USDT</code>"

    if signal_type == "grind":
        header = f"⚡ <b>РАЗГОН 15м: {bybit_ticker}</b>"
        window_note = f"за ~{GRIND_WINDOW_CANDLES * 15} мин ({kc.get('green', '?')}/{GRIND_WINDOW_CANDLES} свечей зелёные)"
    elif signal_type == "fast":
        header = f"⚡⚡⚡ <b>ИМПУЛЬС: {bybit_ticker}</b>"
        window_note = f"за ~{kc['elapsed_hours'] * 60:.0f} мин (свеча ещё открыта)"
    else:
        header = f"📈📈📈 <b>РАЗГОН: {bybit_ticker}</b>"
        window_note = f"за {ROC_WINDOW_HOURS}ч"

    def _oi_line(name, delta):
        if delta <= -OI_MIN_GROWTH_PCT:
            return f"  {name}: {delta:+.1f}% (шорт-сквиз)"
        return f"  {name}: {delta:+.1f}%"

    msg = (
        f"{header}\n\n"
        f"<b>🎯 KuCoin (триггер, {window_note})</b>\n"
        f"  Рост: <b>+{kc['pct']:.1f}%</b>\n"
        f"  Пробой базы: <b>{kc['base_high']:.6g} → {kc['close']:.6g}</b>\n"
        f"  RVOL: <b>{kc['rvol']:.2f}x</b>\n\n"
        f"<b>✅ Подтверждение</b>\n" + ("\n".join(conf_lines) if conf_lines else "  нет данных") + "\n\n"
        f"<b>📊 OI Δ ({ROC_WINDOW_HOURS}ч)</b>\n"
        f"{_oi_line('KuCoin', oi_deltas.get('kucoin', 0))}\n"
        f"{_oi_line('Bitget', oi_deltas.get('bitget', 0))}\n"
        f"{_oi_line('Bybit', oi_deltas.get('bybit', 0))}\n\n"
        f"<b>24h change:</b> {change_24h:+.1f}%\n"
    )
    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1
        if signal_type == "grind":
            STATS["signals_grind"] += 1
        else:
            STATS["signals_fast" if signal_type == "fast" else "signals_slow"] += 1
    return sent


# ============================================================
# PROCESS SYMBOL ENGINE
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    if time.time() - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
        return

    if time.time() - LAST_REJECT.get(base, 0) < REJECT_COOLDOWN_SEC:
        return

    kc = None
    signal_type = None

    # 1. GRIND (15м)
    if GRIND_ENABLED:
        kc15 = await fetch_kucoin_candles_15m(item["kucoin_symbol"])
        g = calc_grind_metrics(kc15)
        if g:
            if not g["breakout"]:
                STATS["rejected_no_breakout"] += 1
            elif g["rvol"] < MIN_GRIND_RVOL:
                STATS["rejected_low_rvol"] += 1
            elif g["green"] < GRIND_MIN_GREEN:
                pass
            elif g["pct"] >= MIN_GRIND_PCT:
                kc = g
                signal_type = "grind"

    # 2. 1H пути
    if kc is None:
        kc_candles = await fetch_kucoin_candles_1h(item["kucoin_symbol"])

        if ACCUM_ENABLED and time.time() - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
            acc = calc_accumulation(base, kc_candles)
            if acc:
                ACCUM_LAST_SIGNAL[base] = time.time()
                await send_accumulation_alert(base, acc)
                log.info("⚠️ НАКОПЛЕНИЕ: %s | ΔЦена %.1f%%, Ratio=%.2f",
                         base, acc["price_change_pct"], acc["oi_to_price_ratio"])

        if FAST_PATH_ENABLED:
            kc_fast = calc_intracandle_metrics(kc_candles)
            if kc_fast:
                if not kc_fast["breakout"]:
                    STATS["rejected_no_breakout"] += 1
                elif kc_fast["pct"] > MAX_PUMP_PCT:
                    STATS["rejected_too_late"] += 1
                elif kc_fast["rvol"] < MIN_RVOL_FAST:
                    STATS["rejected_low_rvol"] += 1
                elif kc_fast["volume_usd"] >= MIN_INTRACANDLE_VOLUME_USD and kc_fast["pct"] >= MIN_PUMP_PCT_FAST:
                    kc = kc_fast
                    signal_type = "fast"

        if kc is None:
            kc_slow = calc_breakout_metrics(kc_candles)
            if kc_slow:
                if not kc_slow["breakout"]:
                    STATS["rejected_no_breakout"] += 1
                elif kc_slow["pct"] > MAX_PUMP_PCT:
                    STATS["rejected_too_late"] += 1
                elif kc_slow["rvol"] < MIN_RVOL_1H:
                    STATS["rejected_low_rvol"] += 1
                elif kc_slow["pct"] >= MIN_PUMP_PCT:
                    kc = kc_slow
                    signal_type = "slow"

    if kc is None:
        return

    STATS["pump_triggers"] += 1
    if signal_type == "grind":
        STATS["pump_triggers_grind"] += 1
    else:
        STATS["pump_triggers_fast" if signal_type == "fast" else "pump_triggers_slow"] += 1

    # Подтверждение
    if signal_type == "grind":
        bg_candles, bb_candles = await asyncio.gather(
            fetch_bitget_candles_15m(item["bitget_symbol"]),
            fetch_bybit_candles_15m(base),
        )
        metric_fn = calc_grind_metrics
        confirm_pct_base = MIN_GRIND_PCT
    elif signal_type == "fast":
        bg_candles, bb_candles = await asyncio.gather(
            fetch_bitget_candles_1h(item["bitget_symbol"]),
            fetch_bybit_candles_1h(base),
        )
        metric_fn = calc_intracandle_metrics
        confirm_pct_base = MIN_PUMP_PCT_FAST
    else:
        bg_candles, bb_candles = await asyncio.gather(
            fetch_bitget_candles_1h(item["bitget_symbol"]),
            fetch_bybit_candles_1h(base),
        )
        metric_fn = calc_breakout_metrics
        confirm_pct_base = MIN_PUMP_PCT

    bg = metric_fn(bg_candles)
    bb = metric_fn(bb_candles)

    confirm_threshold = confirm_pct_base * CONFIRM_PCT_RATIO
    confirmations = 0
    if bg and bg["pct"] >= confirm_threshold:
        confirmations += 1
    if bb and bb["pct"] >= confirm_threshold:
        confirmations += 1

    if confirmations < MIN_CONFIRMATIONS:
        STATS["rejected_no_confirm"] += 1
        LAST_REJECT[base] = time.time()
        return

    # OI
    oi_deltas = {}
    positive_sources = 0
    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch)
        oi_deltas[exch] = d if d is not None else 0.0
        if d is not None and abs(d) >= OI_MIN_GROWTH_PCT:
            positive_sources += 1

    if positive_sources < OI_MIN_SOURCES:
        STATS["rejected_no_oi"] += 1
        LAST_REJECT[base] = time.time()
        return

    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, kc, bg, bb, oi_deltas, item.get("change24", 0), signal_type)


# ============================================================
# BACKGROUND WORKERS
# ============================================================

async def sample_oi_for_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return
    now = time.time()
    kc_oi, bg_oi, bb_oi = await asyncio.gather(
        fetch_kucoin_oi(item["kucoin_symbol"]),
        fetch_bitget_oi(item["bitget_symbol"]),
        fetch_bybit_oi(base),
    )
    if kc_oi > 0:
        OI_HISTORY[base]["kucoin"].append((now, kc_oi))
    if bg_oi > 0:
        OI_HISTORY[base]["bitget"].append((now, bg_oi))
    if bb_oi > 0:
        OI_HISTORY[base]["bybit"].append((now, bb_oi))
    STATS["oi_samples"] += 1


async def oi_sample_cycle():
    if not UNIVERSE:
        return
    bases = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(10)

    async def worker(base):
        async with semaphore:
            try:
                await sample_oi_for_symbol(base)
            except Exception:
                pass

    await asyncio.gather(*[worker(b) for b in bases])
    gc.collect()


async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    now_ms = time.time() * 1000
    min_age_ms = MIN_LISTING_AGE_DAYS * 86400 * 1000
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT or not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        first_open = info.get("first_open_ms", 0)
        if first_open > 0 and (now_ms - first_open) < min_age_ms:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс обновлен: %d пар", len(UNIVERSE))
    gc.collect()


async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(10)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(b) for b in cands])
    gc.collect()


async def universe_loop():
    while True:
        try:
            await asyncio.sleep(UNIVERSE_REFRESH_SEC)
            await refresh_universe()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(10)


async def oi_loop():
    while True:
        try:
            if UNIVERSE:
                await oi_sample_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(OI_SAMPLE_INTERVAL_SEC)


async def scan_loop():
    while True:
        try:
            if UNIVERSE:
                await scan_cycle()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(SCAN_INTERVAL_SEC)


# ============================================================
# WEB SERVER
# ============================================================

async def index(req):
    return web.Response(
        text=(f"PUMP-HUNTER v10.3 | Uni: {len(UNIVERSE)} | "
              f"Triggers: {STATS['pump_triggers']} | Signals: {STATS['signals']} | "
              f"Scans: {STATS['scans']}"),
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE, START_TIME
    START_TIME = time.time()
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(10)

    if BOT_TOKEN:
        try:
            async with SESSION.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook",
                params={"drop_pending_updates": "true"},
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                log.info("deleteWebhook: HTTP %s", r.status)
        except Exception as e:
            log.warning("deleteWebhook error: %s", e)

    await refresh_universe()

    app["universe_task"] = asyncio.create_task(universe_loop())
    app["oi_task"] = asyncio.create_task(oi_loop())
    app["scan_task"] = asyncio.create_task(scan_loop())
    app["poll_task"] = asyncio.create_task(telegram_poll_loop())

    await send_tg("🚀 <b>PUMP-HUNTER v10.3 запущен и готов к работе.</b>")


async def stop(app):
    for key in ("universe_task", "oi_task", "scan_task", "poll_task"):
        t = app.get(key)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if SESSION and not SESSION.closed:
        await SESSION.close()


app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", index)
app.on_startup.append(start)
app.on_cleanup.append(stop)


if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
