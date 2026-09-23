import asyncio
import aiohttp
from aiohttp import web
import os
import time
import logging
from collections import defaultdict, deque
from html import escape

# ============================================================
# PAМП-ХАНТЕР v10.0 — Detecting sustained multi-hour breakouts
# ============================================================
#
# Логика полностью переработана под "разгонные" движения (см. примеры
# STRK/B2/FUSDT/MYX): цена растёт не одной резкой 1m-свечой, а плавно
# на протяжении многих часов, с параллельным ростом OI. Старый триггер
# по 1-минутным свечам такие движения почти не ловил — он реагировал
# только на финальный параболический хвост.
#
# Новая логика:
#   1. Считаем накопительное изменение цены за ROC_WINDOW_HOURS на 1H.
#   2. Требуем пробой локального диапазона (BREAKOUT_LOOKBACK_HOURS) —
#      чтобы не ловить рост внутри бокового шума.
#   3. Требуем повышенный объём на последней 1H свече (RVOL).
#   4. Подтверждаем движение на Bitget/Bybit (тот же % роста).
#   5. Требуем рост открытого интереса (OI) минимум на 2 из 3 бирж —
#      OI сэмплируется отдельным фоновым задачником каждые
#      OI_SAMPLE_INTERVAL_SEC, независимо от того, сработал ли триггер,
#      иначе к моменту триггера истории OI за 12 часов просто не будет.

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
PORT = int(os.environ.get("PORT", "10000"))

# --- Периодика ---
SCAN_INTERVAL_SEC = int(os.environ.get("SCAN_INTERVAL_SEC", "90"))           # проверка триггера — раз в 1.5 мин (нужно ловить импульс внутри часа)
OI_SAMPLE_INTERVAL_SEC = int(os.environ.get("OI_SAMPLE_INTERVAL_SEC", "900"))  # сэмплинг OI — раз в 15 мин
UNIVERSE_REFRESH_SEC = int(os.environ.get("UNIVERSE_REFRESH_SEC", "900"))

MAX_UNIVERSE_SYMBOLS = int(os.environ.get("MAX_UNIVERSE_SYMBOLS", "200"))
MAX_SCAN_CANDIDATES = int(os.environ.get("MAX_SCAN_CANDIDATES", "150"))

MIN_24H_VOLUME_USDT = float(os.environ.get("MIN_24H_VOLUME_USDT", "300000"))
MIN_PRICE_USDT = float(os.environ.get("MIN_PRICE_USDT", "0.0001"))
MAX_PRICE_USDT = float(os.environ.get("MAX_PRICE_USDT", "10.0"))

# === Детектор разгона (1H, "медленный" путь — плавный рост часами) ===
ROC_WINDOW_HOURS = int(os.environ.get("ROC_WINDOW_HOURS", "12"))          # окно накопительного роста
BREAKOUT_LOOKBACK_HOURS = int(os.environ.get("BREAKOUT_LOOKBACK_HOURS", "72"))  # база для пробоя (3 дня)
MIN_PUMP_PCT = float(os.environ.get("MIN_PUMP_PCT", "17.5"))               # мин. рост за окно, % (запрошено 15-20%)
MAX_PUMP_PCT = float(os.environ.get("MAX_PUMP_PCT", "250.0"))              # отсечка "уже поздно" / аномалии данных
MIN_RVOL_1H = float(os.environ.get("MIN_RVOL_1H", "1.8"))

# === "Быстрый" путь — весь памп укладывается в одну ещё НЕ закрытую 1H свечу ===
# Не ждём закрытия свечи: смотрим на неё в процессе формирования на каждом скане.
FAST_PATH_ENABLED = os.environ.get("FAST_PATH_ENABLED", "true").lower() == "true"
MIN_PUMP_PCT_FAST = float(os.environ.get("MIN_PUMP_PCT_FAST", "12.0"))     # % от open текущей свечи
MIN_RVOL_FAST = float(os.environ.get("MIN_RVOL_FAST", "3.0"))              # объём нормализуется на прошедшую долю часа
MIN_INTRACANDLE_VOLUME_USD = float(os.environ.get("MIN_INTRACANDLE_VOLUME_USD", "50000"))
MIN_ELAPSED_HOURS_FAST = float(os.environ.get("MIN_ELAPSED_HOURS_FAST", "0.05"))  # не триггерить в первые ~3 мин свечи (мало данных)

# === "Накопление" — самая ранняя стадия: OI растёт, а цена ещё стоит на месте ===
# Это то самое "тихое вливание" перед пампом (видно на графиках как рост OI/агрегированного
# объёма при плоских свечах). Сигнал заведомо менее надёжный, чем подтверждённый памп —
# это предупреждение "возможна подготовка", а не факт движения.
ACCUM_ENABLED = os.environ.get("ACCUM_ENABLED", "true").lower() == "true"
ACCUM_WINDOW_HOURS = int(os.environ.get("ACCUM_WINDOW_HOURS", "6"))         # окно проверки накопления
ACCUM_MIN_OI_GROWTH_PCT = float(os.environ.get("ACCUM_MIN_OI_GROWTH_PCT", "15.0"))  # рост OI, %
ACCUM_MAX_PRICE_MOVE_PCT = float(os.environ.get("ACCUM_MAX_PRICE_MOVE_PCT", "6.0"))  # цена должна быть "тихой"
ACCUM_MIN_SOURCES = int(os.environ.get("ACCUM_MIN_SOURCES", "2"))          # сколько бирж должны показать рост OI
ACCUM_COOLDOWN_SEC = int(os.environ.get("ACCUM_COOLDOWN_SEC", str(4 * 3600)))

# === Подтверждение на других биржах ===
CONFIRM_PCT_RATIO = float(os.environ.get("CONFIRM_PCT_RATIO", "0.5"))     # confirm-биржа должна показать >= ratio*MIN_PUMP_PCT
MIN_CONFIRMATIONS = int(os.environ.get("MIN_CONFIRMATIONS", "1"))        # сколько из {Bitget, Bybit} обязаны подтвердить

# === OI ===
OI_MIN_GROWTH_PCT = float(os.environ.get("OI_MIN_GROWTH_PCT", "8.0"))
OI_MIN_SOURCES = int(os.environ.get("OI_MIN_SOURCES", "2"))

# === Защита от повторов ===
COOLDOWN_SEC = int(os.environ.get("COOLDOWN_SEC", str(6 * 3600)))         # разгон длится часами -> кулдаун длиннее
MIN_CANDLES_NEEDED = ROC_WINDOW_HOURS + BREAKOUT_LOOKBACK_HOURS + 5

# Сколько 1H свечей запрашивать (с запасом)
CANDLE_FETCH_LIMIT = min(200, MIN_CANDLES_NEEDED + 10)

KUCOIN_BASE = "https://api-futures.kucoin.com"
BITGET_BASE = "https://api.bitget.com"
BYBIT_BASE = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("PUMP-HUNTER-v10")

SESSION = None
HTTP_SEMAPHORE = None

UNIVERSE = {}
LAST_SIGNAL = {}
ACCUM_LAST_SIGNAL = {}

# OI_HISTORY[base][exchange] = deque[(ts, oi_value)]
OI_HIST_MAXLEN = int((ROC_WINDOW_HOURS + 2) * 3600 / OI_SAMPLE_INTERVAL_SEC) + 5
OI_HISTORY = defaultdict(lambda: defaultdict(lambda: deque(maxlen=OI_HIST_MAXLEN)))

STATS = {
    "scans": 0,
    "oi_samples": 0,
    "pump_triggers": 0,
    "pump_triggers_fast": 0,
    "pump_triggers_slow": 0,
    "signals": 0,
    "signals_fast": 0,
    "signals_slow": 0,
    "accum_alerts": 0,
    "rejected_no_breakout": 0,
    "rejected_low_rvol": 0,
    "rejected_too_late": 0,
    "rejected_no_confirm": 0,
    "rejected_no_oi": 0,
}

# ============================================================
# HTTP
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
# FETCHERS — universe
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
# FETCHERS — 1H candles
# ============================================================

async def fetch_kucoin_candles_1h(symbol):
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - CANDLE_FETCH_LIMIT * 3600 * 1000
    data = await http_get(f"{KUCOIN_BASE}/api/v1/kline/query", {
        "symbol": symbol, "granularity": "60", "from": from_ms, "to": now_ms,
    })
    return _parse_kucoin_candles(data)


def _parse_kucoin_candles(data):
    if not data or not isinstance(data.get("data"), list):
        return []
    candles = []
    for row in data["data"]:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts * 1000 if ts < 10**12 else ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bitget_candles_1h(symbol):
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/candles", {
        "symbol": symbol, "productType": "USDT-FUTURES",
        "granularity": "1H", "limit": str(CANDLE_FETCH_LIMIT),
    })
    return _parse_bitget_candles(data)


def _parse_bitget_candles(data):
    if not data or data.get("code") != "00000":
        return []
    candles = []
    for row in data.get("data", []):
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


async def fetch_bybit_candles_1h(base):
    data = await http_get(f"{BYBIT_BASE}/v5/market/kline", {
        "category": "linear", "symbol": f"{base}USDT", "interval": "60", "limit": CANDLE_FETCH_LIMIT,
    })
    if not data or data.get("retCode") != 0:
        return []
    result = data.get("result")
    if not isinstance(result, dict):
        return []
    items = result.get("list")
    if not isinstance(items, list):
        return []
    candles = []
    for row in items:
        try:
            ts, o, h, l, c, v = int(row[0]), num(row[1]), num(row[2]), num(row[3]), num(row[4]), num(row[5])
            if o > 0 and c > 0:
                candles.append({"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v})
        except (TypeError, ValueError, IndexError):
            continue
    candles.sort(key=lambda x: x["ts"])
    return candles


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
    data = await http_get(f"{BITGET_BASE}/api/v2/mix/market/open-interest", {
        "symbol": symbol, "productType": "USDT-FUTURES",
    })
    if not data or data.get("code") != "00000":
        return 0.0
    raw = data.get("data")
    row = {}
    if isinstance(raw, dict):
        items = raw.get("list")
        row = items[0] if isinstance(items, list) and items else raw
    elif isinstance(raw, list) and raw:
        row = raw[0]
    if not isinstance(row, dict):
        return 0.0
    return num(row.get("amount") or row.get("openInterest") or row.get("openInterestUsd"))


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
# METRICS — разгон на 1H
# ============================================================

def calc_breakout_metrics(candles):
    """
    Возвращает метрики разгона по 1H свечам, либо None если данных
    недостаточно. Последняя свеча в ответе биржи считается ещё
    формирующейся и не используется как "текущая" точка отсчёта —
    берём последнюю ЗАКРЫТУЮ свечу (индекс -2).
    """
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

    # база для пробоя — диапазон ДО начала окна роста
    base_start = idx_now - ROC_WINDOW_HOURS - BREAKOUT_LOOKBACK_HOURS
    base_slice = candles[max(0, base_start):idx_past]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = close_now > base_high

    # объём последней закрытой 1H свечи против среднего за предыдущие 20
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
    """
    "Быстрый" путь: смотрим на ПОСЛЕДНЮЮ (ещё формирующуюся) 1H свечу
    прямо сейчас, не дожидаясь её закрытия. Ловит случаи, когда весь
    памп укладывается в одну свечу (например, +50% за 20 минут).
    """
    needed = BREAKOUT_LOOKBACK_HOURS + 3
    if len(candles) < needed:
        return None

    idx_now = len(candles) - 1  # текущая, ещё открытая свеча
    current = candles[idx_now]
    if current["open"] <= 0:
        return None

    elapsed_hours = max((time.time() * 1000 - current["ts"]) / 1000 / 3600, MIN_ELAPSED_HOURS_FAST)
    if elapsed_hours < MIN_ELAPSED_HOURS_FAST:
        return None

    pct = ((current["close"] / current["open"]) - 1) * 100

    # база — диапазон ДО текущей формирующейся свечи
    base_slice = candles[max(0, idx_now - BREAKOUT_LOOKBACK_HOURS):idx_now]
    if not base_slice:
        return None
    base_high = max(c["close"] for c in base_slice)
    breakout = current["close"] > base_high

    vol_usd_so_far = current["volume"] * current["close"]
    # нормализуем объём на прошедшую долю часа, чтобы сравнивать с полными часовыми свечами
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


def oi_growth_pct(base, exchange, window_hours=None):
    if window_hours is None:
        window_hours = ROC_WINDOW_HOURS
    hist = OI_HISTORY[base][exchange]
    if len(hist) < 2:
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
    now_ts, now_oi = hist[-1]
    past_ts, past_oi = past
    if past_oi <= 0:
        return None
    return ((now_oi / past_oi) - 1) * 100


def calc_accumulation(base, kc_candles):
    """
    Ищем "тихое вливание": OI растёт на нескольких биржах за ACCUM_WINDOW_HOURS,
    а цена за то же окно почти не двигается (диапазон закрытий < ACCUM_MAX_PRICE_MOVE_PCT).
    Это не подтверждённый памп, а ранний признак возможной подготовки к нему.
    """
    needed = ACCUM_WINDOW_HOURS + 2
    if len(kc_candles) < needed:
        return None

    idx_now = len(kc_candles) - 2  # последняя закрытая свеча
    window = kc_candles[max(0, idx_now - ACCUM_WINDOW_HOURS + 1):idx_now + 1]
    if len(window) < 2:
        return None

    closes = [c["close"] for c in window]
    lo, hi = min(closes), max(closes)
    if lo <= 0:
        return None
    price_range_pct = (hi - lo) / lo * 100
    if price_range_pct > ACCUM_MAX_PRICE_MOVE_PCT:
        return None  # цена уже двигается — это не тихая стадия, а обычный памп/шум

    oi_deltas = {}
    growing_sources = 0
    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch, ACCUM_WINDOW_HOURS)
        oi_deltas[exch] = d if d is not None else 0.0
        if d is not None and d >= ACCUM_MIN_OI_GROWTH_PCT:
            growing_sources += 1

    if growing_sources < ACCUM_MIN_SOURCES:
        return None

    return {
        "price_range_pct": price_range_pct,
        "oi_deltas": oi_deltas,
        "growing_sources": growing_sources,
        "close": closes[-1],
    }


# ============================================================
# TELEGRAM
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
    msg = (
        f"⚠️ <b>НАКОПЛЕНИЕ: {escape(base)}USDT</b>\n\n"
        f"Цена почти не двигается (диапазон {acc['price_range_pct']:.1f}% за {ACCUM_WINDOW_HOURS}ч),\n"
        f"но открытый интерес заметно растёт — похоже на тихий вход перед движением.\n\n"
        f"<b>📊 OI Δ ({ACCUM_WINDOW_HOURS}ч)</b>\n"
        f"  KuCoin: {acc['oi_deltas'].get('kucoin', 0):+.1f}%\n"
        f"  Bitget: {acc['oi_deltas'].get('bitget', 0):+.1f}%\n"
        f"  Bybit:  {acc['oi_deltas'].get('bybit', 0):+.1f}%\n\n"
        f"Цена сейчас: {acc['close']:.6g}\n\n"
        f"🔍 Это не подтверждённый памп, а ранний сигнал — стоит держать в поле зрения."
    )
    sent = await send_tg(msg)
    if sent:
        STATS["accum_alerts"] += 1
    return sent


async def send_pump_signal(base, kc, bg, bb, oi_deltas, change_24h, signal_type):
    is_fast = signal_type == "fast"
    conf_lines = []
    if bg:
        conf_lines.append(f"  Bitget: +{bg['pct']:.1f}% | RVOL {bg['rvol']:.1f}x")
    if bb:
        conf_lines.append(f"  Bybit:  +{bb['pct']:.1f}% | RVOL {bb['rvol']:.1f}x")

    if is_fast:
        header = f"⚡⚡⚡ <b>ИМПУЛЬС (внутри 1H свечи): {escape(base)}USDT</b>"
        window_note = f"за ~{kc['elapsed_hours'] * 60:.0f} мин (свеча ещё не закрыта)"
    else:
        header = f"📈📈📈 <b>РАЗГОН: {escape(base)}USDT</b>"
        window_note = f"за {ROC_WINDOW_HOURS}ч"

    msg = (
        f"{header}\n\n"
        f"<b>🎯 KuCoin (триггер, {window_note})</b>\n"
        f"  Рост: <b>+{kc['pct']:.1f}%</b>\n"
        f"  Пробой базы ({BREAKOUT_LOOKBACK_HOURS}ч): <b>{kc['base_high']:.6g} → {kc['close']:.6g}</b>\n"
        f"  RVOL: <b>{kc['rvol']:.2f}x</b>\n\n"
        f"<b>✅ Подтверждение</b>\n" + ("\n".join(conf_lines) if conf_lines else "  нет данных") + "\n\n"
        f"<b>📊 OI Δ ({ROC_WINDOW_HOURS}ч)</b>\n"
        f"  KuCoin: {oi_deltas.get('kucoin', 0):+.1f}%\n"
        f"  Bitget: {oi_deltas.get('bitget', 0):+.1f}%\n"
        f"  Bybit:  {oi_deltas.get('bybit', 0):+.1f}%\n\n"
        f"<b>24h change:</b> {change_24h:+.1f}%\n\n"
        + ("⚡ Резкий импульс в моменте — реагировать нужно быстро."
           if is_fast else
           "📈 Устойчивый многочасовой разгон, подтверждён объёмом и ростом OI.")
    )
    sent = await send_tg(msg)
    if sent:
        STATS["signals"] += 1
        STATS["signals_fast" if is_fast else "signals_slow"] += 1
    return sent


# ============================================================
# CORE — детекция триггера
# ============================================================

async def process_symbol(base):
    item = UNIVERSE.get(base)
    if not item:
        return

    if time.time() - LAST_SIGNAL.get(base, 0) < COOLDOWN_SEC:
        return

    kc_candles = await fetch_kucoin_candles_1h(item["kucoin_symbol"])

    # --- 0. Проверка накопления (тихое вливание, цена ещё не двигается) ---
    if ACCUM_ENABLED and time.time() - ACCUM_LAST_SIGNAL.get(base, 0) >= ACCUM_COOLDOWN_SEC:
        acc = calc_accumulation(base, kc_candles)
        if acc:
            ACCUM_LAST_SIGNAL[base] = time.time()
            await send_accumulation_alert(base, acc)
            log.info("⚠️ НАКОПЛЕНИЕ: %s | диапазон цены %.1f%%, источников OI=%d",
                      base, acc["price_range_pct"], acc["growing_sources"])

    kc = None
    signal_type = None

    # --- 1. Сначала проверяем "быстрый" путь: памп внутри текущей свечи ---
    if FAST_PATH_ENABLED:
        kc_fast = calc_intracandle_metrics(kc_candles)
        if (kc_fast and kc_fast["pct"] >= MIN_PUMP_PCT_FAST and kc_fast["breakout"]
                and kc_fast["rvol"] >= MIN_RVOL_FAST
                and kc_fast["volume_usd"] >= MIN_INTRACANDLE_VOLUME_USD
                and kc_fast["pct"] <= MAX_PUMP_PCT):
            kc = kc_fast
            signal_type = "fast"

    # --- 2. Если быстрый путь не сработал — проверяем "медленный" (многочасовой) ---
    if kc is None:
        kc_slow = calc_breakout_metrics(kc_candles)
        if (kc_slow and kc_slow["pct"] >= MIN_PUMP_PCT and kc_slow["pct"] <= MAX_PUMP_PCT
                and kc_slow["breakout"] and kc_slow["rvol"] >= MIN_RVOL_1H):
            kc = kc_slow
            signal_type = "slow"

    if kc is None:
        return

    STATS["pump_triggers"] += 1
    STATS["pump_triggers_fast" if signal_type == "fast" else "pump_triggers_slow"] += 1
    log.info("🚨 ТРИГГЕР(%s): %s | +%.1f%%, RVOL=%.2fx", signal_type, base, kc["pct"], kc["rvol"])

    # === Подтверждение на Bitget / Bybit (тем же методом, что сработал на KuCoin) ===
    bg_candles, bb_candles = await asyncio.gather(
        fetch_bitget_candles_1h(item["bitget_symbol"]),
        fetch_bybit_candles_1h(base),
    )
    metric_fn = calc_intracandle_metrics if signal_type == "fast" else calc_breakout_metrics
    bg = metric_fn(bg_candles)
    bb = metric_fn(bb_candles)

    confirm_pct_base = MIN_PUMP_PCT_FAST if signal_type == "fast" else MIN_PUMP_PCT
    confirm_threshold = confirm_pct_base * CONFIRM_PCT_RATIO
    confirmations = 0
    if bg and bg["pct"] >= confirm_threshold:
        confirmations += 1
    if bb and bb["pct"] >= confirm_threshold:
        confirmations += 1

    if confirmations < MIN_CONFIRMATIONS:
        STATS["rejected_no_confirm"] += 1
        log.info("REJECT %s: подтверждений %d/%d", base, confirmations, MIN_CONFIRMATIONS)
        return

    # === OI: используем накопленную фоновым сэмплером историю ===
    oi_deltas = {}
    positive_sources = 0
    for exch in ("kucoin", "bitget", "bybit"):
        d = oi_growth_pct(base, exch)
        oi_deltas[exch] = d if d is not None else 0.0
        if d is not None and d >= OI_MIN_GROWTH_PCT:
            positive_sources += 1

    if positive_sources < OI_MIN_SOURCES:
        STATS["rejected_no_oi"] += 1
        log.info("REJECT %s: OI-источников %d/%d, deltas=%s", base, positive_sources, OI_MIN_SOURCES, oi_deltas)
        return

    # === СИГНАЛ ===
    LAST_SIGNAL[base] = time.time()
    await send_pump_signal(base, kc, bg, bb, oi_deltas, item.get("change24", 0), signal_type)
    log.info("✅ СИГНАЛ(%s): %s | +%.1f%%", signal_type, base, kc["pct"])


# ============================================================
# ФОНОВЫЙ СЭМПЛЕР OI
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
                log.exception("OI sample error: %s", base)

    await asyncio.gather(*[worker(b) for b in bases])


# ============================================================
# UNIVERSE
# ============================================================

async def refresh_universe():
    global UNIVERSE
    kc = await fetch_kucoin_contracts()
    bg = await fetch_bitget_tickers()
    if not kc or not bg:
        return

    bg_set = set(bg.keys())
    uni = {}
    for base, info in kc.items():
        if info["volume24"] < MIN_24H_VOLUME_USDT:
            continue
        if not (MIN_PRICE_USDT <= info["price"] <= MAX_PRICE_USDT):
            continue
        if base not in bg_set:
            continue
        uni[base] = {
            "kucoin_symbol": info["symbol"],
            "bitget_symbol": f"{base}USDT",
            "volume24": info["volume24"],
            "change24": info.get("change24", 0),
        }

    sorted_u = sorted(uni.items(), key=lambda x: x[1]["volume24"], reverse=True)
    UNIVERSE = dict(sorted_u[:MAX_UNIVERSE_SYMBOLS])
    log.info("🌐 Юниверс: %d пар", len(UNIVERSE))


# ============================================================
# SCAN
# ============================================================

async def scan_cycle():
    if not UNIVERSE:
        return
    STATS["scans"] += 1
    cands = list(UNIVERSE.keys())[:MAX_SCAN_CANDIDATES]
    semaphore = asyncio.Semaphore(8)

    async def worker(base):
        async with semaphore:
            try:
                await process_symbol(base)
                await asyncio.sleep(0.05)
            except Exception as e:
                log.exception("Error %s: %s", base, e)

    await asyncio.gather(*[worker(b) for b in cands])


async def main_loop():
    last_universe = 0
    last_scan = 0
    last_oi_sample = 0
    while True:
        try:
            now = time.time()
            if now - last_universe > UNIVERSE_REFRESH_SEC:
                await refresh_universe()
                last_universe = now
            if UNIVERSE and now - last_oi_sample > OI_SAMPLE_INTERVAL_SEC:
                await oi_sample_cycle()
                last_oi_sample = now
            if UNIVERSE and now - last_scan > SCAN_INTERVAL_SEC:
                await scan_cycle()
                last_scan = now
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Scanner error: %s", e)
            await asyncio.sleep(10)


# ============================================================
# WEB
# ============================================================

async def index(req):
    return web.Response(
        text=(f"PUMP-HUNTER v10 (breakout+fast mode) | Uni: {len(UNIVERSE)} | "
              f"Triggers: {STATS['pump_triggers']} (fast={STATS['pump_triggers_fast']}, slow={STATS['pump_triggers_slow']}) | "
              f"Signals: {STATS['signals']} (fast={STATS['signals_fast']}, slow={STATS['signals_slow']}) | "
              f"Accum alerts: {STATS['accum_alerts']} | "
              f"Scans: {STATS['scans']} | OI samples: {STATS['oi_samples']} | "
              f"Rejected: no_breakout={STATS['rejected_no_breakout']} "
              f"low_rvol={STATS['rejected_low_rvol']} "
              f"no_confirm={STATS['rejected_no_confirm']} "
              f"no_oi={STATS['rejected_no_oi']} "
              f"too_late={STATS['rejected_too_late']}"),
        content_type="text/plain",
    )


async def start(app):
    global SESSION, HTTP_SEMAPHORE
    SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=50, ttl_dns_cache=300))
    HTTP_SEMAPHORE = asyncio.Semaphore(15)
    app["task"] = asyncio.create_task(main_loop())
    await send_tg(
        "📈 <b>ПАМП-ХАНТЕР v10.0 запущен (накопление + разгон + импульс)</b>\n\n"
        f"⚠️ Накопление: рост OI ≥{ACCUM_MIN_OI_GROWTH_PCT}% за {ACCUM_WINDOW_HOURS}ч при плоской цене (<{ACCUM_MAX_PRICE_MOVE_PCT}%)\n"
        f"⚡ Быстрый путь: ≥{MIN_PUMP_PCT_FAST:.1f}% внутри ещё не закрытой 1H свечи, RVOL≥{MIN_RVOL_FAST}x\n"
        f"📈 Медленный путь: ≥{MIN_PUMP_PCT:.1f}% за {ROC_WINDOW_HOURS}ч, RVOL(1H)≥{MIN_RVOL_1H}x\n"
        f"Пробой базы: {BREAKOUT_LOOKBACK_HOURS}ч\n"
        f"Подтверждение: Bitget/Bybit, мин. {MIN_CONFIRMATIONS} из 2\n"
        f"OI: рост ≥{OI_MIN_GROWTH_PCT}% минимум на {OI_MIN_SOURCES} из 3 бирж\n"
        f"Скан: раз в {SCAN_INTERVAL_SEC}с | Кулдаун: {COOLDOWN_SEC // 3600}ч (накопление: {ACCUM_COOLDOWN_SEC // 3600}ч)"
    )


async def stop(app):
    t = app.get("task")
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
