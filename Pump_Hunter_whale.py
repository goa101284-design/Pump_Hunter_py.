import asyncio
import logging
import os
import time
import datetime
import numpy as np
import pandas as pd
from aiohttp import web
import aiohttp

# =====================================================================
# ⚙️ НАСТРОЙКИ И КОНФИГУРАЦИЯ
# =====================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")

BINGX_BASE_URL = "https://open-api.bingx.com"
EXCLUDE_COINS = ["USDT", "USDC", "BUSD", "EUR", "GBP", "DAI", "TUSD"]
MIN_24H_VOLUME_USDT = 50000
SCAN_INTERVAL_SECONDS = 30

# Пороги для Real-Time сигнала
MIN_1H_CHANGE_PCT = 0.5
MIN_1H_RVOL = 1.8

SIGNAL_CACHE = {}
COOLDOWN_SECONDS = 1800

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# =====================================================================
# 📡 BINGX API & TELEGRAM
# =====================================================================
async def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    logging.error(f"Ошибка Telegram: {await resp.text()}")
    except Exception as e:
        logging.error(f"Исключение Telegram: {e}")


async def get_active_usdt_pairs(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            res = await resp.json()
            if res.get("code") == 0:
                data = res.get("data", [])
                valid = []
                for item in data:
                    symbol = item.get("symbol", "")
                    vol = float(item.get("quoteVolume", 0))
                    if not symbol.endswith("-USDT"): continue
                    if symbol.split("-")[0] in EXCLUDE_COINS: continue
                    if vol >= MIN_24H_VOLUME_USDT:
                        valid.append(symbol)
                return valid
    except Exception as e:
        logging.error(f"Ошибка получения пар: {e}")
    return []


async def get_klines(session, symbol: str, interval: str, limit: int = 80):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    params = {"symbol": symbol.replace("-", ""), "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            res = await resp.json()
            if res.get("code") == 0:
                return res.get("data", [])
    except Exception as e:
        logging.error(f"Ошибка Klines {symbol}: {e}")
    return []


# =====================================================================
# 🧠 АНАЛИТИКА
# =====================================================================
def calculate_ema(closes, period):
    if len(closes) < period: return closes[-1]
    return pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1]


def find_base_params(klines_1h, min_hours, max_hours, max_width):
    """Ищет базу накопления, исключая ложные флэты из одной свечи."""
    closed = klines_1h[:-1]

    for w in range(max_hours, min_hours - 1, -2):
        if len(closed) < w: continue

        recent = closed[-w:]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        vols = [float(k[5]) * float(k[4]) for k in recent]

        # Защита от одной аномально тихой свечи
        ranges = [(float(k[2]) - float(k[3])) / float(k[3]) * 100 for k in recent]
        avg_range = np.mean(ranges)
        max_range = max(ranges)

        if max_range > max_width * 2 and avg_range > max_width * 0.8:
            continue

        range_high = max(highs)
        range_low = min(lows)
        if range_low <= 0: continue

        flat_width = ((range_high - range_low) / range_low) * 100
        avg_vol = np.mean(vols) if vols else 0.0

        if flat_width <= max_width and avg_vol > 0:
            return True, round(flat_width, 2), range_high, range_low, w, avg_vol

    return False, 0.0, 0.0, 0.0, 0, 0.0


def find_stop_clusters(klines_1h, range_low, range_high):
    """
    🔧 НОВОЕ: Сканер скопления стоп-лоссов.
    Ищет зоны, где маркет-мейкер мог собрать ликвидность.
    """
    closed = klines_1h[:-1]
    if len(closed) < 12:
        return []

    lows = [float(k[3]) for k in closed]
    highs = [float(k[2]) for k in closed]
    closes = [float(k[4]) for k in closed]
    volumes = [float(k[5]) * float(k[4]) for k in closed]

    clusters = []

    # --- 1. Поиск равных минимумов (Double/Triple Bottom) ---
    for i in range(len(lows) - 3):
        for j in range(i + 2, min(i + 8, len(lows))):
            diff_pct = abs((lows[i] - lows[j]) / lows[i]) * 100
            if diff_pct <= 0.3 and j - i >= 2:
                level = (lows[i] + lows[j]) / 2
                # Был ли свип (прокол с возвратом)?
                swept = any(low < level * 0.998 and close > level for low, close in zip(lows[j:], closes[j:]))
                # Был ли рост объёма при свипе?
                vol_at_sweep = volumes[j] if j < len(volumes) else 0
                avg_vol = np.mean(volumes[max(0, j - 5):j]) if j >= 5 else vol_at_sweep

                clusters.append({
                    "level": level,
                    "type": "DOUBLE_BOTTOM",
                    "touches": 2,
                    "swept": swept,
                    "vol_spike": vol_at_sweep > avg_vol * 1.5 if avg_vol > 0 else False,
                    "zone_low": level * 0.997,
                    "zone_high": level * 1.003
                })
                break

    # --- 2. Стопы под нижней границей базы ---
    if range_low:
        swept_base = any(low < range_low and close > range_low for low, close in zip(lows[-8:], closes[-8:]))
        if swept_base:
            clusters.append({
                "level": range_low,
                "type": "BASE_SWEEP",
                "touches": 1,
                "swept": True,
                "vol_spike": False,
                "zone_low": range_low * 0.996,
                "zone_high": range_low * 1.004
            })

    return clusters


def check_ema_bounce(klines_1h, range_high, range_low):
    """Проверяет отскок от EMA внутри базы."""
    closes = [float(k[4]) for k in klines_1h]
    ema20 = calculate_ema(closes, 20)
    ema40 = calculate_ema(closes, 40)

    ema20_in = range_low <= ema20 <= range_high
    ema40_in = range_low <= ema40 <= range_high

    if not (ema20_in or ema40_in):
        return None, 0, ""

    dist = abs((ema20 - ema40) / ema40) * 100 if ema40 > 0 else 100
    squeezed = dist <= 2.0

    curr = klines_1h[-1]
    open_h, high_h, low_h, close_h = float(curr[1]), float(curr[2]), float(curr[3]), float(curr[4])

    prev = klines_1h[-4:-1]
    touched20 = any(abs(float(c[4]) - ema20) / ema20 * 100 <= 0.8 for c in prev)
    touched40 = any(abs(float(c[4]) - ema40) / ema40 * 100 <= 0.8 for c in prev)

    green = close_h > open_h
    above20 = close_h > ema20
    above40 = close_h > ema40

    if touched20 and touched40 and green and above20 and above40:
        score = 30
        detail = "🔥 Отскок от EMA20+EMA40 (+30)"
        if squeezed:
            score += 15
            detail += f" | Сжатие {dist:.1f}% (+15)"
        return "DUAL_EMA", score, detail

    if touched40 and green and above40:
        return "EMA40", 25, "🦾 Отскок от EMA40 (+25)"

    if touched20 and green and above20:
        return "EMA20", 20, "💪 Отскок от EMA20 (+20)"

    return None, 0, ""


def evaluate_realtime_signal(klines_1h):
    """Real-Time анализ с детекцией скопления стопов."""
    if len(klines_1h) < 24:
        return None

    # --- Поиск базы ---
    is_flat, width, range_h, range_l, hours, avg_vol = find_base_params(
        klines_1h, min_hours=4, max_hours=12, max_width=3.5
    )
    if not is_flat:
        is_flat, width, range_h, range_l, hours, avg_vol = find_base_params(
            klines_1h, min_hours=24, max_hours=72, max_width=4.5
        )
    if not is_flat:
        return None

    # --- Текущая свеча ---
    curr = klines_1h[-1]
    open_h, high_h, low_h, close_h = float(curr[1]), float(curr[2]), float(curr[3]), float(curr[4])
    vol_h = float(curr[5]) * close_h

    now = datetime.datetime.now()
    elapsed = now.minute + now.second / 60
    if elapsed < 1: elapsed = 1

    projected_vol = vol_h * (60 / elapsed)
    rvol = projected_vol / avg_vol if avg_vol > 0 else 1.0
    change_pct = ((close_h - open_h) / open_h) * 100

    if change_pct < MIN_1H_CHANGE_PCT or rvol < MIN_1H_RVOL:
        return None

    distance_to_high = ((range_h - close_h) / close_h) * 100
    if distance_to_high > 0.5 and close_h < range_h:
        return None

    # --- EMA-отскок ---
    ema_type, ema_score, ema_detail = check_ema_bounce(klines_1h, range_h, range_l)

    # 🔧 НОВОЕ: Поиск скопления стопов
    stop_clusters = find_stop_clusters(klines_1h, range_l, range_h)
    stop_score = 0
    stop_details = []

    for cluster in stop_clusters:
        if cluster.get("swept"):
            if cluster["type"] == "BASE_SWEEP":
                stop_score += 20
                stop_details.append(f"🧹 Свип нижней границы базы — сбор стопов (+20)")
            elif cluster["type"] == "DOUBLE_BOTTOM":
                stop_score += 25
                stop_details.append(f"🧹 Свип двойного дна на ${cluster['level']:.6f} — сбор стопов (+25)")
                if cluster.get("vol_spike"):
                    stop_score += 10
                    stop_details.append("📊 Всплеск объёма при сборе стопов (+10)")

    # --- Сборка ---
    score = 50
    reasons = [f"⏰ Real-Time ({elapsed:.0f} мин от начала часа)"]

    if hours <= 12:
        reasons.append(f"Короткая база: {hours}ч (ширина {width}%) (+15)")
    else:
        reasons.append(f"Тяжёлая база: {hours}ч (ширина {width}%) (+25)")

    if close_h >= range_h:
        score += 15
        reasons.append(f"Пробой верхней границы ${range_h:.6f} (+15)")
    else:
        score += 10
        reasons.append(f"Поджим к границе ({distance_to_high:.2f}%) (+10)")

    if rvol >= 2.5:
        score += 20
        reasons.append(f"🔥 Взрывной объём x{round(rvol, 1)} (+20)")
    elif rvol >= MIN_1H_RVOL:
        score += 10
        reasons.append(f"Объём выше среднего x{round(rvol, 1)} (+10)")

    if ema_type:
        score += ema_score
        reasons.append(ema_detail)

    # 🔧 Добавляем баллы за стопы
    if stop_score > 0:
        score += stop_score
        reasons.extend(stop_details)

    if score >= 60:
        return {
            "score": score,
            "reasons": reasons,
            "price": close_h,
            "change_1h": round(change_pct, 2),
            "rvol": round(rvol, 1),
            "flat_width": width,
            "hours": hours,
            "elapsed": int(elapsed),
            "ema_type": ema_type,
            "stop_clusters": stop_clusters
        }

    return None


# =====================================================================
# 🔄 ГЛАВНЫЙ ЦИКЛ
# =====================================================================
async def scan_market():
    logging.info("🚀 Real-Time сканер + Стоп-кластеры запущен...")

    async with aiohttp.ClientSession() as session:
        await send_telegram_message("🤖 **Сканер запущен!**\nReal-Time 1H + сбор стопов.")

        while True:
            try:
                symbols = await get_active_usdt_pairs(session)
                logging.info(f"Сканирую {len(symbols)} пар...")

                for symbol in symbols:
                    klines_1h = await get_klines(session, symbol, "1h", limit=80)
                    if not klines_1h or len(klines_1h) < 24:
                        continue

                    signal = evaluate_realtime_signal(klines_1h)
                    if not signal:
                        continue

                    cache_key = f"{symbol}_1h"
                    now_ts = time.time()
                    if cache_key in SIGNAL_CACHE and now_ts - SIGNAL_CACHE[cache_key] < COOLDOWN_SECONDS:
                        continue
                    SIGNAL_CACHE[cache_key] = now_ts

                    clean = symbol.replace("-USDT", "")
                    reasons_fmt = "\n".join([f"• {r}" for r in signal["reasons"]])

                    ema_label = ""
                    if signal.get("ema_type"):
                        labels = {"DUAL_EMA": "🔥 Отскок от EMA20+EMA40", "EMA40": "🦾 Отскок от EMA40", "EMA20": "💪 Отскок от EMA20"}
                        ema_label = f"\n🧲 **Сетап:** {labels.get(signal['ema_type'], '')}"

                    # 🔧 Информация о стоп-кластерах
                    stop_info = ""
                    if signal.get("stop_clusters"):
                        for c in signal["stop_clusters"]:
                            if c.get("swept"):
                                stop_info += f"\n🧹 **Сбор стопов:** ${c['level']:.6f} (зона {c['zone_low']:.6f} - {c['zone_high']:.6f})"

                    msg = (
                        f"⚡ **BINGX: REAL-TIME СИГНАЛ**\n\n"
                        f"📌 **Монета:** #{clean} / USDT\n"
                        f"💵 **Текущая цена:** `{signal['price']}`\n"
                        f"📊 **Рост за час:** `+{signal['change_1h']}%`\n"
                        f"📈 **Проекция RVOL:** `x{signal['rvol']}`\n"
                        f"⏰ **Прогресс свечи:** `{signal['elapsed']} мин из 60`\n"
                        f"📦 **База:** `{signal['hours']}ч` (ширина `{signal['flat_width']}%`)\n"
                        f"⭐ **Скор:** `{signal['score']}/100`{ema_label}{stop_info}\n\n"
                        f"🔍 **Факторы:**\n{reasons_fmt}\n\n"
                        f"🔗 [Открыть график](https://bingx.com/ru-ru/futures/forward/{clean}USDT/)"
                    )

                    await send_telegram_message(msg)
                    logging.info(f"SIGNAL: {symbol} | Score: {signal['score']} | RVOL: {signal['rvol']}")

                    await asyncio.sleep(0.1)

            except Exception as e:
                logging.error(f"Ошибка цикла: {e}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# =====================================================================
# 🌐 ВЕБ-СЕРВЕР
# =====================================================================
async def health(request):
    return web.Response(text="Real-Time + Stop Clusters OK", status=200)


async def self_ping():
    await asyncio.sleep(30)
    if not RENDER_EXTERNAL_URL: return
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(RENDER_EXTERNAL_URL) as r:
                    logging.info(f"Ping: {r.status}")
            except Exception as e:
                logging.error(f"Ping error: {e}")
            await asyncio.sleep(600)


async def main():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    logging.info(f"Web-сервер на порту {port}")

    asyncio.create_task(self_ping())
    await scan_market()


if __name__ == "__main__":
    asyncio.run(main())
