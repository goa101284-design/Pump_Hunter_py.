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
MIN_24H_VOLUME_USDT = 150000
SCAN_INTERVAL_SECONDS = 30

FULL_SCAN_INTERVAL_SECONDS = 3600
HOLD_AFTER_EXIT_SECONDS = 7200

MIN_1H_CHANGE_PCT = 0.4
MIN_1H_RVOL = 0.7
IDEAL_ENTRY_MINUTES = (49, 56)

BLACKLIST_FILE = "blacklist.txt"
SIGNAL_CACHE = {}
COOLDOWN_SECONDS = 1800

WATCHLIST = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# =====================================================================
# 🚫 УПРАВЛЕНИЕ ЧЁРНЫМ СПИСКОМ
# =====================================================================
def load_blacklist():
    coins = {"USDT", "USDC", "BUSD", "EUR", "GBP", "DAI", "TUSD"}
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    c = line.strip().upper()
                    if c and not c.startswith("#"):
                        coins.add(c)
        except Exception as e:
            logging.error(f"Ошибка загрузки blacklist: {e}")
    return coins


def save_to_blacklist(coin_name: str):
    coin = coin_name.strip().upper()
    try:
        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
            f.write(f"{coin}\n")
    except Exception as e:
        logging.error(f"Ошибка сохранения в blacklist: {e}")


def remove_from_blacklist(coin_name: str):
    coin = coin_name.strip().upper()
    coins = load_blacklist()
    coins.discard(coin)
    try:
        with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
            for c in coins:
                if c not in ["USDT", "USDC", "BUSD", "EUR", "GBP", "DAI", "TUSD"]:
                    f.write(f"{c}\n")
    except Exception as e:
        logging.error(f"Ошибка удаления из blacklist: {e}")


# =====================================================================
# 📡 BINGX API & TELEGRAM COMMANDS
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


async def handle_telegram_commands():
    offset = 0
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params = {"offset": offset, "timeout": 10}
                async with session.get(url, params=params, timeout=12) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for update in data.get("result", []):
                            offset = update["update_id"] + 1
                            message = update.get("message", {})
                            text = message.get("text", "").strip()
                            chat_id = str(message.get("chat", {}).get("id", ""))

                            if chat_id != str(TELEGRAM_CHAT_ID):
                                continue

                            if text.startswith("/block") or text.startswith("/ban"):
                                parts = text.split()
                                if len(parts) > 1:
                                    coin = parts[1].replace("#", "").replace("-USDT", "").replace("USDT", "").upper()
                                    save_to_blacklist(coin)
                                    symbol_key = f"{coin}-USDT"
                                    if symbol_key in WATCHLIST:
                                        del WATCHLIST[symbol_key]
                                    await send_telegram_message(f"🚫 **Монета #{coin} добавлена в Чёрный Список!**")
                                else:
                                    await send_telegram_message("⚠️ Пример: `/block ESPORT`")

                            elif text.startswith("/unblock"):
                                parts = text.split()
                                if len(parts) > 1:
                                    coin = parts[1].replace("#", "").replace("-USDT", "").replace("USDT", "").upper()
                                    remove_from_blacklist(coin)
                                    await send_telegram_message(f"✅ **Монета #{coin} удалена из Чёрного Списка.**")
                                else:
                                    await send_telegram_message("⚠️ Пример: `/unblock ESPORT`")

                            elif text == "/blacklist":
                                current_list = load_blacklist()
                                custom_coins = [c for c in current_list if c not in ["USDT", "USDC", "BUSD", "EUR", "GBP", "DAI", "TUSD"]]
                                if custom_coins:
                                    coins_str = ", ".join([f"`{c}`" for c in custom_coins])
                                    await send_telegram_message(f"📜 **Чёрный список:**\n{coins_str}")
                                else:
                                    await send_telegram_message("📜 Чёрный список пуст.")

            except Exception as e:
                logging.error(f"Ошибка Telegram команд: {e}")

            await asyncio.sleep(3)


async def get_active_usdt_pairs(session):
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    exclude_set = load_blacklist()
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
                    base_coin = symbol.split("-")[0].upper()
                    if base_coin in exclude_set: continue

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
                data = res.get("data", [])
                if data:
                    data.reverse()
                return data
    except Exception as e:
        logging.error(f"Ошибка Klines {symbol}: {e}")
    return []


# =====================================================================
# 🧠 АНАЛИТИЧЕСКИЙ БЛОК
# =====================================================================
def calculate_ema(closes, period):
    if len(closes) < period: return closes[-1]
    return pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1]


def find_base_params(klines_1h, min_hours, max_hours, max_width):
    closed = klines_1h[:-1]

    for w in range(max_hours, min_hours - 1, -2):
        if len(closed) < w: continue
        recent = closed[-w:]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        vols = [float(k[5]) * float(k[4]) for k in recent]

        ranges = [(float(k[2]) - float(k[3])) / float(k[3]) * 100 for k in recent]
        avg_range = np.mean(ranges)
        max_range = max(ranges)

        if max_range > max_width * 2 and avg_range > max_width * 0.8: continue

        range_high = max(highs)
        range_low = min(lows)
        if range_low <= 0: continue

        flat_width = ((range_high - range_low) / range_low) * 100
        quiet_vol = np.median(vols) if vols else 0.0

        if flat_width <= max_width and quiet_vol > 0:
            return True, round(flat_width, 2), range_high, range_low, w, quiet_vol

    return False, 0.0, 0.0, 0.0, 0, 0.0


def check_ema_bounce(klines_1h, range_high, range_low):
    closes = [float(k[4]) for k in klines_1h]
    ema20 = calculate_ema(closes, 20)
    ema40 = calculate_ema(closes, 40)

    ema20_in = range_low <= ema20 <= range_high
    ema40_in = range_low <= ema40 <= range_high

    if not (ema20_in or ema40_in): return None, 0, ""

    dist = abs((ema20 - ema40) / ema40) * 100 if ema40 > 0 else 100
    squeezed = dist <= 2.0

    curr = klines_1h[-1]
    open_h, close_h = float(curr[1]), float(curr[4])

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

    if touched40 and green and above40: return "EMA40", 25, "🦾 Отскок от EMA40 (+25)"
    if touched20 and green and above20: return "EMA20", 20, "💪 Отскок от EMA20 (+20)"

    return None, 0, ""


def evaluate_realtime_signal(klines_1h):
    if len(klines_1h) < 24: return None

    # Поиск базы накопления
    is_flat, width, range_h, range_l, hours, avg_vol = find_base_params(klines_1h, 4, 12, 6.0)
    if not is_flat:
        is_flat, width, range_h, range_l, hours, avg_vol = find_base_params(klines_1h, 24, 72, 7.0)
    if not is_flat: return None

    curr = klines_1h[-1]
    open_h, high_h, low_h, close_h = float(curr[1]), float(curr[2]), float(curr[3]), float(curr[4])
    vol_h = float(curr[5]) * close_h

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    elapsed = now_dt.minute + now_dt.second / 60
    if elapsed < 1: elapsed = 1

    projected_vol = vol_h * (60 / elapsed)
    rvol = projected_vol / avg_vol if avg_vol > 0 else 1.0
    change_pct = ((close_h - open_h) / open_h) * 100
    candle_range = ((high_h - low_h) / low_h) * 100

    # ---------------------------------------------------------------------
    # 🐭 РЕЖИМ 2: СКРЫТЫЙ АЛГОРИТМИЧЕСКИЙ ЗАКУП (Усиленная версия)
    # ---------------------------------------------------------------------
    if len(klines_1h) >= 9:
        last = klines_1h[-8:-1]

        hist_ranges = [(float(k[2]) - float(k[3])) / float(k[3]) * 100 for k in last]
        hist_bodies = [abs(float(k[4]) - float(k[1])) / float(k[1]) * 100 for k in last]
        hist_money = [float(k[5]) * float(k[4]) for k in last]
        hist_closes = [float(k[4]) for k in last]

        avg_money = np.mean(hist_money) if hist_money else 0.0
        avg_close = np.mean(hist_closes) if hist_closes else 0.0
        
        stable_count = sum(1 for m in hist_money if 0.65 * avg_money <= m <= 1.35 * avg_money) if avg_money > 0 else 0
        stable_money = stable_count >= 5

        close_deviations = [abs(c - avg_close) / avg_close * 100 for c in hist_closes] if avg_close > 0 else [100]
        stable_closes = max(close_deviations) <= 0.7 and np.mean(close_deviations) <= 0.4

        quiet_history = (
            np.mean(hist_ranges) <= 1.2 and
            np.mean(hist_bodies) <= 0.5 and
            stable_money and
            stable_closes
        )

        body_pct = abs(close_h - open_h) / open_h * 100
        upper_wick = (high_h - max(open_h, close_h)) / open_h * 100
        lower_wick = (min(open_h, close_h) - low_h) / open_h * 100
        total_wicks = upper_wick + lower_wick

        if (
            quiet_history and
            candle_range <= 1.4 and
            body_pct <= 0.7 and
            total_wicks <= 1.8 and
            0.8 <= rvol <= 2.0
        ):
            return {
                "type": "QUIET_ACCUMULATION",
                "score": 90,
                "price": close_h,
                "change_1h": round(change_pct, 2),
                "rvol": round(rvol, 1),
                "hours": hours,
                "width": width,
                "elapsed": int(elapsed),
                "reasons": [
                    "🐭 **Затаившаяся мышь (DRIFT Mode v2):** Алгоритмический набор позиции!",
                    f"📊 **Поток денег:** {stable_count}/7 свечей в ровном коридоре",
                    f"📏 **Стабильность закрытий:** откл. `max {max(close_deviations):.2f}%`",
                    f"🕯 **Анатомия H1:** Диапазон `{candle_range:.2f}%`, тени `↑{upper_wick:.2f}% + ↓{lower_wick:.2f}% = {total_wicks:.2f}%`",
                    f"⚖️ **RVOL:** `x{round(rvol, 1)}` — набивание брюха без шума в стакане"
                ]
            }

    # ---------------------------------------------------------------------
    # ⚡ РЕЖИМ 1: КЛАССИЧЕСКИЙ H1 BREAKOUT
    # ---------------------------------------------------------------------
    if change_pct < MIN_1H_CHANGE_PCT or rvol < MIN_1H_RVOL: return None

    max_entry_change = 5.0 if width <= 3.0 else (8.0 if width <= 5.0 else 12.0)
    if change_pct > max_entry_change: return None

    distance_to_high = ((range_h - close_h) / range_h) * 100 if close_h < range_h else 0.0
    if close_h < range_h and distance_to_high > 1.5: return None

    ema_type, ema_score, ema_detail = check_ema_bounce(klines_1h, range_h, range_l)

    score = 50
    elapsed_minutes = int(elapsed)
    reasons = [f"⏰ Real-Time H1 ({elapsed_minutes} мин от начала часа)"]

    if hours <= 12: reasons.append(f"Короткая база: {hours}ч (ширина {width}%) (+15)")
    else: reasons.append(f"Тяжёлая база: {hours}ч (ширина {width}%) (+25)")

    if close_h >= range_h:
        score += 15
        reasons.append(f"Пробой верхней границы ${range_h:.6f} (+15)")
    else:
        score += 10
        reasons.append(f"Поджим к границе ({distance_to_high:.2f}%) (+10)")

    if IDEAL_ENTRY_MINUTES[0] <= elapsed_minutes <= IDEAL_ENTRY_MINUTES[1]:
        score += 20
        reasons.append(f"🔥 Идеальный тайминг перед закрытием H1! ({elapsed_minutes}я мин) (+20)")
    elif elapsed_minutes >= 40:
        score += 5
        reasons.append(f"⏱ Подготовка к закрытию часа ({elapsed_minutes}я мин) (+5)")

    if rvol >= 2.5: score += 20; reasons.append(f"🔥 Взрывной объём x{round(rvol, 1)} (+20)")
    elif rvol >= 1.0: score += 15; reasons.append(f"📊 Ровный объём x{round(rvol, 1)} (+15)")

    if ema_type:
        score += ema_score
        reasons.append(ema_detail)

    if score >= 45:
        return {
            "type": "H1_BREAKOUT",
            "score": score,
            "reasons": reasons,
            "price": close_h,
            "change_1h": round(change_pct, 2),
            "rvol": round(rvol, 1),
            "width": width,
            "hours": hours,
            "elapsed": elapsed_minutes,
            "ema_type": ema_type
        }

    return None


# =====================================================================
# 🔄 ГЛАВНЫЙ ЦИКЛ СКАНИРОВАНИЯ
# =====================================================================
async def scan_market():
    global SIGNAL_CACHE, WATCHLIST
    logging.info("🚀 Сканер H1 запущен...")
    last_full_scan_time = 0

    async with aiohttp.ClientSession() as session:
        await send_telegram_message("🤖 **Сканер запущен и готов!**\n• Режим 1: Пробои баз H1 (EMA 20/40)\n• Режим 2: Затаившаяся мышь v2 (DRIFT Mode)")

        while True:
            try:
                now_ts = time.time()

                if now_ts - last_full_scan_time > FULL_SCAN_INTERVAL_SECONDS or not WATCHLIST:
                    all_symbols = await get_active_usdt_pairs(session)
                    SIGNAL_CACHE = {k: v for k, v in SIGNAL_CACHE.items() if now_ts - v < COOLDOWN_SECONDS * 2}
                    
                    for symbol in all_symbols:
                        if symbol not in WATCHLIST:
                            WATCHLIST[symbol] = {"status": "ACTIVE"}
                    last_full_scan_time = now_ts
                    logging.info(f"✅ Активных пар в обработке: {len(WATCHLIST)}...")

                for symbol in list(WATCHLIST.keys()):
                    klines_1h = await get_klines(session, symbol, "1h", limit=80)
                    if not klines_1h or len(klines_1h) < 24: continue

                    signal = evaluate_realtime_signal(klines_1h)
                    if not signal: continue

                    cache_key = f"{symbol}_{signal['type']}"
                    if cache_key in SIGNAL_CACHE and now_ts - SIGNAL_CACHE[cache_key] < COOLDOWN_SECONDS:
                        continue
                    SIGNAL_CACHE[cache_key] = now_ts

                    clean = symbol.replace("-USDT", "")
                    reasons_fmt = "\n".join([f"• {r}" for r in signal["reasons"]])

                    if signal["type"] == "QUIET_ACCUMULATION":
                        msg = (
                            f"🚨 **🔍 СКРЫТЫЙ АЛГОРИТМИЧЕСКИЙ ЗАКУП**\n\n"
                            f"📌 **Монета:** #{clean} / USDT\n"
                            f"💵 **Текущая цена:** `{signal['price']}`\n"
                            f"📊 **Изменение H1:** `{signal['change_1h']}%`\n"
                            f"📈 **Проекция RVOL:** `x{signal['rvol']}`\n"
                            f"⏰ **Минута часа:** `{signal['elapsed']} из 60`\n"
                            f"📦 **База:** `{signal['hours']}ч` (ширина `{signal['width']}%`)\n"
                            f"⭐ **Скор:** `{signal['score']}/100`\n\n"
                            f"🔍 **Анализ паттерна:**\n{reasons_fmt}\n\n"
                            f"🔗 [Открыть график](https://bingx.com/ru-ru/futures/forward/{clean}USDT/)"
                        )
                    else:
                        ema_label = f"\n🧲 **Сетап:** {signal['ema_type']}" if signal.get("ema_type") else ""
                        msg = (
                            f"⚡ **BINGX: REAL-TIME H1 СИГНАЛ**\n\n"
                            f"📌 **Монета:** #{clean} / USDT\n"
                            f"💵 **Текущая цена:** `{signal['price']}`\n"
                            f"📊 **Рост за час:** `+{signal['change_1h']}%`\n"
                            f"📈 **Проекция RVOL:** `x{signal['rvol']}`\n"
                            f"⏰ **Минута часа:** `{signal['elapsed']} из 60`\n"
                            f"📦 **База:** `{signal['hours']}ч` (ширина `{signal['width']}%`)\n"
                            f"⭐ **Скор:** `{signal['score']}/100`{ema_label}\n\n"
                            f"🔍 **Факторы:**\n{reasons_fmt}\n\n"
                            f"🔗 [Открыть график](https://bingx.com/ru-ru/futures/forward/{clean}USDT/)"
                        )

                    await send_telegram_message(msg)
                    logging.info(f"SIGNAL [{signal['type']}]: {symbol}")
                    await asyncio.sleep(0.05)

            except Exception as e:
                logging.error(f"Ошибка главного цикла: {e}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# =====================================================================
# 🌐 ВЕБ-СЕРВЕР
# =====================================================================
async def health(request):
    return web.Response(text=f"Dual H1 Scanner Active. Coins: {len(WATCHLIST)}", status=200)

async def self_ping():
    await asyncio.sleep(30)
    if not RENDER_EXTERNAL_URL: return
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(RENDER_EXTERNAL_URL) as r: pass
            except Exception: pass
            await asyncio.sleep(600)

async def main():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()

    asyncio.create_task(self_ping())
    asyncio.create_task(handle_telegram_commands())
    await scan_market()

if __name__ == "__main__":
    asyncio.run(main())
