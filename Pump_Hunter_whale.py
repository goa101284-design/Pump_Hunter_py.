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
MIN_24H_VOLUME_USDT = 80000
SCAN_INTERVAL_SECONDS = 30

FULL_SCAN_INTERVAL_SECONDS = 3600
IDEAL_ENTRY_MINUTES = (49, 56)

BLACKLIST_FILE = "blacklist.txt"
SIGNAL_CACHE = {}
COOLDOWN_SECONDS = 7200  # 2 часа паузы для монеты после сигнала

WATCHLIST = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# =====================================================================
# 🛠 ВСПOМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАЗБОРА СВЕЧЕЙ
# =====================================================================
def parse_kline(kline):
    if isinstance(kline, dict):
        open_p = float(kline.get("open", kline.get("o", 0)))
        high_p = float(kline.get("high", kline.get("h", 0)))
        low_p = float(kline.get("low", kline.get("l", 0)))
        close_p = float(kline.get("close", kline.get("c", 0)))
        vol = float(kline.get("volume", kline.get("v", 0)))
    elif isinstance(kline, (list, tuple)):
        open_p = float(kline[1])
        high_p = float(kline[2])
        low_p = float(kline[3])
        close_p = float(kline[4])
        vol = float(kline[5])
    else:
        open_p = high_p = low_p = close_p = vol = 0.0
    return open_p, high_p, low_p, close_p, vol


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
            logging.error(f"❌ Ошибка загрузки blacklist: {e}")
    return coins


def save_to_blacklist(coin_name: str):
    coin = coin_name.strip().upper()
    try:
        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
            f.write(f"{coin}\n")
    except Exception as e:
        logging.error(f"❌ Ошибка сохранения в blacklist: {e}")


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
        logging.error(f"❌ Ошибка удаления из blacklist: {e}")


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
    
    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status == 200:
                        return
                    elif resp.status == 429:
                        res_json = await resp.json()
                        retry_after = res_json.get("parameters", {}).get("retry_after", 5)
                        logging.warning(f"⚠️ Telegram Rate Limit (429)! Ждем {retry_after} секунд...")
                        await asyncio.sleep(retry_after + 1)
                    else:
                        logging.error(f"❌ Ошибка Telegram: Status {resp.status} - {await resp.text()}")
                        break
        except Exception as e:
            logging.error(f"❌ Исключение Telegram: {e}")
            await asyncio.sleep(2)


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
                logging.error(f"❌ Ошибка Telegram команд: {e}")

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
                    if symbol.startswith("NC") or "USD2" in symbol or "NATGAS" in symbol: continue
                    if symbol[0].isdigit(): continue

                    base_coin = symbol.split("-")[0].upper()
                    if base_coin in exclude_set: continue

                    if vol >= MIN_24H_VOLUME_USDT:
                        valid.append(symbol)
                logging.info(f"🌐 Загружено {len(valid)} активных крипто-пар...")
                return valid
            else:
                logging.error(f"❌ BingX Ticker Ошибка: Code {res.get('code')}")
    except Exception as e:
        logging.error(f"❌ Ошибка получения пар: {e}")
    return []


async def get_klines(session, symbol: str, interval: str, limit: int = 80):
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    clean_symbol = symbol if "-" in symbol else f"{symbol[:-4]}-USDT"
    params = {"symbol": clean_symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=5) as resp:
            res = await resp.json()
            code = res.get("code")
            if code == 0:
                data = res.get("data", [])
                if data:
                    data.reverse()
                return data
            elif code == 109429:
                await asyncio.sleep(1.0)
            elif code in [109400, 109404, 80001]:
                return "INVALID_SYMBOL"
            else:
                logging.warning(f"⚠️ Ошибка Klines [{symbol}]: Code {code}")
    except Exception as e:
        logging.error(f"❌ Исключение Klines [{symbol}]: {e}")
    return []


# =====================================================================
# 🧠 АНАЛИТИЧЕСКИЙ БЛОК (СТРОГИЙ ФИЛЬТР)
# =====================================================================
def calculate_ema(closes, period):
    if len(closes) < period: return closes[-1]
    return pd.Series(closes).ewm(span=period, adjust=False).mean().iloc[-1]


def find_base_params(klines_1h, min_hours, max_hours, max_width):
    closed = klines_1h[:-1]

    for w in range(max_hours, min_hours - 1, -2):
        if len(closed) < w: continue
        recent = closed[-w:]
        
        highs, lows, vols = [], [], []
        for k in recent:
            _, h, l, c, v = parse_kline(k)
            highs.append(h)
            lows.append(l)
            vols.append(v * c)

        range_high = max(highs) if highs else 0
        range_low = min(lows) if lows else 0
        if range_low <= 0: continue

        flat_width = ((range_high - range_low) / range_low) * 100
        quiet_vol = np.median(vols) if vols else 0.0

        if flat_width <= max_width and quiet_vol > 0:
            return True, round(flat_width, 2), range_high, range_low, w, quiet_vol

    return False, 0.0, 0.0, 0.0, 0, 0.0


def check_ema_bounce(klines_1h, range_high, range_low):
    closes = [parse_kline(k)[3] for k in klines_1h]
    ema20 = calculate_ema(closes, 20)
    ema40 = calculate_ema(closes, 40)

    ema20_in = range_low <= ema20 <= range_high
    ema40_in = range_low <= ema40 <= range_high

    if not (ema20_in or ema40_in): return None, 0, ""

    dist = abs((ema20 - ema40) / ema40) * 100 if ema40 > 0 else 100
    squeezed = dist <= 3.0

    curr = klines_1h[-1]
    _, _, _, close_h, _ = parse_kline(curr)

    prev = klines_1h[-5:-1]
    touched20 = any(abs(parse_kline(c)[2] - ema20) / ema20 * 100 <= 1.8 or abs(parse_kline(c)[3] - ema20) / ema20 * 100 <= 1.8 for c in prev)
    touched40 = any(abs(parse_kline(c)[2] - ema40) / ema40 * 100 <= 1.8 or abs(parse_kline(c)[3] - ema40) / ema40 * 100 <= 1.8 for c in prev)

    above20 = close_h > ema20
    above40 = close_h > ema40

    if touched20 and touched40 and (above20 or above40):
        score = 30
        detail = "🔥 Опора на EMA20+EMA40 (+30)"
        if squeezed:
            score += 15
            detail += f" | Сжатие EMA {dist:.1f}% (+15)"
        return "DUAL_EMA", score, detail

    if touched40 and above40: return "EMA40", 25, "🦾 Опора на EMA40 (+25)"
    if touched20 and above20: return "EMA20", 20, "💪 Опора на EMA20 (+20)"

    return None, 0, ""


def evaluate_realtime_signal(klines_1h):
    if len(klines_1h) < 24: return None

    curr = klines_1h[-1]
    open_h, high_h, low_h, close_h, vol_raw = parse_kline(curr)
    vol_h = vol_raw * close_h

    if open_h <= 0 or low_h <= 0: return None

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    elapsed = now_dt.minute + now_dt.second / 60
    if elapsed < 1: elapsed = 1

    change_pct = ((close_h - open_h) / open_h) * 100
    candle_range = ((high_h - low_h) / low_h) * 100

    past_vols = [parse_kline(k)[4] * parse_kline(k)[3] for k in klines_1h[-21:-1]]
    avg_vol = np.mean(past_vols) if past_vols else 1.0
    projected_vol = vol_h * (60 / elapsed)
    rvol = projected_vol / avg_vol if avg_vol > 0 else 1.0

    closes = [parse_kline(k)[3] for k in klines_1h]
    ema20_val = calculate_ema(closes, 20)

    # 1. Жесткая узкая база (Максимум 4.5% - 6.0% ширины)
    is_flat, width, range_h, range_l, hours, _ = find_base_params(klines_1h, 4, 12, 4.5)
    if not is_flat:
        is_flat, width, range_h, range_l, hours, _ = find_base_params(klines_1h, 12, 48, 6.0)

    ema_type, ema_score, ema_detail = check_ema_bounce(klines_1h, range_h, range_l) if is_flat else (None, 0, "")
    ema_detail = ema_detail if ema_type else ""

    # 2. Скрытая мышь (Набор без шума)
    if is_flat and len(klines_1h) >= 9:
        last = klines_1h[-8:-1]
        hist_ranges, hist_bodies, hist_money, hist_closes = [], [], [], []
        for k in last:
            o, h, l, c, v = parse_kline(k)
            if l > 0 and o > 0:
                hist_ranges.append((h - l) / l * 100)
                hist_bodies.append(abs(c - o) / o * 100)
                hist_money.append(v * c)
                hist_closes.append(c)

        avg_money = np.mean(hist_money) if hist_money else 0.0
        avg_close = np.mean(hist_closes) if hist_closes else 0.0

        stable_count = sum(1 for m in hist_money if 0.5 * avg_money <= m <= 1.5 * avg_money) if avg_money > 0 else 0
        close_deviations = [abs(c - avg_close) / avg_close * 100 for c in hist_closes] if avg_close > 0 else [100]

        quiet_history = (
            np.mean(hist_ranges) <= 2.2 and
            np.mean(hist_bodies) <= 1.2 and
            stable_count >= 5 and
            max(close_deviations) <= 0.8
        ) if hist_ranges and hist_bodies else False

        body_pct = abs(close_h - open_h) / open_h * 100

        if quiet_history and candle_range <= 1.5 and body_pct <= 0.6 and rvol >= 1.5:
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
                    "🐭 **Затаившаяся мышь:** Набор позиции в узком флэте!",
                    f"📊 **Поток денег:** {stable_count}/7 свечей без рывков",
                    f"🕯 **Анатомия H1:** Диапазон `{candle_range:.2f}%` | RVOL `x{round(rvol,1)}`"
                ]
            }

    # 3. Первая импульсная свеча (Пробой узкой базы + рост от 1.6% + RVOL x1.8+)
    if is_flat and change_pct >= 1.6 and rvol >= 1.8 and close_h > ema20_val:
        is_dangerous = change_pct > 6.0
        return {
            "type": "H1_BREAKOUT",
            "score": 85,
            "reasons": [
                f"🚀 **ПЕРВАЯ ИМПУЛЬСНАЯ СВЕЧА ({int(elapsed)}-я мин):** Пробой базы!",
                f"📊 Импульс: +{round(change_pct,2)}% | Проекция RVOL: x{round(rvol,1)}",
                f"📦 Накопление: {hours}ч (ширина {width}%)",
                ema_detail if ema_detail else "🧲 Закрепление над EMA20"
            ],
            "price": close_h,
            "change_1h": round(change_pct, 2),
            "rvol": round(rvol, 1),
            "width": width,
            "hours": hours,
            "elapsed": int(elapsed),
            "ema_type": ema_type,
            "is_dangerous": is_dangerous
        }

    # 4. Залив на 49-56 мин (Только при росте от 1.2% и RVOL x2.0+)
    if is_flat and (IDEAL_ENTRY_MINUTES[0] <= int(elapsed) <= IDEAL_ENTRY_MINUTES[1]):
        if change_pct >= 1.2 and rvol >= 2.0 and close_h > ema20_val:
            return {
                "type": "H1_BREAKOUT",
                "score": 90,
                "reasons": [
                    f"🎯 **ЗАЛИВ В БАЗЕ ({int(elapsed)}я мин):** Набор объема перед закрытием H1!",
                    f"📊 Импульс: +{round(change_pct,2)}% | RVOL: x{round(rvol,1)}",
                    f"📦 База: {hours}ч (ширина {width}%)",
                    ema_detail if ema_detail else ""
                ],
                "price": close_h,
                "change_1h": round(change_pct, 2),
                "rvol": round(rvol, 1),
                "width": width,
                "hours": hours,
                "elapsed": int(elapsed),
                "ema_type": ema_type,
                "is_dangerous": False
            }

    return None


# =====================================================================
# 🔄 ГЛАВНЫЙ ЦИКЛ СКАНИРОВАНИЯ
# =====================================================================
async def process_symbol(session, symbol, now_ts):
    klines_1h = await get_klines(session, symbol, "1h", limit=80)
    
    if klines_1h == "INVALID_SYMBOL":
        if symbol in WATCHLIST:
            del WATCHLIST[symbol]
        return None

    if not klines_1h or len(klines_1h) < 24:
        return None

    signal = evaluate_realtime_signal(klines_1h)
    if not signal:
        return None

    cache_key = f"{symbol}_{signal['type']}"
    if cache_key in SIGNAL_CACHE and now_ts - SIGNAL_CACHE[cache_key] < COOLDOWN_SECONDS:
        return None

    SIGNAL_CACHE[cache_key] = now_ts
    return symbol, signal


async def scan_market():
    global SIGNAL_CACHE, WATCHLIST
    logging.info("🚀 Сканер запущен. Оптимизированный разбор типов свечей!")
    last_full_scan_time = 0

    async with aiohttp.ClientSession() as session:
        await send_telegram_message("🤖 **Сканер перезапущен с обновленными фильтрами!**")

        while True:
            start_time = time.time()
            now_ts = start_time

            try:
                if now_ts - last_full_scan_time > FULL_SCAN_INTERVAL_SECONDS or not WATCHLIST:
                    all_symbols = await get_active_usdt_pairs(session)
                    SIGNAL_CACHE = {k: v for k, v in SIGNAL_CACHE.items() if now_ts - v < COOLDOWN_SECONDS * 2}

                    if all_symbols:
                        WATCHLIST = {symbol: {"status": "ACTIVE"} for symbol in all_symbols}
                        last_full_scan_time = now_ts
                        logging.info(f"🔄 Список монет обновлен. Всего пар: {len(WATCHLIST)}")
                    else:
                        logging.warning("⚠️ Не удалось получить список монет. Повтор через 10 сек...")
                        await asyncio.sleep(10)
                        continue

                symbols_list = list(WATCHLIST.keys())
                total_symbols = len(symbols_list)
                signals_found = 0

                batch_size = 10
                for i in range(0, total_symbols, batch_size):
                    batch = symbols_list[i:i + batch_size]
                    tasks = [process_symbol(session, sym, now_ts) for sym in batch]
                    results = await asyncio.gather(*tasks)

                    for res in results:
                        if res:
                            symbol, signal = res
                            signals_found += 1
                            clean = symbol.replace("-USDT", "")
                            
                            # Очистка строк от незакрытых тегов Markdown
                            clean_reasons = []
                            for r in signal["reasons"]:
                                if r:
                                    clean_reasons.append(f"• {r}")
                            reasons_fmt = "\n".join(clean_reasons)

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
                                danger_header = "⚠️ **[ВХОД ОПАСЕН — АГРЕССИВНАЯ СВЕЧА]**\n" if signal.get("is_dangerous") else ""

                                msg = (
                                    f"{danger_header}"
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
                            logging.info(f"🔥 СИГНАЛ ОТПРАВЛЕН [{signal['type']}]: {symbol}")
                            await asyncio.sleep(0.5)

                    await asyncio.sleep(0.15)

                duration = round(time.time() - start_time, 2)
                logging.info(f"💓 [HEARTBEAT] Круг завершен за {duration}сек | Сканировано монет: {len(WATCHLIST)} | Найдено сигналов: {signals_found}")

            except Exception as e:
                logging.error(f"❌ Ошибка главного цикла: {e}", exc_info=True)

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# =====================================================================
# 🌐 ВЕБ-СЕРВЕР DUMMY
# =====================================================================
async def health(request):
    return web.Response(text=f"Scanner Active. Coins: {len(WATCHLIST)}", status=200)

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

    logging.info(f"🌐 Сервер запущен на порту {port}")

    asyncio.create_task(self_ping())
    asyncio.create_task(handle_telegram_commands())
    await scan_market()

if __name__ == "__main__":
    asyncio.run(main())
