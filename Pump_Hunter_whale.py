import asyncio
import logging
import os
import time
import numpy as np
import pandas as pd
import requests
from aiohttp import web
import aiohttp
from collections import defaultdict

# =====================================================================
# ⚙️ НАСТРОЙКИ И КОНФИГУРАЦИЯ
# =====================================================================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")

# Базовые параметры BingX API
BINGX_BASE_URL = "https://open-api.bingx.com"

# Список монет исключений
EXCLUDE_COINS = ["USDT", "USDC", "BUSD", "EUR", "GBP", "DAI", "TUSD"]

# Ограничение по минимальному 24h объему
MIN_24H_VOLUME_USDT = 50000 

# Тайм-аут между циклами
SCAN_INTERVAL_SECONDS = 60

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

SIGNAL_CACHE = {} 
COOLDOWN_SECONDS = 1800


# =====================================================================
# 📡 РАБОТА С BINGX API & TELEGRAM
# =====================================================================
async def send_telegram_message(text: str):
    """Отправка сообщений в Telegram чат/канал"""
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
                    logging.error(f"Ошибка отправки в Telegram: {await resp.text()}")
    except Exception as e:
        logging.error(f"Исключение при отправке в Telegram: {e}")


async def get_active_usdt_pairs(session):
    """Получение списка активных USDT-M фьючерсных пар BingX с фильтрацией по объему"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/ticker"
    try:
        async with session.get(url, timeout=10) as resp:
            res = await resp.json()
            if res.get("code") == 0:
                data = res.get("data", [])
                valid_symbols = []
                for item in data:
                    symbol = item.get("symbol")
                    vol_usdt = float(item.get("quoteVolume", 0))
                    
                    if not symbol.endswith("-USDT"):
                        continue
                    
                    base_coin = symbol.split("-")[0]
                    if base_coin in EXCLUDE_COINS:
                        continue
                    
                    if vol_usdt >= MIN_24H_VOLUME_USDT:
                        valid_symbols.append(symbol)
                        
                return valid_symbols
    except Exception as e:
        logging.error(f"Ошибка получения списка пар: {e}")
    return []


async def get_klines(session, symbol: str, interval: str, limit: int = 100):
    """Запросить свечной график (Klines)"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v3/quote/klines"
    clean_symbol = symbol.replace("-", "")
    params = {"symbol": clean_symbol, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            res = await resp.json()
            if res.get("code") == 0:
                return res.get("data", [])
    except Exception as e:
        logging.error(f"Ошибка Klines для {symbol} ({interval}): {e}")
    return []


async def get_open_interest_delta(session, symbol: str):
    """Получить дельту Открытого Интереса (OI)"""
    url = f"{BINGX_BASE_URL}/openApi/swap/v2/quote/openInterest"
    clean_symbol = symbol.replace("-", "")
    params = {"symbol": clean_symbol}
    try:
        async with session.get(url, params=params, timeout=5) as resp:
            res = await resp.json()
            if res.get("code") == 0:
                data = res.get("data", {})
                open_interest = float(data.get("openInterest", 0))
                return True, open_interest
    except Exception:
        pass
    return False, 0.0


# =====================================================================
# 🧠 ДВУХРЕЖИМНЫЙ АНАЛИТИЧЕСКИЙ ДВИЖОК
# =====================================================================
def calculate_ema(closes, period):
    """Расчет Экспоненциального Скользящего Среднего (EMA)"""
    if len(closes) < period:
        return closes[-1]
    df = pd.DataFrame({"close": closes})
    ema = df["close"].ewm(span=period, adjust=False).mean()
    return ema.iloc[-1]


def find_support_resistance(klines_1h, lookback=48):
    """
    Поиск уровней поддержки и сопротивления на основе фракталов.
    Группирует локальные экстремумы в кластеры для определения сильных уровней.
    """
    if len(klines_1h) < lookback:
        return None, None, 0, 0
    
    recent = klines_1h[-lookback:]
    highs = [float(k[2]) for k in recent]
    lows = [float(k[3]) for k in recent]
    
    resistance_levels = []
    for i in range(2, len(highs) - 2):
        if highs[i] >= highs[i-1] and highs[i] >= highs[i-2] and highs[i] >= highs[i+1] and highs[i] >= highs[i+2]:
            resistance_levels.append(highs[i])
    
    support_levels = []
    for i in range(2, len(lows) - 2):
        if lows[i] <= lows[i-1] and lows[i] <= lows[i-2] and lows[i] <= lows[i+1] and lows[i] <= lows[i+2]:
            support_levels.append(lows[i])
    
    def cluster_levels(levels, threshold_pct=0.5):
        if not levels:
            return []
        levels = sorted(levels)
        clusters = []
        current_cluster = [levels[0]]
        
        for lvl in levels[1:]:
            if current_cluster and abs((lvl - current_cluster[-1]) / current_cluster[-1]) * 100 <= threshold_pct:
                current_cluster.append(lvl)
            else:
                clusters.append(current_cluster)
                current_cluster = [lvl]
        clusters.append(current_cluster)
        
        return [(np.mean(c), len(c)) for c in clusters if len(c) >= 2]
    
    resistance_clusters = cluster_levels(resistance_levels)
    support_clusters = cluster_levels(support_levels)
    
    strongest_resistance = max(resistance_clusters, key=lambda x: x[1]) if resistance_clusters else (None, 0)
    strongest_support = max(support_clusters, key=lambda x: x[1]) if support_clusters else (None, 0)
    
    return strongest_support[0], strongest_resistance[0], strongest_support[1], strongest_resistance[1]


def find_base_params(klines_1h, min_hours, max_hours, max_allowed_width):
    """Универсальный детектор длинных/коротких спальных баз"""
    closed_1h = klines_1h[:-1]
    
    for w in range(max_hours, min_hours - 1, -2):
        if len(closed_1h) < w:
            continue
            
        recent = closed_1h[-w:]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        vols = [float(k[5]) * float(k[4]) for k in recent]
        
        range_high = max(highs)
        range_low = min(lows)
        avg_vol = np.mean(vols) if vols else 0.0
        
        if range_low <= 0:
            continue
            
        flat_width = ((range_high - range_low) / range_low) * 100
        
        if flat_width <= max_allowed_width:
            return True, round(flat_width, 2), range_high, range_low, w, avg_vol
            
    return False, 0.0, 0.0, 0.0, 0, 0.0


def evaluate_all_signals(klines_1h, klines_5m, oi_valid, oi_val):
    """
    Анализирует монету на 2 параллельных типа сигналов:
    1. Quick Pump (+6.0% ширина базы max)
    2. Heavy Rocket (+7.0% ширина базы max)
    """
    if len(klines_5m) < 20 or len(klines_1h) < 48:
        return []

    support_level, resistance_level, support_touches, resistance_touches = find_support_resistance(klines_1h)

    signals = []

    # -------------------------------------------------------------
    # ⚡ РЕЖИМ 1: БЫСТРЫЙ ЛОКАЛЬНЫЙ ИМПУЛЬС (+5%..12%)
    # -------------------------------------------------------------
    # 🔧 ОБНОВЛЕНО: max_allowed_width = 6.0% (для щиткоинов)
    is_flat, width, range_h, range_l, hours, avg_vol = find_base_params(
        klines_1h, min_hours=4, max_hours=12, max_allowed_width=6.0
    )
    
    if is_flat:
        curr_5m = klines_5m[-1]
        open_5m, close_5m = float(curr_5m[1]), float(curr_5m[4])
        change_5m = ((close_5m - open_5m) / open_5m) * 100
        
        # Динамический порог 5m свечи: если база > 4.0%, требовать импульс минимум 0.9%
        min_5m_impulse = 0.9 if width > 4.0 else 0.6
        
        if change_5m >= min_5m_impulse:
            vol_5m = float(curr_5m[5]) * close_5m
            projected_1h_vol = vol_5m * 12
            spike_ratio = projected_1h_vol / avg_vol if avg_vol > 0 else 1.0

            closes_1h = [float(k[4]) for k in klines_1h]
            ema20 = calculate_ema(closes_1h, 20)
            prev_close_5m = float(klines_5m[-2][4])
            is_ema_breakout = (prev_close_5m <= ema20 and close_5m > ema20)

            if close_5m >= ema20 or is_ema_breakout:
                score = 50
                reasons = [f"Короткая база: {hours}ч (ширина {width}%) (+15)"]

                if resistance_level and close_5m > resistance_level:
                    score += 10
                    reasons.append(f"Пробой сопротивления ${resistance_level} (касаний: {resistance_touches}) (+10)")
                elif support_level:
                    reasons.append(f"📏 Ближайшая поддержка: ${support_level} (касаний: {support_touches})")

                if spike_ratio >= 1.6:
                    score += 20
                    reasons.append(f"Локальный всплеск объема x{round(spike_ratio, 1)} (+20)")

                if is_ema_breakout:
                    score += 15
                    reasons.append("⚡ Импульсный пробой EMA20 снизу вверх (+15)")

                if oi_valid:
                    score += 15
                    reasons.append(f"Приток OI зарегистрирован (+15)")

                if score >= 70:
                    signals.append({
                        "type": "QUICK_PUMP",
                        "header": "⚡ **BINGX: БЫСТРЫЙ ИМПУЛЬС (5-12%)**",
                        "score": score,
                        "reasons": reasons,
                        "price": close_5m,
                        "change_5m": round(change_5m, 2),
                        "rvol": round(spike_ratio, 1),
                        "support_level": support_level,
                        "resistance_level": resistance_level,
                        "support_touches": support_touches,
                        "resistance_touches": resistance_touches
                    })

    # -------------------------------------------------------------
    # 🚀 РЕЖИМ 2: ГЛОБАЛЬНАЯ РАКЕТА (+20%..50%)
    # -------------------------------------------------------------
    # 🔧 ОБНОВЛЕНО: max_allowed_width = 7.0% (для широких баз щиткоинов)
    is_heavy_flat, h_width, h_range_h, _, h_hours, h_avg_vol = find_base_params(
        klines_1h, min_hours=24, max_hours=72, max_allowed_width=7.0
    )

    if is_heavy_flat:
        curr_5m = klines_5m[-1]
        open_5m, close_5m = float(curr_5m[1]), float(curr_5m[4])
        change_5m = ((close_5m - open_5m) / open_5m) * 100

        if change_5m >= 0.9:
            vol_5m = float(curr_5m[5]) * close_5m
            projected_1h_vol = vol_5m * 12
            spike_ratio = projected_1h_vol / h_avg_vol if h_avg_vol > 0 else 1.0

            if spike_ratio >= 2.2:
                score = 60
                reasons = [f"🐘 Тяжелая база: {h_hours}ч (ширина {h_width}%) (+30)"]
                reasons.append(f"🔥 Взрыв объема базы x{round(spike_ratio, 1)} (+25)")

                if close_5m >= h_range_h:
                    score += 15
                    reasons.append(f"Пробой верхнего уровня базы ${h_range_h} (+15)")

                if resistance_level and close_5m > resistance_level:
                    score += 10
                    reasons.append(f"Пробой сопротивления ${resistance_level} (касаний: {resistance_touches}) (+10)")
                elif support_level:
                    reasons.append(f"📏 Ближайшая поддержка: ${support_level} (касаний: {support_touches})")

                if oi_valid:
                    score += 10
                    reasons.append("Занос крупного OI (+10)")

                if score >= 75:
                    signals.append({
                        "type": "HEAVY_ROCKET",
                        "header": "🚀 **BINGX: ГЛОБАЛЬНАЯ РАКЕТА (30-50%)**",
                        "score": score,
                        "reasons": reasons,
                        "price": close_5m,
                        "change_5m": round(change_5m, 2),
                        "rvol": round(spike_ratio, 1),
                        "support_level": support_level,
                        "resistance_level": resistance_level,
                        "support_touches": support_touches,
                        "resistance_touches": resistance_touches
                    })

    return signals


# =====================================================================
# 🔄 ГЛАВНЫЙ ЦИКЛ СКАНЕРА
# =====================================================================
async def scan_market():
    logging.info("🚀 Поисковый скрипт запущен и сканирует рынок BingX...")
    
    async with aiohttp.ClientSession() as session:
        await send_telegram_message("🤖 **Бот успешно запущен!**\nМониторинг BingX в двух режимах (Локальный и Ракета) с уровнями активен.")

        while True:
            try:
                symbols = await get_active_usdt_pairs(session)
                logging.info(f"Найдено {len(symbols)} активных пар BingX для анализа.")

                for symbol in symbols:
                    klines_1h = await get_klines(session, symbol, "1h", limit=80)
                    klines_5m = await get_klines(session, symbol, "5m", limit=30)

                    if not klines_1h or not klines_5m:
                        continue

                    oi_valid, oi_val = await get_open_interest_delta(session, symbol)

                    found_signals = evaluate_all_signals(klines_1h, klines_5m, oi_valid, oi_val)

                    current_time = time.time()
                    for sig in found_signals:
                        cache_key = f"{symbol}_{sig['type']}"
                        
                        last_time = SIGNAL_CACHE.get(cache_key, 0)
                        if current_time - last_time < COOLDOWN_SECONDS:
                            continue

                        SIGNAL_CACHE[cache_key] = current_time

                        clean_symbol = symbol.replace("-USDT", "")
                        reasons_fmt = "\n".join([f"• {r}" for r in sig["reasons"]])
                        
                        levels_info = ""
                        if sig.get("support_level"):
                            levels_info += f"\n📏 **Уровни:** Поддержка `${sig['support_level']}` ({sig['support_touches']} кас.)"
                        if sig.get("resistance_level"):
                            levels_info += f" | Сопротивление `${sig['resistance_level']}` ({sig['resistance_touches']} кас.)"
                        
                        msg = (
                            f"{sig['header']}\n\n"
                            f"📌 **Монета:** #{clean_symbol} / USDT\n"
                            f"💵 **Текущая цена:** `{sig['price']}`\n"
                            f"📊 **Импульс 5m:** `+{sig['change_5m']}%`\n"
                            f"📈 **Всплеск объёма (RVOL):** `x{sig['rvol']}`\n"
                            f"⭐ **Балл сетапа:** `{sig['score']}/100`{levels_info}\n\n"
                            f"🔍 **Факторы входа:**\n{reasons_fmt}\n\n"
                            f"🔗 [Открыть график BingX](https://bingx.com/ru-ru/futures/forward/{clean_symbol}USDT/)"
                        )

                        await send_telegram_message(msg)
                        logging.info(f"SENT SIGNAL: {symbol} [{sig['type']}] - Score: {sig['score']}")

                    await asyncio.sleep(0.15)

            except Exception as e:
                logging.error(f"Ошибка в главном цикле сканера: {e}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)


# =====================================================================
# 🌐 ВЕБ-СЕРВЕР ДЛЯ RENDER
# =====================================================================
async def health_check_handler(request):
    return web.Response(text="Bot is running 24/7", status=200)


async def self_ping_loop():
    await asyncio.sleep(30)
    if not RENDER_EXTERNAL_URL:
        logging.warning("RENDER_EXTERNAL_URL не задан. Авто-пинг отключен.")
        return

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(RENDER_EXTERNAL_URL) as resp:
                    logging.info(f"Self-ping выполнен, статус: {resp.status}")
            except Exception as e:
                logging.error(f"Ошибка self-ping: {e}")
            await asyncio.sleep(600)


async def main():
    app = web.Application()
    app.router.add_get("/", health_check_handler)
    app.router.add_get("/health", health_check_handler)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Веб-сервер запущен на порту {port}")

    asyncio.create_task(self_ping_loop())
    await scan_market()


if __name__ == "__main__":
    asyncio.run(main())
