import asyncio
import datetime
import logging
import os
import time
import numpy as np
import pandas as pd
import ccxt.async_support as ccxt
from aiohttp import web
import aiohttp
from telegram import Bot
from telegram.error import TelegramError

# ==========================
# 1. НАСТРОЙКИ И КОНФИГУРАЦИЯ
# ==========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("❌ Не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID в переменных окружения")

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip()

# Фильтры монет
MIN_PRICE = 0.000001
MAX_PRICE = 10.0
MIN_24H_VOLUME_USDT = 50000

# Настройки скоринга и сетевой защиты
MIN_SCORE_THRESHOLD = 60         
MIN_CANDLE_IMPULSE_PCT = 0.3
MAX_1H_ACCUMULATION_RANGE_PCT = 8.0  # База на 1H не шире 8%
SIGNAL_COOLDOWN = 1800
MAX_CONCURRENT_REQUESTS = 15

# Инициализация биржи BingX Futures (Swap)
exchange = ccxt.bingx({
    'enableRateLimit': True,
    'options': {
        'defaultType': 'swap',
    }
})

bot = Bot(token=TELEGRAM_BOT_TOKEN)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Хранилища состояния
last_signals = {}
oi_history = {}
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


# ==========================
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==========================
async def send_telegram_msg_with_retry(text, parse_mode="Markdown", retries=3):
    """Надежная отправка сообщений в Telegram с повторными попытками"""
    for attempt in range(retries):
        try:
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=parse_mode)
            return
        except TelegramError as e:
            logging.warning(f"Ошибка отправки Telegram (попытка {attempt+1}/{retries}): {e}")
            await asyncio.sleep(2 * (attempt + 1))
    logging.error("Не удалось отправить сообщение в Telegram после всех попыток.")


def cleanup_old_data():
    """Очистка устаревших данных из памяти"""
    now = time.time()
    expired_signals = [k for k, v in last_signals.items() if now - v > 86400]
    for k in expired_signals:
        del last_signals[k]
        
    expired_oi = [k for k, v in oi_history.items() if len(v) > 0 and (now - v[-1][0]/1000) > 86400]
    for k in expired_oi:
        del oi_history[k]


def calculate_ema_pandas(prices, period):
    if len(prices) < period:
        return prices[-1]
    series = pd.Series(prices)
    ema = series.ewm(span=period, adjust=False).mean()
    return float(ema.iloc[-1])


def check_dynamic_accumulation_1h(klines_1h):
    """
    Ищет НАСТОЯЩИЙ флэт на ЧАСОВОМ таймфрейме (от 4 до 12 часов).
    Исключает случаи, когда 1 маленькая часовая свеча принимается за базу.
    """
    windows = [4, 6, 8, 12]
    for w in windows:
        if len(klines_1h) < w + 1:
            continue
            
        recent_1h = klines_1h[-(w + 1):-1]
        highs = [float(k[2]) for k in recent_1h]
        lows = [float(k[3]) for k in recent_1h]
        
        range_high = max(highs)
        range_low = min(lows)
        
        if range_low <= 0:
            continue
            
        flat_width = ((range_high - range_low) / range_low) * 100
        
        if flat_width <= MAX_1H_ACCUMULATION_RANGE_PCT:
            return True, round(flat_width, 2), range_high, range_low, w
            
    return False, 0.0, 0.0, 0.0, 0


def update_and_check_oi(symbol, curr_oi, last_candle_time):
    if curr_oi == 0:
        return False, 0.0

    if symbol not in oi_history:
        oi_history[symbol] = []
    
    history = oi_history[symbol]
    
    if not history or history[-1][0] != last_candle_time:
        history.append((last_candle_time, curr_oi))
        if len(history) > 4:
            history.pop(0)
            
    if len(history) >= 2:
        prev_oi = history[-2][1]
        oi_delta_pct = ((curr_oi - prev_oi) / prev_oi) * 100 if prev_oi > 0 else 0
        
        is_3_growth = False
        if len(history) >= 3:
            is_3_growth = (history[-1][1] > history[-2][1] > history[-3][1])
            
        if oi_delta_pct >= 0.15 or is_3_growth:
            return True, round(oi_delta_pct, 2)
            
    return False, 0.0


# ==========================
# 3. SCORE-МОДУЛЬ (С ФИЛЬТРАМИ ТРЕНДА И 1H-БАЗЫ)
# ==========================
def evaluate_accumulation_signal(klines_1h, klines_5m, is_oi_valid, oi_delta_pct):
    if len(klines_5m) < 60 or len(klines_1h) < 15:
        return None

    # --- 1. ПРОВЕРКА ЧАСОВОЙ СТРУКТУРЫ И ФЛЭТА ---
    is_flat_1h, flat_width_1h, range_high_1h, range_low_1h, hours_cnt = check_dynamic_accumulation_1h(klines_1h)
    if not is_flat_1h:
        return None  # Отсекаем, если нет 4-12 часовой полки

    closes_1h_all = [float(k[4]) for k in klines_1h]
    ema20_1h = calculate_ema_pandas(closes_1h_all, 20)

    curr_1h = klines_1h[-1]
    prev_1h = klines_1h[-2]
    open_1h, close_1h_curr = float(curr_1h[1]), float(curr_1h[4])
    curr_1h_growth_pct = ((close_1h_curr - open_1h) / open_1h) * 100

    # --- 2. 5-МИНУТНЫЙ ТРИГГЕР (ВХОД) ---
    curr_5m = klines_5m[-1]
    prev_5m = klines_5m[-2]

    open_5m, high_5m, low_5m, close_5m = float(curr_5m[1]), float(curr_5m[2]), float(curr_5m[3]), float(curr_5m[4])
    candle_change_pct = ((close_5m - open_5m) / open_5m) * 100

    if candle_change_pct < MIN_CANDLE_IMPULSE_PCT:
        return None

    # 🛑 ФИЛЬТР 1: ЗАЩИТА ОТ ДАУНТРЕНДА
    # Если цена ниже часовой EMA20 — монета падает, лонги запрещены!
    if close_5m < ema20_1h:
        return None

    # 🛑 ФИЛЬТР 2: ЗАЩИТА ОТ ОТСКОКОВ ПОСЛЕ СЛИВА (Как на DOGE)
    min_3h_low = min([float(k[3]) for k in klines_5m[-36:]])
    if (close_5m - min_3h_low) / min_3h_low * 100 > 1.5 and close_5m < range_high_1h:
        return None  # Это просто дохлый отскок со дна, а не пробой базы

    # 🛑 ФИЛЬТР 3: ЗАЩИТА ОТ ВХОДА НА ОБРАТНОМ ОТКАТЕ (Как на ADA)
    recent_3h_high = max([float(k[2]) for k in klines_5m[-36:]])
    dist_to_recent_high_pct = ((recent_3h_high - close_5m) / close_5m) * 100
    if dist_to_recent_high_pct > 0.8:
        return None  # Монета уже откатывается от вершинки

    # --- 3. РАСЧЕТ RVOL И EMA (5m) ---
    prev_20_5m = klines_5m[-21:-1]
    avg_vol_20 = np.mean([float(k[5]) * float(k[4]) for k in prev_20_5m])
    vol_5m = float(curr_5m[5]) * close_5m
    rvol = vol_5m / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if rvol < 1.2:
        return None

    closes_5m_all = [float(k[4]) for k in klines_5m]
    ema20_5m = calculate_ema_pandas(closes_5m_all, 20)
    ema40_5m = calculate_ema_pandas(closes_5m_all, 40)

    if close_5m < ema20_5m:
        return None

    # --- 4. ПОДСЧЕТ БАЛЛОВ (SCORES) ---
    score = 0
    reasons = []

    growth_from_1h_base = ((close_5m - range_high_1h) / range_high_1h) * 100 if range_high_1h > 0 else 0
    ema_distance_pct = ((close_5m - ema20_5m) / ema20_5m) * 100

    # Фактор 1: Часовая сжатость перед пробоем
    prev_1h_range_pct = ((float(prev_1h[2]) - float(prev_1h[3])) / float(prev_1h[4])) * 100
    if prev_1h_range_pct <= 2.5:
        score += 20
        reasons.append(f"Узкая 1H свеча перед поджимом ({round(prev_1h_range_pct, 1)}%) (+20)")

    # Фактор 2: Поджим / Пробой часовой базы
    if close_5m >= range_high_1h:
        score += 20
        reasons.append(f"Пробой 1H флэта ({hours_cnt}ч) (+20)")
    elif (range_high_1h - close_5m) / close_5m * 100 <= 0.5:
        score += 15
        reasons.append(f"Поджим к границе 1H флэта ({hours_cnt}ч) (+15)")

    # Фактор 3: Объёмы
    vol_p1 = float(prev_5m[5]) * float(prev_5m[4])
    vol_p2 = float(klines_5m[-3][5]) * float(klines_5m[-3][4])
    rvol_p1 = vol_p1 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    rvol_p2 = vol_p2 / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if rvol > rvol_p1 > rvol_p2 and rvol >= 1.3:
        score += 15
        reasons.append(f"Волна объема: {round(rvol_p2,1)} -> {round(rvol_p1,1)} -> {round(rvol,1)} (+15)")
    elif rvol >= 1.5:
        score += 10
        reasons.append(f"Всплеск RVOL x{round(rvol,1)} (+10)")

    # Фактор 4: Открытый интерес
    if is_oi_valid:
        score += 15
        reasons.append(f"Рост OI (+15, {oi_delta_pct}%)")

    # Фактор 5: Положение относительно 5M EMA20
    if ema20_5m > ema40_5m and close_5m > ema20_5m:
        if ema_distance_pct <= 2.5:
            score += 15
            reasons.append(f"Идеальное поджатие к EMA20 (+{round(ema_distance_pct, 1)}%) (+15)")

    # Фактор 6: Часовой загон
    now_minute = datetime.datetime.now().minute
    if 53 <= now_minute <= 58:
        score += 10
        reasons.append("⏰ Часовой загон (53-58 мин) (+10)")

    # 🚨 РАРЗДЕЛЕНИЕ: ТОЧНЫЙ ВХОД VS ОПАСНО (ВЫЛЕТ)
    is_dangerous = False
    danger_reasons = []

    if growth_from_1h_base > 3.5:
        is_dangerous = True
        danger_reasons.append(f"Вылет от границы 1H базы: +{round(growth_from_1h_base, 1)}%")

    if curr_1h_growth_pct > 3.5:
        is_dangerous = True
        danger_reasons.append(f"Часовая свеча уже выросла на +{round(curr_1h_growth_pct, 1)}%")

    if ema_distance_pct > 4.0:
        is_dangerous = True
        danger_reasons.append(f"Отрыв от EMA20 (5m): +{round(ema_distance_pct, 1)}%")

    if is_dangerous:
        header_title = "⚠️ **BINGX: СИГНАЛ (ОПАСНО: ВЫЛЕТ > 3.5%)**"
        danger_note = f"\n\n🛑 **Риск входа на верхушке:**\n• " + "\n• ".join(danger_reasons)
    else:
        header_title = "🎯 **BINGX: РАННИЙ СИГНАЛ (ТОЧНЫЙ ВХОД)**"
        danger_note = ""

    return {
        "score": score,
        "reasons": reasons,
        "price": close_5m,
        "rvol": round(rvol, 1),
        "flat_width": flat_width_1h,
        "change_5m": round(candle_change_pct, 2),
        "header_title": header_title,
        "danger_note": danger_note
    }


# ==========================
# 4. АНАЛИЗАТОР (BINGX)
# ==========================
async def check_orderbook_density(symbol):
    try:
        orderbook = await exchange.fetch_order_book(symbol, limit=20)
        bids_vol = sum([b[1] for b in orderbook['bids']])
        asks_vol = sum([a[1] for a in orderbook['asks']])
        if asks_vol > 0 and (bids_vol / asks_vol) >= 1.4:
            return True, round(bids_vol / asks_vol, 2)
    except Exception as e:
        logging.debug(f"Orderbook error on {symbol}: {e}")
    return False, 0.0


async def process_symbol(symbol, ticker):
    async with semaphore:
        try:
            now = time.time()
            
            if symbol in last_signals and (now - last_signals[symbol]) < SIGNAL_COOLDOWN:
                return

            klines_1h = await exchange.fetch_ohlcv(symbol, timeframe='1h', limit=20)
            klines_5m = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=60)

            if not klines_5m or not klines_1h:
                return

            last_candle_time = klines_5m[-1][0]

            curr_oi = 0.0
            try:
                oi_data = await exchange.fetch_open_interest(symbol)
                curr_oi = float(oi_data.get('openInterestAmount', 0) or oi_data.get('openInterestValue', 0))
            except Exception as e:
                logging.debug(f"OI error on {symbol}: {e}")

            is_oi_valid, oi_delta_pct = update_and_check_oi(symbol, curr_oi, last_candle_time)

            signal = evaluate_accumulation_signal(klines_1h, klines_5m, is_oi_valid, oi_delta_pct)
            if not signal or signal['score'] < MIN_SCORE_THRESHOLD:
                return

            # Funding Rate
            funding_rate = 0.0
            try:
                funding_info = await exchange.fetch_funding_rate(symbol)
                funding_rate = float(funding_info.get('fundingRate', 0.0))
            except Exception as e:
                logging.debug(f"Funding error on {symbol}: {e}")

            if funding_rate <= 0:
                signal['score'] += 10
                signal['reasons'].append(f"Отрицательный/Нулевой Funding ({round(funding_rate*100, 3)}%) (+10)")

            # Orderbook
            is_orderbook_bullish, ratio = await check_orderbook_density(symbol)
            if is_orderbook_bullish:
                signal['score'] += 10
                signal['reasons'].append(f"Стакан: Преобладание бидов x{ratio} (+10)")

            if signal['score'] >= MIN_SCORE_THRESHOLD:
                last_signals[symbol] = now

                raw_ticker = symbol.split(':')[0].replace('/', '')
                clean_ticker = raw_ticker.upper().replace("USDT", "")

                reasons_str = "\n• " + "\n• ".join(signal['reasons'])
                
                msg = (
                    f"{signal['header_title']} **(Score: {signal['score']}/100)**\n"
                    f"📌 **Монета:** `{clean_ticker}`\n"
                    f"💵 **Цена:** `${signal['price']}`\n"
                    f"📊 **Импульс 5m:** `{signal['change_5m']}%`\n"
                    f"🔥 **RVOL:** `x{signal['rvol']}` | **База 1H:** `{signal['flat_width']}%`\n\n"
                    f"🧠 **Факторы входа:**{reasons_str}"
                    f"{signal['danger_note']}"
                )
                logging.info(f"Сигнал по BingX [{clean_ticker}]! Score: {signal['score']}")
                await send_telegram_msg_with_retry(msg)

        except Exception as e:
            logging.debug(f"Process error on {symbol}: {e}")


async def scanner_loop():
    logging.info("🚀 Сканер запущен (1H Накопление + 5M Триггеры)...")
    markets = await exchange.load_markets()

    valid_market_symbols = {
        m_code for m_code, m in markets.items() 
        if m.get('swap', False) and m.get('linear', False) and m.get('active', True) and m.get('quote') == 'USDT'
    }

    while True:
        try:
            cleanup_old_data()
            
            tickers = await exchange.fetch_tickers()
            valid_symbols = []

            for symbol, ticker in tickers.items():
                if symbol in valid_market_symbols:
                    price = ticker.get('last', 0)
                    vol_usdt = ticker.get('quoteVolume', 0)
                    
                    if price and MIN_PRICE <= price <= MAX_PRICE and vol_usdt and vol_usdt >= MIN_24H_VOLUME_USDT:
                        valid_symbols.append((symbol, ticker))

            if valid_symbols:
                logging.info(f"Сканирую {len(valid_symbols)} активных монет...")
                BATCH_SIZE = 50
                for i in range(0, len(valid_symbols), BATCH_SIZE):
                    batch = valid_symbols[i:i + BATCH_SIZE]
                    tasks = [process_symbol(sym, tick) for sym, tick in batch]
                    await asyncio.gather(*tasks)

        except Exception as e:
            logging.error(f"Ошибка главного цикла: {e}")

        await asyncio.sleep(5)


# ==========================
# 5. ЗАГЛУШКА И SELF-PING ДЛЯ RENDER
# ==========================
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


async def safe_scanner_runner():
    while True:
        try:
            await scanner_loop()
        except Exception as e:
            logging.exception(f"💥 Ошибка сканера! Перезапуск через 10 сек: {e}")
            await asyncio.sleep(10)


async def main():
    app = web.Application()
    app.router.add_get("/", health_check_handler)
    app.router.add_get("/health", health_check_handler)

    port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Заглушка Web-сервера запущена на порту {port}")

    asyncio.create_task(safe_scanner_runner())
    asyncio.create_task(self_ping_loop())

    try:
        await asyncio.Event().wait()
    finally:
        logging.info("Закрытие сессии с биржей BingX...")
        await exchange.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Сканер остановлен пользователем.")
