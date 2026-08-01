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
MIN_SCORE_THRESHOLD = 60         # Подняли порог, чтобы убрать слабые сигналы
MIN_CANDLE_IMPULSE_PCT = 0.3
MAX_ACCUMULATION_RANGE_PCT = 12.0
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
    """Очистка устаревших данных из памяти для предотвращения утечек ОЗУ"""
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


def check_dynamic_accumulation(klines_1h):
    windows = [3, 4, 6, 8, 12]
    for w in windows:
        if len(klines_1h) < w + 1:
            continue
        recent = klines_1h[-(w + 1):-1]
        highs = [float(k[2]) for k in recent]
        lows = [float(k[3]) for k in recent]
        
        range_high = max(highs)
        range_low = min(lows)
        if range_low <= 0:
            continue
            
        flat_width = ((range_high - range_low) / range_low) * 100
        
        if flat_width <= MAX_ACCUMULATION_RANGE_PCT:
            return True, round(flat_width, 2), range_high, range_low
            
    return False, 0.0, 0.0, 0.0


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
# 3. SCORE-МОДУЛЬ (С УМНОЙ EMA, ТАЙМИНГОМ И ФИЛЬТРОМ ИМПУЛЬСА)
# ==========================
def evaluate_accumulation_signal(klines_1h, klines_5m, is_oi_valid, oi_delta_pct):
    if len(klines_5m) < 60 or len(klines_1h) < 9:
        return None

    curr_5m = klines_5m[-1]
    prev_5m = klines_5m[-2]
    prev2_5m = klines_5m[-3]

    open_5m, high_5m, low_5m, close_5m = float(curr_5m[1]), float(curr_5m[2]), float(curr_5m[3]), float(curr_5m[4])
    candle_change_pct = ((close_5m - open_5m) / open_5m) * 100

    # 1. Проверка минимального импульса свечи
    if candle_change_pct < MIN_CANDLE_IMPULSE_PCT:
        return None

    # 2. ЖЕСТКИЙ ФИЛЬТР ОБЪЁМА (RVOL >= 1.2)
    prev_20_5m = klines_5m[-21:-1]
    avg_vol_20 = np.mean([float(k[5]) * float(k[4]) for k in prev_20_5m])
    vol_5m = float(curr_5m[5]) * close_5m
    rvol = vol_5m / avg_vol_20 if avg_vol_20 > 0 else 1.0

    if rvol < 1.2:
        return None  # Отсекаем флэты без объема (ложные движения)

    # 3. Проверка на наличие базы (боковика)
    is_flat, flat_width, range_high_1h, range_low_1h = check_dynamic_accumulation(klines_1h)
    if not is_flat:
        return None

    vol_p1 = float(prev_5m[5]) * float(prev_5m[4])
    vol_p2 = float(prev2_5m[5]) * float(prev2_5m[4])

    candle_range = high_5m - low_5m
    closes_5m_all = [float(k[4]) for k in klines_5m]

    ema20 = calculate_ema_pandas(closes_5m_all, 20)
    ema40 = calculate_ema_pandas(closes_5m_all, 40)

    # 🛑 4. ЖЕСТКАЯ ЗАЩИТА EMA ОТ ПАДЕНИЙ (TLM-дамп)
    # Если цена ПОД оранжевой EMA20 — монета катится вниз, ЛОНГ ЗАПРЕЩЕН!
    if close_5m < ema20:
        return None

    score = 0
    reasons = []

    # Фактор 1: Позиция относительно границы базы
    distance_to_high = ((range_high_1h - close_5m) / close_5m) * 100
    if close_5m >= range_high_1h:
        score += 25
        reasons.append("Пробой 1H флэта (+25)")
    elif 0 <= distance_to_high <= 0.8:
        score += 20
        reasons.append(f"Поджим к уровню {round(distance_to_high, 2)}% (+20)")

    # Фактор 2: Нарастание волны объема
    rvol_p2 = vol_p2 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    rvol_p1 = vol_p1 / avg_vol_20 if avg_vol_20 > 0 else 1.0
    
    if rvol > rvol_p1 > rvol_p2 and rvol >= 1.3:
        score += 15
        reasons.append(f"Волна объема: {round(rvol_p2,1)} -> {round(rvol_p1,1)} -> {round(rvol,1)} (+15)")
    elif rvol >= 1.5:
        score += 10
        reasons.append(f"Всплеск RVOL x{round(rvol,1)} (+10)")

    # Фактор 3: Открытый интерес
    if is_oi_valid:
        score += 15
        reasons.append(f"Рост OI (+15, {oi_delta_pct}%)")

    # Фактор 4: ПРАВИЛЬНЫЙ АНАЛИЗ EMA
    ema_distance_pct = ((close_5m - ema20) / ema20) * 100
    if ema20 > ema40 and close_5m > ema20:
        if ema_distance_pct <= 3.0:
            score += 15
            reasons.append(f"Идеальное поджатие к EMA20 (+{round(ema_distance_pct, 1)}%) (+15)")
        else:
            score += 5
            reasons.append(f"EMA20 > EMA40, но цена оторвалась (+{round(ema_distance_pct, 1)}%) (+5)")

    # Фактор 5: Ускорение шага цены
    price_delta_now = close_5m - float(prev_5m[4])
    price_delta_prev = float(prev_5m[4]) - float(prev2_5m[4])
    if price_delta_now > price_delta_prev > 0:
        score += 10
        reasons.append("Ускорение шага цены (+10)")

    # Фактор 6: Закрытие под хай свечи
    if candle_range > 0 and close_5m >= (high_5m - (candle_range * 0.25)):
        score += 10
        reasons.append("Закрытие под High (+10)")

    # Фактор 7: Качество накопительной базы
    if flat_width <= 3.5:
        score += 15
        reasons.append(f"Очень узкая база {flat_width}% (+15)")
    elif 3.5 < flat_width <= 7.0:
        score += 10
        reasons.append(f"Умеренная база {flat_width}% (+10)")
    elif 7.0 < flat_width <= MAX_ACCUMULATION_RANGE_PCT:
        score += 5
        reasons.append(f"Широкая база {flat_width}% (+5)")

    # Фактор 8: Часовой загон (53 - 58 минута часа)
    now_minute = datetime.datetime.now().minute
    if 53 <= now_minute <= 58:
        score += 10
        reasons.append("⏰ Часовой загон (53-58 мин) (+10)")

    # Определяем тип алерта по величине импульса
    if candle_change_pct > 5.0:
        header_title = "ℹ️ ИНФОРМАЦИОННЫЙ АЛЕРТ (Памп улетел > +5%)"
    else:
        header_title = "🎯 BINGX: РАННИЙ СИГНАЛ"

    return {
        "score": score,
        "reasons": reasons,
        "price": close_5m,
        "rvol": round(rvol, 1),
        "flat_width": flat_width,
        "change_5m": round(candle_change_pct, 2),
        "header_title": header_title
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

            klines_1h = await exchange.fetch_ohlcv(symbol, timeframe='1h', limit=15)
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

                # ✂️ Чистый тикер для BingX (из "ESPORTSUSDT:USDT" или "ESPORTSUSDT" делаем "ESPORTS")
                raw_ticker = symbol.split(':')[0].replace('/', '')
                clean_ticker = raw_ticker.upper().replace("USDT", "")

                reasons_str = "\n• " + "\n• ".join(signal['reasons'])
                
                msg = (
                    f"{signal['header_title']} **(Score: {signal['score']}/100)**\n"
                    f"📌 **Монета:** `{clean_ticker}`\n"
                    f"💵 **Цена:** `${signal['price']}`\n"
                    f"📊 **Импульс 5m:** `{signal['change_5m']}%`\n"
                    f"🔥 **RVOL:** `x{signal['rvol']}` | **База:** `{signal['flat_width']}%`\n\n"
                    f"🧠 **Факторы входа:**{reasons_str}"
                )
                logging.info(f"Сигнал по BingX [{clean_ticker}]! Score: {signal['score']}")
                await send_telegram_msg_with_retry(msg)

        except Exception as e:
            logging.debug(f"Process error on {symbol}: {e}")


async def scanner_loop():
    logging.info("🚀 Сканер пампа запущен на BingX Futures...")
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
