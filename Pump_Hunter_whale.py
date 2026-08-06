import datetime
import logging
import requests
import numpy as np

# -------------------------------------------------------------
# НАСТРОЙКИ И КОНСТАНТЫ
# -------------------------------------------------------------
MIN_24H_VOLUME_USDT = 2000000  # Фильтр: минимум 2 млн $ суточного объема
IDEAL_ENTRY_MINUTES = (49, 56)  # Окно для залива перед закрытием H1

# Исключаем только стейблкоины (золото XAUT/PAXG и фиатные пары остаются)
EXCLUDED_SYMBOLS = ["USDC", "FDUSD", "USDT"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -------------------------------------------------------------
def parse_kline(kline):
    """
    Разбор свечи BingX / Binance:
    [open_time, open, high, low, close, volume, ...]
    """
    o = float(kline[1])
    h = float(kline[2])
    l = float(kline[3])
    c = float(kline[4])
    v = float(kline[5])
    return o, h, l, c, v


def calculate_ema(prices, period):
    """Расчет экспоненциальной скользящей средней (EMA)"""
    if len(prices) < period:
        return prices[-1] if prices else 0.0
    weights = np.exp(np.linspace(-1.0, 0.0, period))
    weights /= weights.sum()
    return float(np.convolve(prices, weights, mode='valid')[-1])


def find_base_params(klines_1h, min_hours=4, max_hours=12, max_width_pct=4.5):
    """Проверка наличия накопительной базы (флэта)"""
    if len(klines_1h) < min_hours:
        return False, 0.0, 0.0, 0.0, 0, []

    # Исправлены индексы: High = [2], Low = [3], Close = [4]
    highs = [parse_kline(k)[2] for k in klines_1h]
    lows = [parse_kline(k)[3] for k in klines_1h]

    for h in range(max_hours, min_hours - 1, -1):
        if len(klines_1h) < h:
            continue
        sub_highs = highs[-h:]
        sub_lows = lows[-h:]
        max_h = max(sub_highs)
        min_l = min(sub_lows)

        if min_l <= 0:
            continue

        width = ((max_h - min_l) / min_l) * 100
        if width <= max_width_pct:
            return True, round(width, 2), max_h, min_l, h, klines_1h[-h:]

    return False, 0.0, 0.0, 0.0, 0, []


def check_ema_bounce(klines_1h, range_h, range_l):
    """Проверка поджатия и отскока от EMA20/40"""
    closes = [parse_kline(k)[4] for k in klines_1h]
    if len(closes) < 40:
        return None, 0, ""

    ema20 = calculate_ema(closes, 20)
    ema40 = calculate_ema(closes, 40)
    last_close = closes[-1]

    if range_l <= ema20 <= range_h:
        return "EMA20", 15, "🧲 Накопление прямо на линии EMA20"
    elif range_l <= ema40 <= range_h:
        return "EMA40", 15, "🧲 Накопление прямо на линии EMA40"
    elif last_close >= ema20:
        return "ABOVE_EMA20", 10, "📈 Закрепление выше EMA20"

    return None, 0, ""


# -------------------------------------------------------------
# ОСНОВНОЙ АЛГОРИТМ ОЦЕНКИ СИГНАЛА
# -------------------------------------------------------------
def evaluate_realtime_signal(symbol, klines_1h, volume_24h_usdt=0.0):
    # 0. Игнорируем стейблкоины (золото и валюты остаются)
    symbol_upper = symbol.upper()
    if any(ex in symbol_upper for ex in EXCLUDED_SYMBOLS):
        return None

    # Проверка ликвидности монеты
    if volume_24h_usdt < MIN_24H_VOLUME_USDT:
        return None

    if len(klines_1h) < 24:
        return None

    curr = klines_1h[-1]
    open_h, high_h, low_h, close_h, vol_raw = parse_kline(curr)
    vol_h = vol_raw * close_h

    if open_h <= 0 or low_h <= 0:
        return None

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    elapsed = now_dt.minute + now_dt.second / 60
    if elapsed < 1:
        elapsed = 1

    change_pct = ((close_h - open_h) / open_h) * 100
    candle_range = ((high_h - low_h) / low_h) * 100

    past_vols = [parse_kline(k)[5] * parse_kline(k)[4] for k in klines_1h[-21:-1]]
    avg_vol = np.mean(past_vols) if past_vols else 1.0
    projected_vol = vol_h * (60 / elapsed)
    rvol = projected_vol / avg_vol if avg_vol > 0 else 1.0

    closes = [parse_kline(k)[4] for k in klines_1h]
    ema20_val = calculate_ema(closes, 20)

    # 1. Поиск базы (узкого флэта)
    is_flat, width, range_h, range_l, hours, _ = find_base_params(klines_1h, 4, 12, 4.5)
    if not is_flat:
        is_flat, width, range_h, range_l, hours, _ = find_base_params(klines_1h, 12, 48, 6.0)

    ema_type, ema_score, ema_detail = check_ema_bounce(klines_1h, range_h, range_l) if is_flat else (None, 0, "")
    ema_detail = ema_detail if ema_type else ""

    # -------------------------------------------------------------
    # 2. СИГНАЛ: Скрытая мышь (Истинная аккумуляция)
    # -------------------------------------------------------------
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

        # Фильтр выноса стопов (проверка длинных фитилей на прошлых свечах)
        prev1_o, prev1_h, prev1_l, prev1_c, _ = parse_kline(klines_1h[-2])
        prev2_o, prev2_h, prev2_l, prev2_c, _ = parse_kline(klines_1h[-3])

        prev1_upper = ((prev1_h - max(prev1_o, prev1_c)) / prev1_c) * 100 if prev1_c > 0 else 0
        prev1_lower = ((min(prev1_o, prev1_c) - prev1_l) / prev1_l) * 100 if prev1_l > 0 else 0
        prev2_upper = ((prev2_h - max(prev2_o, prev2_c)) / prev2_c) * 100 if prev2_c > 0 else 0

        no_manipulation = (prev1_upper <= 1.0 and prev1_lower <= 1.0 and prev2_upper <= 1.0)

        # Окно подтверждения объема (только с 3 по 5 минуту)
        is_valid_window = (3 <= int(elapsed) <= 5)

        # Комплексная проверка:
        if (quiet_history and 
            candle_range <= 1.5 and 
            body_pct <= 0.6 and 
            change_pct >= -0.05 and        # Не слать на отрицательных/падающих свечах
            rvol >= 1.6 and 
            close_h >= ema20_val and       # Строго над EMA20
            no_manipulation and            # Без выносов стопов
            is_valid_window):              # Подтвержденный объем на 3-5 минуте

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
                    f"🐭 **Затаившаяся мышь:** Набор позиции ({int(elapsed)}-я мин, RVOL подтвержден)",
                    f"📊 **Поток денег:** {stable_count}/7 свечей без рывков",
                    f"🕯 **Анатомия H1:** Диапазон `{candle_range:.2f}%` | RVOL `x{round(rvol,1)}`"
                ]
            }

    # -------------------------------------------------------------
    # 3. СИГНАЛ: Первая импульсная свеча (Выход из базы)
    # -------------------------------------------------------------
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

    # -------------------------------------------------------------
    # 4. СИГНАЛ: Залив на последних минутах (49-56 мин)
    # -------------------------------------------------------------
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


# -------------------------------------------------------------
# ФОРМАТИРОВАНИЕ СООБЩЕНИЯ ДЛЯ TELEGRAM
# -------------------------------------------------------------
def format_telegram_message(symbol, signal):
    """Форматирование сообщения без хэштега и с корректным выводом цен (.8f)"""
    price_str = f"{signal['price']:.8f}".rstrip('0').rstrip('.')
    clean_symbol = symbol.replace('USDT', '').replace('#', '').strip()
    
    msg = (
        f"🚨 🔍 **СКРЫТЫЙ АЛГОРИТМИЧЕСКИЙ ЗАКУП**\n\n"
        f"📌 Монета: {clean_symbol} / USDT\n"
        f"💵 Текущая цена: `{price_str}`\n"
        f"📊 Изменение H1: `{signal['change_1h']}%`\n"
        f"📈 Проекция RVOL: `x{signal['rvol']}`\n"
        f"⏰ Минута часа: {signal['elapsed']} из 60\n"
        f"📦 База: {signal['hours']}ч (ширина {signal['width']}%)\n"
        f"⭐ Скор: {signal['score']}/100\n\n"
        f"🔍 **Анализ паттерна:**\n"
    )
    for reason in signal['reasons']:
        msg += f"• {reason}\n"
        
    return msg
