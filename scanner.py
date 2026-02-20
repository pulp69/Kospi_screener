import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import requests
from pykrx import stock

MARKET = "KOSPI"
MULT = 2.0
LOOKBACK_AVG = 10
MA_WINDOW = 30
MA_SLOPE_DAYS = 10
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_CROSS_LOOKBACK = 3
USE_VALUE_FILTER = True

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def ema(s, span):
    return s.ewm(span=span, adjust=False).mean()

def compute_macd(close):
    macd_line = ema(close, MACD_FAST) - ema(close, MACD_SLOW)
    signal_line = ema(macd_line, MACD_SIGNAL)
    return macd_line, signal_line

def macd_cross_up_within(macd, signal, lookback=3):
    if len(macd) < lookback + 1:
        return False
    m = macd.values
    s = signal.values
    for i in range(lookback):
        if (m[-2-i] <= s[-2-i]) and (m[-1-i] > s[-1-i]):
            return True
    return False

def ma_slope(ma, days=10):
    ma = ma.dropna()
    if len(ma) < days:
        return np.nan
    y = ma.iloc[-days:].values
    x = np.arange(days)
    return np.polyfit(x, y, 1)[0]

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

def main():
    today = datetime.now() - timedelta(days=1)
    date = stock.get_nearest_business_day_in_a_week(today.strftime("%Y%m%d"))
    start = (today - timedelta(days=260)).strftime("%Y%m%d")

    tickers = stock.get_market_ticker_list(date, market=MARKET)

    result = []

    for t in tickers[:300]:
        try:
            df = stock.get_market_ohlcv_by_date(start, date, t)
            if len(df) < 200:
                continue

            close = df["종가"].astype(float)
            vol = df["거래량"].astype(float)

            if USE_VALUE_FILTER:
                liq = close * vol
            else:
                liq = vol

            ratio = liq.iloc[-1] / liq.iloc[-11:-1].mean()
            if ratio < MULT:
                continue

            ma60 = close.rolling(60).mean()
            slope = ma_slope(ma60, 10)
            if slope <= 0:
                continue

            macd, signal = compute_macd(close)
            if not macd_cross_up_within(macd.dropna(), signal.dropna(), 3):
                continue

            name = stock.get_market_ticker_name(t)
            result.append(f"{name}({t})")

        except:
            continue

    if result:
        msg = "KOSPI 조건검색 결과\n" + "\n".join(result[:20])
    else:
        msg = "조건 만족 종목 없음"

    send_telegram(msg)

if __name__ == "__main__":
    main()
