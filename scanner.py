import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from pykrx import stock

# =========================
# 설정
# =========================
MARKETS = ["KOSPI", "KOSDAQ"]

TOP_N_VALUE = 300
USE_VALUE_FILTER = True
MULT = 2.0
LOOKBACK_AVG = 10

MA_WINDOW = 30
MA_SLOPE_DAYS = 10

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
MACD_CROSS_LOOKBACK = 3

MAX_RESULTS_SEND = 25

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

KST = ZoneInfo("Asia/Seoul")
CUTOFF_HOUR = 15
CUTOFF_MIN = 30


# =========================
# 유틸
# =========================
def yyyymmdd(dt):
    return dt.strftime("%Y%m%d")

def nearest_prev_business_day(date_str):
    try:
        return stock.get_nearest_business_day_in_a_week(date_str, prev=True)
    except TypeError:
        return stock.get_nearest_business_day_in_a_week(date_str)

def ema(s, span):
    return s.ewm(span=span, adjust=False).mean()

def compute_macd(close):
    macd_line = ema(close, MACD_FAST) - ema(close, MACD_SLOW)
    signal_line = ema(macd_line, MACD_SIGNAL)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def macd_cross_up_within(macd, signal, lookback):
    macd = macd.dropna()
    signal = signal.dropna()
    if len(macd) < lookback + 1:
        return False
    m = macd.values
    s = signal.values
    for i in range(lookback):
        if (m[-2 - i] <= s[-2 - i]) and (m[-1 - i] > s[-1 - i]):
            return True
    return False

def ma_slope_positive(ma, days):
    ma = ma.dropna()
    if len(ma) < days:
        return np.nan
    y = ma.iloc[-days:].values
    x = np.arange(days)
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)

def telegram_send(msg):
    if BOT_TOKEN and CHAT_ID:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})


def decide_target_date_kst():
    now_kst = datetime.now(KST)
    cutoff = now_kst.replace(hour=CUTOFF_HOUR, minute=CUTOFF_MIN, second=0, microsecond=0)

    if now_kst >= cutoff:
        base_dt = now_kst
        mode = "당일 기준(15:30 이후)"
    else:
        base_dt = now_kst - timedelta(days=1)
        mode = "전일 기준(15:30 이전)"

    target_date = nearest_prev_business_day(yyyymmdd(base_dt))
    return target_date, mode, now_kst


# =========================
# 메인
# =========================
def main():
    print("===== 조건검색 시작 =====")

    target_date, mode, now_kst = decide_target_date_kst()

    print(f"현재시각(KST): {now_kst}")
    print(f"기준선택: {mode}")
    print(f"기준일: {target_date}")

    start_dt = datetime.strptime(target_date, "%Y%m%d") - timedelta(days=260)
    start_date = nearest_prev_business_day(yyyymmdd(start_dt))

    # 🔥 (ticker, market) 형태로 저장
    tickers = []

    for m in MARKETS:
        df = stock.get_market_ohlcv_by_ticker(target_date, market=m)
        df["거래대금"] = df["종가"] * df["거래량"]
        top = df.sort_values("거래대금", ascending=False).head(TOP_N_VALUE)
        for t in top.index:
            tickers.append((t, m))

    print("스캔 대상 종목 수:", len(tickers))

    rows = []

    for t, market in tickers:
        try:
            df = stock.get_market_ohlcv_by_date(start_date, target_date, t)
            if len(df) < 120:
                continue

            close = df["종가"].astype(float)
            vol = df["거래량"].astype(float)

            liq = close * vol if USE_VALUE_FILTER else vol
            avg_prev = liq.iloc[-(LOOKBACK_AVG+1):-1].mean()
            if avg_prev <= 0:
                continue

            ratio = liq.iloc[-1] / avg_prev
            if ratio < MULT:
                continue

            ma30 = close.rolling(MA_WINDOW).mean()
            slope = ma_slope_positive(ma30, MA_SLOPE_DAYS)
            if slope <= 0:
                continue

            macd, signal, _ = compute_macd(close)
            if not macd_cross_up_within(macd, signal, MACD_CROSS_LOOKBACK):
                continue

            rows.append({
                "Market": market,  # 🔥 시장 추가
                "Name": stock.get_market_ticker_name(t),
                "Ticker": t,
                "Close": close.iloc[-1],
                "Ratio": ratio,
                "Slope": slope
            })

        except:
            continue

    print("----------------------------------")
    print("조건 만족 종목 수:", len(rows))

    if not rows:
        print("조건 만족 종목 없음")
        telegram_send("조건 만족 종목 없음")
        return

    result = pd.DataFrame(rows)
    result = result.sort_values(["Ratio", "Slope"], ascending=False)

    print("\n===== 상위 결과 =====")
    print(result.head(MAX_RESULTS_SEND))

    # 🔥 텔레그램 메시지에 시장 표시
    msg = "[조건검색 결과]\n"
    for i, r in result.head(MAX_RESULTS_SEND).iterrows():
        msg += f"{r['Market']} | {r['Name']}({r['Ticker']}) Ratio:{r['Ratio']:.2f}\n"

    telegram_send(msg)


if __name__ == "__main__":
    main()
