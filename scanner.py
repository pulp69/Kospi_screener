import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from pykrx import stock

KST = ZoneInfo("Asia/Seoul")


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def log(msg: str):
    print(msg, flush=True)


def retry_krx(func, *args, retries=4, delay=2, **kwargs):
    """
    pykrx/KRX 호출용 재시도 래퍼
    """
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            log(f"[WARN] KRX 호출 실패 {attempt}/{retries}: {func.__name__}{args} -> {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_err


def safe_get_index_ohlcv_by_date(fromdate: str, todate: str, ticker: str = "1001") -> pd.DataFrame:
    """
    KOSPI index 등 지수 데이터 안전 조회
    """
    try:
        df = retry_krx(stock.get_index_ohlcv_by_date, fromdate, todate, ticker)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception as e:
        log(f"[WARN] safe_get_index_ohlcv_by_date 실패: {fromdate}~{todate}, ticker={ticker}, err={e}")
        return pd.DataFrame()


def nearest_prev_business_day_safe(date_str: str) -> str:
    """
    pykrx의 get_nearest_business_day_in_a_week가 실패하거나
    빈 DataFrame으로 죽는 경우를 대비한 안전 버전
    """
    try:
        day = retry_krx(stock.get_nearest_business_day_in_a_week, date_str, prev=True)
        if day:
            return day
    except Exception as e:
        log(f"[WARN] get_nearest_business_day_in_a_week 실패: {date_str} -> {e}")

    # fallback: 하루씩 뒤로 가며 KOSPI 지수 존재 여부로 영업일 추정
    dt = datetime.strptime(date_str, "%Y%m%d")
    for _ in range(14):
        dt -= timedelta(days=1)
        probe = dt.strftime("%Y%m%d")
        df = safe_get_index_ohlcv_by_date(probe, probe, "1001")
        if not df.empty:
            log(f"[INFO] fallback 영업일 계산 성공: {date_str} -> {probe}")
            return probe

    raise RuntimeError(f"이전 영업일 계산 실패: {date_str}")


def nearest_same_or_prev_business_day_safe(date_str: str) -> str:
    """
    해당 날짜가 영업일이면 그 날짜, 아니면 직전 영업일
    """
    try:
        day = retry_krx(stock.get_nearest_business_day_in_a_week, date_str, prev=False)
        if day == date_str:
            return day
    except Exception as e:
        log(f"[WARN] same/prev 영업일 확인 실패: {date_str} -> {e}")

    try:
        df = safe_get_index_ohlcv_by_date(date_str, date_str, "1001")
        if not df.empty:
            return date_str
    except Exception:
        pass

    return nearest_prev_business_day_safe(date_str)


def decide_target_date_kst():
    """
    장 마감 전이면 직전 영업일, 장 마감 후면 당일(영업일이면) / 아니면 직전 영업일
    필요에 맞게 cutoff_hour 조정 가능
    """
    now_kst = datetime.now(KST)
    cutoff_hour = 18

    if now_kst.hour < cutoff_hour:
        base_dt = now_kst - timedelta(days=1)
        mode = "PRE_CLOSE_USE_PREV"
        target_date = nearest_prev_business_day_safe(yyyymmdd(base_dt))
    else:
        mode = "POST_CLOSE_USE_SAME_OR_PREV"
        target_date = nearest_same_or_prev_business_day_safe(yyyymmdd(now_kst))

    return target_date, mode, now_kst


def check_krx_login_env():
    """
    로그인 정보가 없더라도 치명 에러로 만들지 않음
    """
    krx_id = os.getenv("KRX_ID")
    krx_pw = os.getenv("KRX_PW")

    if not krx_id or not krx_pw:
        log("[WARN] KRX 로그인 스킵: KRX_ID 또는 KRX_PW 환경 변수가 설정되지 않았습니다.")
        return False

    # 실제 로그인 로직이 있다면 여기서 수행
    # 실패해도 바로 sys.exit 하지 말고 경고만 남기도록 권장
    try:
        log("[INFO] KRX 로그인 시도")
        # login_to_krx(krx_id, krx_pw)
        log("[INFO] KRX 로그인 성공")
        return True
    except Exception as e:
        log(f"[WARN] KRX 로그인 실패: {e}")
        return False


def get_market_data_safe(date_str: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    예시: 종목 데이터 조회도 빈 값 방어
    실제 사용 중인 함수명으로 바꿔도 됨
    """
    try:
        df = retry_krx(stock.get_market_ohlcv_by_ticker, date_str, market=market)
        if df is None or df.empty:
            log(f"[WARN] 종목 데이터가 비어 있습니다: {date_str}, market={market}")
            return pd.DataFrame()
        return df
    except Exception as e:
        log(f"[WARN] 종목 데이터 조회 실패: {date_str}, market={market}, err={e}")
        return pd.DataFrame()


def main():
    log("===== 조건검색 시작 =====")

    check_krx_login_env()

    try:
        target_date, mode, now_kst = decide_target_date_kst()
        log(f"[INFO] now_kst={now_kst}")
        log(f"[INFO] mode={mode}")
        log(f"[INFO] target_date={target_date}")
    except Exception as e:
        log(f"[ERROR] 대상일 계산 실패: {e}")
        sys.exit(1)

    # 아래는 예시
    market_df = get_market_data_safe(target_date, market="KOSPI")

    if market_df.empty:
        log(f"[ERROR] {target_date} 시장 데이터가 비어 있어 스캐너를 종료합니다.")
        sys.exit(1)

    # 여기부터 기존 조건검색 로직
    # --------------------------------------------------
    # result_df = run_conditions(market_df, target_date)
    # if result_df.empty:
    #     log("[INFO] 조건 충족 종목 없음")
    # else:
    #     log(result_df.to_string())
    # --------------------------------------------------

    log("===== 조건검색 종료 =====")


if __name__ == "__main__":
    main()
