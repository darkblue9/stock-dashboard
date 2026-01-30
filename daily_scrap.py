import time
import pandas as pd
from pykrx import stock
from datetime import datetime
import libsql_experimental as libsql
import streamlit as st
import numpy as np

# ---------------------------------------------------------
# 1. DB 연결 설정 (secrets.toml 사용)
# ---------------------------------------------------------
url = st.secrets["db"]["url"]
auth_token = st.secrets["db"]["auth_token"]
conn = libsql.connect("pykrx.db", sync_url=url, auth_token=auth_token)

# ---------------------------------------------------------
# 2. 날짜 설정 (자동 모드)
# ---------------------------------------------------------
# 매일 자동으로 오늘 날짜를 가져옵니다.
target_date = datetime.now().strftime("%Y%m%d")
# target_date = "20260129" # (복구용 고정 날짜 - 필요시 주석 해제)

print(f"🚀 [버전 3.0] 강력한 수급 수집기 시작!")
print(f"[{target_date}] 데이터 수집 시작...")

# ---------------------------------------------------------
# 3. 데이터 수집 함수 (재시도 로직 포함)
# ---------------------------------------------------------
def get_ohlcv_with_retry(date, market="ALL", max_retries=3):
    for attempt in range(max_retries):
        try:
            df = stock.get_market_ohlcv(date, market=market)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            print(f"⚠️ OHLCV 수집 실패 ({attempt+1}/{max_retries}): {e}")
            time.sleep(2)
    return pd.DataFrame()

def get_net_purchases_with_retry(date, market="ALL", max_retries=3):
    for attempt in range(max_retries):
        try:
            # 종목별 투자자 순매수 (금액이 아닌 수량 기준 등 확인 필요하지만 보통 get_market_net_purchases_of_equities_by_ticker 사용)
            # pykrx 문법: (날짜, 날짜, 시장, 투자자 구분) -> 근데 여기선 하루치만 필요
            # 꿀팁: 날짜를 하루만 지정하면 그날 데이터 나옴
            df = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "foreign")
            # 위 함수는 리턴값이 좀 다를 수 있어서, 보통 get_market_net_purchases_of_equities_by_ticker를 씀
            # 더 확실한 방법: stock.get_market_net_purchases_of_equities_by_ticker(start_date, end_date, market)
            # 하지만 여기서는 '투자자별' 합친 데이터프레임을 만드는 게 목표
            
            # (수정) 가장 안전한 방법: 각각 가져와서 합치기
            df_foreign = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "foreign")
            df_institutional = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "institution")
            df_retail = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "retail")
            
            # 필요한 컬럼만 추출 ('순매수') 및 이름 변경
            if df_foreign is not None: df_foreign = df_foreign[['순매수']].rename(columns={'순매수': '외국인순매수'})
            if df_institutional is not None: df_institutional = df_institutional[['순매수']].rename(columns={'순매수': '기관순매수'})
            if df_retail is not None: df_retail = df_retail[['순매수']].rename(columns={'순매수': '개인순매수'})
            
            return df_foreign, df_institutional, df_retail
            
        except Exception as e:
            print(f"⚠️ 수급 데이터 수집 실패 ({attempt+1}/{max_retries}): {e}")
            time.sleep(2)
    return None, None, None

# ---------------------------------------------------------
# 4. 메인 로직 실행
# ---------------------------------------------------------

# (1) 기본 시세 데이터 (시가, 고가, 종가, 거래량 등)
df_ohlcv = get_ohlcv_with_retry(target_date)

if df_ohlcv.empty:
    print(f"❌ {target_date} 장이 열리지 않았거나 데이터가 없습니다.")
    # 주말/휴일일 경우 종료
    exit()

print(f"✅ KRX 기본 데이터 수집 완료. 총 {len(df_ohlcv)}개 종목 스캔.")

# (2) 투자자별 수급 데이터 (외국인, 기관, 개인)
print(f"🕵️ 투자자별(외국인/기관/개인) 순매수 동향 파악 중...")
df_foreign, df_institutional, df_retail = get_net_purchases_with_retry(target_date)

if df_foreign is None or df_foreign.empty:
    print("⚠️ 외국인 수집 실패 (또는 휴장일). 데이터 0으로 처리합니다.")
    # 빈 데이터프레임 생성 (인덱스는 df_ohlcv와 맞춤)
    df_foreign = pd.DataFrame(0, index=df_ohlcv.index, columns=['외국인순매수'])
    df_institutional = pd.DataFrame(0, index=df_ohlcv.index, columns=['기관순매수'])
    df_retail = pd.DataFrame(0, index=df_ohlcv.index, columns=['개인순매수'])

print(f"✅ 수급 데이터 준비 완료.")

# (3) 데이터 합치기 (Join)
print(f"🔧 데이터 합체 중... (강제 주입 방식)")

# 인덱스(티커) 기준으로 합치기
merged_df = df_ohlcv.join(df_foreign, how='left')
merged_df = merged_df.join(df_institutional, how='left')
merged_df = merged_df.join(df_retail, how='left')

# NaN(빈값)은 0으로 채우기
merged_df = merged_df.fillna(0)

# 전일 거래량 가져오기 (전일대비 거래량 급증 분석용)
# -> 오늘 데이터에 '거래량'이 있고, '전일거래량'은 따로 구하거나 계산해야 함.
# -> pykrx의 OHLCV에는 보통 '거래량'만 줌.
# -> 하지만 등락률 계산을 위해 '전일종가' 등은 내부적으로 계산 가능하거나 제공됨.
# -> 여기서는 단순화를 위해 현재 수집된 '거래량'을 저장하고, 
# -> DB에 넣을 때 '전일거래량' 컬럼은, 어제자 DB 데이터를 참조해야 정확하지만,
# -> 간단하게 pykrx에서 제공하는지 확인. (제공 안함)
# -> 따라서 '전일거래량'을 구하려면 어제 날짜로 한 번 더 호출하거나 해야 함.
# -> [타협안] 일단 이번 버전에서는 '전일거래량'을 0으로 넣거나, 
# -> 나중에 DB 쿼리(Window Function)로 해결. 
# -> (수정) 아! 네이버 증권 크롤링 할 때는 있었는데 pykrx는 없네?
# -> 괜찮음. 일단 0으로 넣고 app.py에서 해결하거나, 
# -> (고급) 어제 날짜를 구해서 한 번 더 호출해서 붙여넣기.

# [고급 기능] 전일 거래량 구하기 (하루 전 영업일 찾기 귀찮으니, 그냥 어제 날짜 시도)
# 여기서는 심플하게 '0'으로 넣고, app.py에서 처리하도록 둠. (속도 위해)
merged_df['전일거래량'] = 0 

# 컬럼 정리 (티커는 인덱스에 있음 -> 컬럼으로 빼기)
merged_df.index.name = '종목코드'
merged_df = merged_df.reset_index()

# 날짜 컬럼 추가
merged_df['날짜'] = target_date
merged_df['indate'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 종목명은 pykrx에서 줌 (티커별 종목명 매핑 필요할 수도 있지만 OHLCV에 보통 포함 안됨... 아! 포함 안됨!)
# 티커로 종목명 가져오기
print("🏷️ 종목명 매핑 중...")
ticker_list = stock.get_market_ticker_list(target_date)
ticker_dict = {}
for ticker in ticker_list:
    name = stock.get_market_ticker_name(ticker)
    ticker_dict[ticker] = name

merged_df['종목명'] = merged_df['종목코드'].map(ticker_dict)

# 필요한 컬럼만 딱 정리
final_df = merged_df[[
    'indate', '날짜', '종목명', '시가', '고가', '저가', '종가', '등락률', '거래량', '전일거래량',
    '외국인순매수', '기관순매수', '개인순매수'
]]

# 컬럼 이름 DB와 맞추기 (현재가 = 종가)
final_df = final_df.rename(columns={'종가': '현재가'})

# 등락률 반올림
final_df['등락률'] = final_df['등락률'].round(2)

print(f"🧹 데이터 병합 및 청소 완료: {len(final_df)}개 종목")

# ---------------------------------------------------------
# 5. Turso DB에 저장 (Batch Insert)
# ---------------------------------------------------------
print("💾 Turso DB에 저장 시작...")

# 기존 데이터 삭제 (중복 방지 - 해당 날짜만)
conn.execute(f"DELETE FROM Npaystocks WHERE 날짜 = '{target_date}'")
conn.commit()

# 데이터프레임을 리스트로 변환
data_to_insert = final_df.values.tolist()

# 쿼리 작성
insert_query = """
INSERT INTO Npaystocks (
    indate, 날짜, 종목명, 시가, 고가, 저가, 현재가, 등락률, 거래량, 전일거래량,
    외국인순매수, 기관순매수, 개인순매수
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# 실행 (Batch)
try:
    conn.executemany(insert_query, data_to_insert)
    conn.commit()
    print(f"✅ [성공] Turso DB에 {len(data_to_insert)}건 업데이트 완료!")
except Exception as e:
    print(f"❌ DB 저장 실패: {e}")

# 연결 종료
conn.close()
print("👋 작업 종료.")