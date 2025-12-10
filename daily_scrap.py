import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, text

# 1. 오늘 날짜 포맷 (20251210 형태)
today = datetime.now().strftime('%Y%m%d')

print(f"[{today}] 주식 데이터 수집 시작...")

# 2. KRX(코스피+코스닥) 전체 종목 가져오기
# FDR은 정말 꿀이야. 이거 한 줄이면 다 가져와.
df_krx = fdr.StockListing('KRX')

# 3. 데이터 필터링 및 가공
# 등락률(ChagesRatio)이 0보다 큰 종목만 (네가 원하던 상승 종목)
df_rise = df_krx[df_krx['ChagesRatio'] > 0].copy()

# 4. 네 DB 스키마(Npaystocks)에 맞게 컬럼 이름 변경 및 정리
# FDR 컬럼: Code, Name, Close, Changes, ChagesRatio, Volume, Marcap, Stocks ...
# 네 DB 컬럼: 날짜, 구분, 종목명, 현재가, 전일비, 등락률, 거래량, 전일거래량, 시가총액, 상장주식수

# 구분(코스피/코스닥) 컬럼 만들기
df_rise['구분'] = df_rise['Market'] # Market 컬럼이 KOSPI, KOSDAQ 정보임

# 데이터프레임 정리
result_df = pd.DataFrame()
result_df['날짜'] = [today] * len(df_rise)
result_df['구분'] = df_rise['Market']
result_df['종목명'] = df_rise['Name']
result_df['현재가'] = df_rise['Close']
result_df['전일비'] = df_rise['Changes']
result_df['등락률'] = df_rise['ChagesRatio']
result_df['거래량'] = df_rise['Volume']
result_df['전일거래량'] = 0 # FDR 리스팅에는 전일거래량이 바로 안 나와. 필요하면 별도 계산 필요.
result_df['시가총액'] = df_rise['Marcap'] // 100000000 # 억 단위 절사 필요시 조정
result_df['상장주식수'] = df_rise['Stocks']

print(f"상승 종목 {len(result_df)}개 발견.")

# 5. Turso (온라인 DB) 접속 및 저장
# GitHub Secrets에 저장된 정보를 가져옴
db_url = os.environ.get("TURSO_DB_URL") # 예: sqlite+libsql://my-db.turso.io
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN")

if db_url and db_auth_token:
    # URL 뒤에 토큰 붙여서 접속 주소 완성
    connection_string = f"{db_url}?authToken={db_auth_token}&secure=true"
    engine = create_engine(connection_string)
    
    # DB에 때려 넣기 (append 모드)
    # index=False는 0,1,2... 같은 인덱스 숫자는 DB에 안 넣겠다는 뜻
    result_df.to_sql('Npaystocks', engine, if_exists='append', index=False)
    print("DB 저장 완료!")
else:
    print("DB 접속 정보가 없어서 저장을 못 했어. 환경변수를 확인해.")
    