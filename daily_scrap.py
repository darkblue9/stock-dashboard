import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

# 1. 오늘 날짜 확인
today = datetime.now().strftime('%Y%m%d')
print(f"[{today}] 주식 데이터 수집 시작...", flush=True)

# 2. KRX 전체 종목 가져오기
try:
    df_krx = fdr.StockListing('KRX')
    print(f"KRX 데이터 수집 완료. 총 {len(df_krx)}개 종목 스캔.", flush=True)
except Exception as e:
    print(f"데이터 수집 중 에러 발생: {e}", flush=True)
    exit(1)

# 3. 데이터 전처리 (빈 행만 제거, 상승/하락 필터링 X)
# [수정] 이제 하락한 종목도 다 가져갑니다. (나중을 위해)
df_clean = df_krx.dropna(subset=['Name', 'Close']).copy()

# ------------------------------------------------------------------
# [4] DB 접속 및 '전일거래량' 가져오기
# ------------------------------------------------------------------
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보가 없습니다.", flush=True)
    exit(1)

# 호스트 정리
clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
if "?" in clean_host: clean_host = clean_host.split("?")[0]

connection_url = f"sqlite+libsql://{clean_host}/?secure=true"
engine = create_engine(connection_url, connect_args={"auth_token": db_auth_token}, poolclass=NullPool)

# 전일거래량 매핑용 사전
prev_vol_map = {}

try:
    with engine.connect() as conn:
        # 가장 최근 날짜 찾기
        query_date = text(f"SELECT MAX(날짜) FROM Npaystocks WHERE 날짜 < '{today}'")
        last_date = conn.execute(query_date).scalar()
        
        if last_date:
            print(f"📅 기준 과거 데이터: {last_date}일자", flush=True)
            # 그 날짜의 모든 종목 거래량 가져오기
            query_vol = text(f"SELECT 종목명, 거래량 FROM Npaystocks WHERE 날짜 = '{last_date}'")
            rows = conn.execute(query_vol).fetchall()
            
            prev_vol_map = {row[0]: row[1] for row in rows}
            print(f"🔍 전일거래량 데이터 {len(prev_vol_map)}건 확보.", flush=True)
        else:
            print("ℹ️ 과거 데이터 없음 (첫 실행이거나 데이터 누락)", flush=True)

except Exception as e:
    print(f"⚠️ 전일거래량 조회 실패 (0으로 진행): {e}", flush=True)

# ------------------------------------------------------------------

# 5. 최종 데이터프레임 조립 (전 종목 대상)
# [수정] df_rise 대신 df_clean(전체)을 사용
result_df = pd.DataFrame()
result_df['날짜'] = [today] * len(df_clean)
result_df['구분'] = df_clean['Market']
result_df['종목명'] = df_clean['Name']
result_df['현재가'] = df_clean['Close']
result_df['전일비'] = df_clean['Changes']
result_df['등락률'] = df_clean['ChagesRatio'] # 하락 종목은 마이너스로 들어감
result_df['거래량'] = df_clean['Volume']

# 전일거래량 매핑 (없으면 0)
result_df['전일거래량'] = result_df['종목명'].map(prev_vol_map).fillna(0).astype(int)

result_df['시가총액'] = df_clean.get('Marcap', 0) // 100000000 
result_df['상장주식수'] = df_clean['Stocks']

print(f"총 {len(result_df)}개 종목 준비 완료. (전종목 저장)", flush=True)

# 6. DB 저장 (트랜잭션)
try:
    with engine.begin() as conn:
        # 청소
        conn.execute(text(f"DELETE FROM Npaystocks WHERE 날짜 = '{today}'"))
        
        # 저장
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print(f"✅ DB 저장 성공! {len(result_df)}건 (전종목) 처리됨.", flush=True)
    engine.dispose()
    
except Exception as e:
    print("❌ DB 저장 실패.", flush=True)
    print(f"에러 메시지: {e}", flush=True)
    exit(1)
