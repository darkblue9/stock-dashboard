import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from pykrx import stock

# ---------------------------------------------------------
# [버전 확인용] 이 로그가 안 보이면 옛날 코드 실행 중인 것임!
print("🚀 [버전 3.0] 강력한 수급 수집기(KeyError 해결판) 시작!", flush=True)
# ---------------------------------------------------------

# 1. 오늘 날짜 확인
#today = datetime.now().strftime('%Y%m%d')
#print(f"[{today}] 데이터 수집 시작...", flush=True)
today = "20260129"
print(f"🔧 [강제 복구 모드] 타겟 날짜: {today}")

# 2. KRX 전체 종목 기본 데이터 가져오기 (FDR)
try:
    df_krx = fdr.StockListing('KRX')
    print(f"✅ KRX 기본 데이터 수집 완료. 총 {len(df_krx)}개 종목 스캔.", flush=True)
except Exception as e:
    print(f"❌ FDR 데이터 수집 중 에러: {e}", flush=True)
    exit(1)

# 3. 투자자별 순매수 데이터 가져오기 (PyKRX)
print("🕵️ 투자자별(외국인/기관/개인) 순매수 동향 파악 중...", flush=True)

# 수급 데이터를 담을 딕셔너리 (실패해도 빈 깡통으로 시작)
supply_data = {
    '외국인순매수': pd.Series(dtype='int64'),
    '기관순매수': pd.Series(dtype='int64'),
    '개인순매수': pd.Series(dtype='int64')
}

def get_supply(investor_name, col_name):
    try:
        # PyKRX에서 데이터 긁기
        df = stock.get_market_net_purchases_of_equities_by_ticker(today, "ALL", investor=investor_name)
        
        # 컬럼명이 버전마다 다를 수 있어서 확인
        target_col = None
        for c in ['순매수수량', '순매수거래량', '순매수']:
            if c in df.columns:
                target_col = c
                break
        
        if target_col:
            return df[target_col] # Series 반환 (인덱스는 티커)
        else:
            return pd.Series(dtype='int64')
    except Exception as e:
        print(f"⚠️ {investor_name} 수집 실패 (장 안 열렸거나 에러): {e}", flush=True)
        return pd.Series(dtype='int64')

# 각각 수집 시도
supply_data['외국인순매수'] = get_supply("외국인", "외국인순매수")
supply_data['기관순매수'] = get_supply("기관합계", "기관순매수")
supply_data['개인순매수'] = get_supply("개인", "개인순매수")

print("✅ 수급 데이터 준비 완료.", flush=True)

# 4. 데이터 병합 및 전처리 🧹
df_clean = df_krx.dropna(subset=['Name']).copy()
df_clean = df_clean[df_clean['Name'].str.strip() != '']

df_clean['Close'] = pd.to_numeric(df_clean['Close'], errors='coerce')
df_clean = df_clean.dropna(subset=['Close'])

# 병합을 위해 Code를 인덱스로
df_clean.set_index('Code', inplace=True)

print("🔧 데이터 합체 중... (강제 주입 방식)", flush=True)

# [핵심] 딕셔너리에 있는 시리즈를 직접 할당 (KeyError 원천 봉쇄)
df_clean['외국인순매수'] = supply_data['외국인순매수']
df_clean['기관순매수'] = supply_data['기관순매수']
df_clean['개인순매수'] = supply_data['개인순매수']

# NaN(데이터 없음)을 0으로 채우기
df_clean['외국인순매수'] = df_clean['외국인순매수'].fillna(0).astype(int)
df_clean['기관순매수'] = df_clean['기관순매수'].fillna(0).astype(int)
df_clean['개인순매수'] = df_clean['개인순매수'].fillna(0).astype(int)

# 인덱스 복구
df_clean.reset_index(inplace=True)
df_clean.rename(columns={'Code': 'Symbol'}, inplace=True)

print(f"🧹 데이터 병합 및 청소 완료: {len(df_clean)}개 종목", flush=True)

# 5. DB 접속 및 '전일거래량' 가져오기
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보가 없습니다.", flush=True)
    exit(1)

clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
if "?" in clean_host: clean_host = clean_host.split("?")[0]

connection_url = f"sqlite+libsql://{clean_host}/?secure=true"
engine = create_engine(connection_url, connect_args={"auth_token": db_auth_token}, poolclass=NullPool)

prev_vol_map = {}

try:
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM Npaystocks WHERE 종목명 IS NULL OR 종목명 = ''"))
        
        query_date = text(f"SELECT MAX(날짜) FROM Npaystocks WHERE 날짜 < '{today}'")
        last_date = conn.execute(query_date).scalar()
        
        if last_date:
            print(f"📅 전일 데이터 기준일: {last_date}", flush=True)
            query_vol = text(f"SELECT 종목명, 거래량 FROM Npaystocks WHERE 날짜 = '{last_date}'")
            rows = conn.execute(query_vol).fetchall()
            prev_vol_map = {row[0]: row[1] for row in rows}
        else:
            print("ℹ️ 과거 데이터 없음 (첫 실행)", flush=True)

except Exception as e:
    print(f"⚠️ 전일거래량 조회 실패 (0 처리): {e}", flush=True)

# 6. 최종 데이터프레임 조립
result_df = pd.DataFrame()

result_df['날짜'] = [today] * len(df_clean)
result_df['종목명'] = df_clean['Name']
result_df['구분'] = df_clean['Market']
result_df['업종명'] = df_clean.get('Sector', '')

result_df['시가'] = df_clean['Open'].fillna(0).astype(int)
result_df['고가'] = df_clean['High'].fillna(0).astype(int)
result_df['저가'] = df_clean['Low'].fillna(0).astype(int)
result_df['현재가'] = df_clean['Close'].fillna(0).astype(int)

result_df['전일비'] = df_clean['Changes'].fillna(0).astype(int)
result_df['등락률'] = df_clean['ChagesRatio'].fillna(0).astype(float)

result_df['거래량'] = df_clean['Volume'].fillna(0).astype(int)
result_df['전일거래량'] = result_df['종목명'].map(prev_vol_map).fillna(0).astype(int)
result_df['시가총액'] = (df_clean.get('Marcap', 0) // 100000000).fillna(0).astype(int)
result_df['상장주식수'] = df_clean['Stocks'].fillna(0).astype(int)

result_df['외국인순매수'] = df_clean['외국인순매수']
result_df['기관순매수'] = df_clean['기관순매수']
result_df['개인순매수'] = df_clean['개인순매수']

result_df['신용잔고율'] = 0.0

print(f"📊 최종 데이터 준비 완료: {len(result_df)}건", flush=True)

# 7. DB 저장
try:
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM Npaystocks WHERE 날짜 = '{today}'"))
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print(f"✅ [성공] Turso DB에 {len(result_df)}건 업데이트 완료!", flush=True)
    engine.dispose()
    
except Exception as e:
    print("❌ DB 저장 실패.", flush=True)
    print(f"에러 메시지: {e}", flush=True)
    exit(1)
