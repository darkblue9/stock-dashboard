import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine

# 1. 오늘 날짜 확인
today = datetime.now().strftime('%Y%m%d')
print(f"[{today}] 주식 데이터 수집 시작...")

# 2. KRX 전체 종목 가져오기
try:
    df_krx = fdr.StockListing('KRX')
    print(f"KRX 데이터 수집 완료. 총 {len(df_krx)}개 종목 스캔.")
except Exception as e:
    print(f"데이터 수집 중 에러 발생: {e}")
    exit(1)

# 3. 데이터 필터링 (상승 종목만)
if 'ChagesRatio' in df_krx.columns:
    df_rise = df_krx[df_krx['ChagesRatio'] > 0].copy()
else:
    print("❌ 데이터에 등락률 컬럼이 없습니다.")
    exit(1)

# 4. DB 저장용 데이터프레임 만들기
result_df = pd.DataFrame()
result_df['날짜'] = [today] * len(df_rise)
result_df['구분'] = df_rise['Market']
result_df['종목명'] = df_rise['Name']
result_df['현재가'] = df_rise['Close']
result_df['전일비'] = df_rise['Changes']
result_df['등락률'] = df_rise['ChagesRatio']
result_df['거래량'] = df_rise['Volume']
result_df['전일거래량'] = 0 
result_df['시가총액'] = df_rise.get('Marcap', 0) // 100000000 
result_df['상장주식수'] = df_rise['Stocks']

print(f"상승 종목 {len(result_df)}개 발견. DB 저장을 시도합니다.")

# 5. Turso DB 접속 및 저장 (검증된 v3.0 방식)
# ------------------------------------------------------------------
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보(Secrets)가 없습니다.")
    exit(1)

try:
    # [1] 주소 세탁 (도메인만 추출)
    # 예: "https://mystock.turso.io" -> "mystock.turso.io"
    clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
    if "/" in clean_host: clean_host = clean_host.split("/")[0]
    if "?" in clean_host: clean_host = clean_host.split("?")[0]
    
    print(f"타겟 호스트: {clean_host}")

    # [2] 엔진 생성 (성공한 방식 적용!)
    # URL: 오직 위치만 적음 (토큰 X)
    connection_url = f"sqlite+libsql://{clean_host}/?secure=true"
    
    # Args: 토큰은 'auth_token'이라는 이름으로 뒷주머니에 넣어 전달
    engine_args = {
        "auth_token": db_auth_token
    }
    
    engine = create_engine(connection_url, connect_args=engine_args)
    
    # [3] 저장 시도
    with engine.connect() as conn:
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print("✅ DB 저장 성공! (Success)")
    
    # 깔끔하게 엔진 종료 (혹시 모를 무한 로딩 방지)
    engine.dispose()
    
except Exception as e:
    print("❌ DB 저장 실패.")
    print(f"에러 메시지: {e}")
    exit(1)
