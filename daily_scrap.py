import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # [핵심] 토큰 안전 포장지

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
result_df['시가총액'] = df_rise.get('Marcap', 0) // 100000000 # 없으면 0 처리
result_df['상장주식수'] = df_rise['Stocks']

print(f"상승 종목 {len(result_df)}개 발견. DB 저장을 시도합니다.")

# 5. Turso DB 접속 및 저장 (정석 방법)
# ------------------------------------------------------------------
# [1] 환경변수 가져오기 (앞뒤 공백 제거)
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보(Secrets)가 없습니다.")
    exit(1)

try:
    # [2] 주소 세탁 (https://, libsql:// 다 떼고 도메인만 남김)
    # 예: "https://mystock.turso.io" -> "mystock.turso.io"
    clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
    
    # 혹시 뒤에 붙은 경로(/)나 파라미터(?) 제거
    if "/" in clean_host:
        clean_host = clean_host.split("/")[0]
    if "?" in clean_host:
        clean_host = clean_host.split("?")[0]

    print(f"타겟 호스트: {clean_host}")

    # [3] 토큰 인코딩 (필수!)
    # 토큰에 있는 '=', '+' 같은 기호가 주소를 망가뜨리지 않게 변환
    encoded_token = quote_plus(db_auth_token)
    
    # [4] SQLAlchemy 연결 문자열 생성 (표준 포맷)
    # 문법: sqlite+libsql://:비밀번호@주소/?secure=true
    # 설명: ID는 비우고(:), 비밀번호 자리에 인코딩된 토큰을 넣음.
    connection_string = f"sqlite+libsql://:{encoded_token}@{clean_host}/?secure=true"
    
    # [5] 엔진 생성 및 저장
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print("✅ DB 저장 성공! (Success)")
    
except Exception as e:
    print("❌ DB 저장 실패.")
    print(f"에러 메시지: {e}")
    exit(1)
