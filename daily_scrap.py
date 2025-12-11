import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # [중요] 토큰 포장용 도구

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
# 'ChagesRatio' 컬럼이 없는 경우를 대비하거나, 데이터 타입을 확실히 함
if 'ChagesRatio' in df_krx.columns:
    df_rise = df_krx[df_krx['ChagesRatio'] > 0].copy()
else:
    print("❌ 'ChagesRatio' 컬럼을 찾을 수 없습니다.")
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
# Marcap이 없는 경우 예외처리 혹은 0으로 대체
if 'Marcap' in df_rise.columns:
    result_df['시가총액'] = df_rise['Marcap'] // 100000000 
else:
    result_df['시가총액'] = 0
result_df['상장주식수'] = df_rise['Stocks']

print(f"상승 종목 {len(result_df)}개 발견. DB 저장을 시도합니다.")

# 5. Turso DB 접속 및 저장
# 환경변수에서 값 가져오기 (앞뒤 공백 제거 필수)
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보(Secrets)가 없습니다. (ENV 변수 확인 필요)")
    exit(1)

try:
    # [1단계] URL 청소: 프로토콜(libsql://, https://) 제거하고 도메인만 남김
    # 예: "libsql://my-db.turso.io" -> "my-db.turso.io"
    clean_host = raw_url.replace("libsql://", "").replace("https://", "").replace("wss://", "")
    
    # 혹시 뒤에 '/'나 '?'가 붙어있으면 제거
    if "/" in clean_host:
        clean_host = clean_host.split("/")[0]
    if "?" in clean_host:
        clean_host = clean_host.split("?")[0]

    # [2단계] 토큰 인코딩: 토큰 내의 특수문자(=, +)를 URL 전용 문자로 변환
    encoded_token = quote_plus(db_auth_token)
    
    # [3단계] 최종 접속 주소 생성 (SQLAlchemy 표준 형식)
    # 형식: sqlite+libsql://:비밀번호@주소/?secure=true
    # 설명: ID는 비워두고(: 앞), 비밀번호 자리에 토큰을 넣음.
    connection_string = f"sqlite+libsql://:{encoded_token}@{clean_host}/?secure=true"
    
    print(f"접속 시도 (Host: {clean_host})")
    
    # 엔진 생성
    engine = create_engine(connection_string)
    
    # 데이터 저장
    with engine.connect() as conn:
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print("✅ DB 저장 성공! (Success)")
    
except Exception as e:
    print("❌ DB 저장 실패.")
    print(f"에러 메시지: {e}")
    exit(1)
