import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

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
df_rise = df_krx[df_krx['ChagesRatio'] > 0].copy()

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
result_df['시가총액'] = df_rise['Marcap'] // 100000000 
result_df['상장주식수'] = df_rise['Stocks']

print(f"상승 종목 {len(result_df)}개 발견. DB 저장을 시도합니다.")

# 5. Turso DB 접속 및 저장
db_url = os.environ.get("TURSO_DB_URL")
# [수정] 토큰 가져올 때 공백 제거(.strip()) 추가
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip() 

if db_url and db_auth_token:
    # [디버깅] 토큰이 진짜 들어왔는지 길이만 확인 (로그에 키 노출 금지)
    print(f"DB 접속 시도: URL={db_url}, Token Length={len(db_auth_token)}")

    if db_url.startswith("libsql://"):
        db_url = "sqlite+" + db_url
        print("URL 스키마 자동 보정 완료 (libsql -> sqlite+libsql)")
    
try:
    # [수정 1] URL 스키마 확실하게 잡기 (https -> libsql 변환 등)
    # 만약 https://로 시작하면 libsql://로 변경 (드라이버 호환성 위함)
    if db_url.startswith("https://"):
        db_url = db_url.replace("https://", "libsql://")

    if not db_url.startswith("sqlite+libsql://"):
         # 기존 libsql:// 등을 sqlite+libsql:// 로 변경
        db_url = db_url.replace("libsql://", "sqlite+libsql://")

    # [수정 2] URL 끝에 슬래시(/) 없으면 추가
    if not db_url.endswith('/'):
        db_url = db_url + '/'

    # [핵심 수정] 토큰을 안전하게 포장(Encoding)
    # 토큰 안의 '=', '+' 같은 특수문자가 URL을 망가뜨리지 않게 변환함
    encoded_token = quote_plus(db_auth_token)

    # 포장된 토큰으로 URL 생성
    connection_string = f"{db_url}?authToken={encoded_token}&secure=true"

    # 엔진 생성
    engine = create_engine(connection_string)

    with engine.connect() as conn:
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)

    print("✅ DB 저장 성공! (Success)")

except Exception as e:
    print("❌ DB 저장 실패.")
    print(f"에러 메시지: {e}")
    exit(1)

else:
    print("❌ DB 접속 정보(Secrets)가 없습니다. (ENV 변수 확인 필요)")
    exit(1)
