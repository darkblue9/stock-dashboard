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

# 5. Turso DB 접속 설정 (HTTPS 강제 모드)
db_url_env = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not db_url_env or not db_auth_token:
    print("❌ DB 접속 정보(Secrets)가 없습니다.")
    exit(1)

# [핵심] URL 프로토콜을 강제로 https:// 로 변경
# libsql:// 이나 wss:// 로 시작하면 무조건 https:// 로 바꿔서 웹소켓 오류 방지
if "libsql://" in db_url_env:
    real_db_url = db_url_env.replace("libsql://", "https://")
elif "wss://" in db_url_env:
    real_db_url = db_url_env.replace("wss://", "https://")
else:
    real_db_url = db_url_env

# https://가 없으면 붙여줌
if not real_db_url.startswith("https://"):
    real_db_url = f"https://{real_db_url}"

print(f"접속 프로토콜: HTTPS (WebSocket 미사용)")
print(f"타겟 URL: {real_db_url}")

try:
    # [신의 한 수] create_engine에 주소를 넣지 말고, connect_args로 전달함.
    # 이렇게 하면 SQLAlchemy가 주소를 멋대로 변환하지 않고 드라이버에게 직통으로 줌.
    engine = create_engine(
        "sqlite+libsql://",  # 드라이버만 지정
        connect_args={
            'url': real_db_url,
            'authToken': db_auth_token
        }
    )
    
    with engine.connect() as conn:
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print("✅ DB 저장 성공! (Success)")
    
except Exception as e:
    print("❌ DB 저장 실패.")
    print(f"에러 메시지: {e}")
    exit(1)
