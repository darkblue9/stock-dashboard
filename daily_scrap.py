import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, text  # [추가] SQL 직접 명령용 'text' 추가
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

# 3. 데이터 필터링 (상승 종목만)
if 'ChagesRatio' in df_krx.columns:
    df_rise = df_krx[df_krx['ChagesRatio'] > 0].copy()
else:
    print("❌ 데이터에 등락률 컬럼이 없습니다.", flush=True)
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

print(f"상승 종목 {len(result_df)}개 발견. DB 저장을 시도합니다.", flush=True)

# 5. Turso DB 접속 및 저장 (중복 방지 로직 적용)
# ------------------------------------------------------------------
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보(Secrets)가 없습니다.", flush=True)
    exit(1)

try:
    # [1] 주소 세탁
    clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
    if "/" in clean_host: clean_host = clean_host.split("/")[0]
    if "?" in clean_host: clean_host = clean_host.split("?")[0]
    
    print(f"타겟 호스트: {clean_host}", flush=True)

    # [2] 엔진 생성
    connection_url = f"sqlite+libsql://{clean_host}/?secure=true"
    engine_args = {"auth_token": db_auth_token}
    
    engine = create_engine(
        connection_url, 
        connect_args=engine_args,
        poolclass=NullPool
    )
    
    # [3] 저장 시도 (트랜잭션 시작)
    # engine.begin()을 쓰면 성공 시 자동 커밋(저장), 실패 시 롤백(취소) 해줌
    with engine.begin() as conn:
        
        # (A) 청소 단계: 오늘 날짜 데이터가 이미 있으면 삭제
        # 만약 테이블이 없으면 에러가 날 수 있으니 try-except로 감쌈
        try:
            delete_query = text(f"DELETE FROM Npaystocks WHERE 날짜 = '{today}'")
            conn.execute(delete_query)
            print(f"🧹 [청소 완료] {today}일자 기존 데이터 삭제됨 (중복 방지)", flush=True)
        except Exception as delete_error:
            # 테이블이 아직 없어서 삭제를 못 하는 경우는 그냥 넘어감
            print(f"ℹ️ 기존 데이터 삭제 건너뜀 (첫 실행이거나 테이블 없음): {delete_error}", flush=True)

        # (B) 입주 단계: 새 데이터 저장
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print(f"✅ DB 저장 성공! 총 {len(result_df)}건 저장 완료.", flush=True)
    
    # 엔진 정리
    engine.dispose()
    
except Exception as e:
    print("❌ DB 저장 실패.", flush=True)
    print(f"에러 메시지: {e}", flush=True)
    exit(1)
