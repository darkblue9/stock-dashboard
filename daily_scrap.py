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
today = datetime.now().strftime('%Y%m%d')
today = "20260202"
#print(f"🔧 [강제 복구 모드] 타겟 날짜: {today}")
print(f"[{today}] 데이터 수집 시작...", flush=True)

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

# [수정] get_supply 함수 전체 교체
def get_supply(investor_name, col_name):
    # ★ 테스트용: 날짜를 아까 성공했던 '20260129'로 잠시 고정!
    # (성공 확인 후 다시 today로 바꿀 예정)
    test_date = "20260129"  
    
    try:
        # 1. 아까 성공한 test_krx.py와 똑같은 파라미터 사용 (ALL)
        # 영어 파라미터로 변환 (안전을 위해)
        inv_code = investor_name
        if investor_name == "외국인": inv_code = "foreign"
        elif investor_name == "기관합계": inv_code = "financial"
        elif investor_name == "개인": inv_code = "individual"

        print(f"👉 [{investor_name}] 데이터 요청 중... (날짜: {test_date})", end=" ", flush=True)
        
        # 2. PyKRX 호출 (날짜 두 번 필수!)
        df = stock.get_market_net_purchases_of_equities_by_ticker(test_date, test_date, "ALL", investor=inv_code)
        
        if df.empty:
            print("❌ 실패 (데이터 없음)", flush=True)
            return pd.Series(dtype='int64')

        # 3. [핵심 수정] 컬럼 위치로 찾기 (이름이 매번 바뀌어서 이게 제일 확실함)
        # test_krx 결과: 0번째=종목명(두산...), 1번째=순매수금액(숫자)
        # 그러니까 우리는 무조건 '1번째' 칸을 가져와야 함!
        
        target_series = None
        
        # 만약 컬럼 이름에 '거래대금'이나 '순매수'가 명확히 있으면 그걸 씀
        found_col = None
        for col in df.columns:
            if "거래대금" in col or "순매수" in col:
                # 근데 '종목명' 컬럼은 제외해야 함
                if "종목명" not in col:
                    found_col = col
                    break
        
        if found_col:
            print(f"✅ 성공! (컬럼명: {found_col})", flush=True)
            target_series = df[found_col]
        elif len(df.columns) >= 2:
            # 이름을 못 찾겠으면, 두 번째 칸(인덱스 1)을 강제로 가져옴
            print(f"✅ 성공! (두 번째 컬럼 강제 선택: {df.columns[1]})", flush=True)
            target_series = df.iloc[:, 1]
        else:
            print("❌ 실패 (가져올 컬럼이 안 보임)", flush=True)
            return pd.Series(dtype='int64')

        return target_series

    except Exception as e:
        print(f"⚠️ 에러 발생: {e}", flush=True)
        return pd.Series(dtype='int64')
    
    
# 각각 수집 시도
supply_data['외국인순매수'] = get_supply("foreign", "외국인순매수")
supply_data['기관순매수'] = get_supply("financial", "기관순매수") # 기관합계 대신 financial 권장
supply_data['개인순매수'] = get_supply("individual", "개인순매수")

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

# -----------------------------------------------------------------------------
# 5. DB 접속 정보 준비 (연결은 아직 안 함)
# -----------------------------------------------------------------------------
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url or not db_auth_token:
    print("❌ DB 접속 정보가 없습니다.", flush=True)
    exit(1)

clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
if "?" in clean_host: clean_host = clean_host.split("?")[0]

connection_url = f"sqlite+libsql://{clean_host}/?secure=true"

# -----------------------------------------------------------------------------
# [수정 1] 전일 거래량 조회용 '1회용' 연결
# -----------------------------------------------------------------------------
print("🔌 [1차 연결] 전일 데이터 조회 중...", flush=True)
prev_vol_map = {}

try:
    # 조회용 엔진 생성
    engine_read = create_engine(connection_url, connect_args={"auth_token": db_auth_token}, poolclass=NullPool)
    
    with engine_read.connect() as conn:
        # 혹시 모를 쓰레기 데이터 정리
        conn.execute(text("DELETE FROM Npaystocks WHERE 종목명 IS NULL OR 종목명 = ''"))
        
        # 전일 날짜 찾기
        query_date = text(f"SELECT MAX(날짜) FROM Npaystocks WHERE 날짜 < '{today}'")
        last_date = conn.execute(query_date).scalar()
        
        if last_date:
            print(f"📅 전일 데이터 기준일: {last_date}", flush=True)
            query_vol = text(f"SELECT 종목명, 거래량 FROM Npaystocks WHERE 날짜 = '{last_date}'")
            rows = conn.execute(query_vol).fetchall()
            prev_vol_map = {row[0]: row[1] for row in rows}
        else:
            print("ℹ️ 과거 데이터 없음 (첫 실행)", flush=True)
            
    # ★ 다 썼으면 과감하게 폐기! (세션 만료 방지)
    engine_read.dispose()
    print("✅ 전일 데이터 조회 완료 및 연결 해제.", flush=True)

except Exception as e:
    print(f"⚠️ 전일거래량 조회 실패 (0 처리): {e}", flush=True)

# -----------------------------------------------------------------------------
# 6. 최종 데이터프레임 조립 (DB 연결 없이 메모리에서 작업)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# [수정 2] 저장을 위한 '새로운' 연결 (싱싱한 세션)
# -----------------------------------------------------------------------------
print("🔌 [2차 연결] DB 저장 시작...", flush=True)

try:
    # 저장용 엔진 새로 생성!
    engine_write = create_engine(connection_url, connect_args={"auth_token": db_auth_token}, poolclass=NullPool)
    
    with engine_write.begin() as conn:
        # 오늘 날짜 중복 데이터 삭제 후 입력
        conn.execute(text(f"DELETE FROM Npaystocks WHERE 날짜 = '{today}'"))
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print(f"✅ [성공] Turso DB에 {len(result_df)}건 업데이트 완료!", flush=True)
    
    # 마무리
    engine_write.dispose()
    
except Exception as e:
    print("❌ DB 저장 실패.", flush=True)
    print(f"에러 메시지: {e}", flush=True)
    exit(1)