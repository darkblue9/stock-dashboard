import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, text
from pykrx import stock

# ---------------------------------------------------------
# [깃허브 액션용] 정석 버전 (SQLAlchemy 사용)
# ---------------------------------------------------------
print("🚀 [GitHub Action] 수급 수집기 시작", flush=True)

# 1. 날짜 설정 (자동)
today = datetime.now().strftime('%Y%m%d')
# ★ 테스트용: 오늘 데이터가 없을 수 있으니, 필요하면 아래 주석 풀어서 과거 날짜로 테스트
# today = "20260129" 
print(f"📅 수집 타겟 날짜: {today}", flush=True)

# 2. KRX 전체 종목 스캔
try:
    df_krx = fdr.StockListing('KRX')
    print(f"✅ KRX 종목 수집 완료: {len(df_krx)}개", flush=True)
except Exception as e:
    print(f"❌ FDR 에러: {e}", flush=True)
    exit(1)

# 3. 수급 데이터 수집 (PyKRX)
def get_supply(investor_name):
    inv_code = "foreign" if investor_name == "외국인" else "financial" if investor_name == "기관" else "individual"
    try:
        # 날짜 두 번 입력 (필수)
        df = stock.get_market_net_purchases_of_equities_by_ticker(today, today, "ALL", investor=inv_code)
        if df.empty: return pd.Series(dtype='int64')

        # 컬럼 자동 찾기
        target_col = None
        for col in df.columns:
            if ("거래대금" in col or "순매수" in col) and "종목명" not in col:
                target_col = col
                break
        if not target_col and len(df.columns) >= 2: target_col = df.columns[1]

        if target_col:
            return pd.to_numeric(df[target_col], errors='coerce').fillna(0).astype('int64')
        return pd.Series(dtype='int64')
    except:
        return pd.Series(dtype='int64')

print("🕵️ 투자자별 데이터 수집 중...", flush=True)
s_foreign = get_supply("외국인")
s_agency = get_supply("기관")
s_individual = get_supply("개인")

# 4. 데이터 병합
print("🔧 데이터 병합 중...", flush=True)
df_clean = df_krx.dropna(subset=['Name']).copy()
df_clean = df_clean[df_clean['Name'].str.strip() != '']
df_clean['Code'] = df_clean['Code'].astype(str)
df_clean.set_index('Code', inplace=True)

df_clean['외국인순매수'] = s_foreign
df_clean['기관순매수'] = s_agency
df_clean['개인순매수'] = s_individual

cols_to_fix = ['외국인순매수', '기관순매수', '개인순매수']
for col in cols_to_fix:
    df_clean[col] = df_clean[col].fillna(0).astype('int64')

df_clean.reset_index(inplace=True)
df_clean.rename(columns={'Code': 'Symbol'}, inplace=True)

# 숫자 변환
numeric_cols = ['Close', 'Open', 'High', 'Low', 'Volume', 'Changes', 'ChagesRatio', 'Stocks', 'Marcap']
for col in numeric_cols:
    if col in df_clean.columns:
        df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)

# 5. DB 저장 (SQLAlchemy + libsql-experimental)
print("🔌 DB 저장 시작...", flush=True)
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url:
    print("❌ 환경변수 누락", flush=True)
    exit(1)

# DB URL 정리
clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
connection_url = f"sqlite+libsql://{clean_host}/?secure=true"

try:
    engine = create_engine(connection_url, connect_args={"auth_token": db_auth_token})
    
    # 전일 거래량 조회 (옵션)
    prev_vol_map = {}
    try:
        with engine.connect() as conn:
            # 전일 날짜 찾기
            q_date = text(f"SELECT MAX(날짜) FROM Npaystocks WHERE 날짜 < '{today}'")
            last_date = conn.execute(q_date).scalar()
            if last_date:
                q_vol = text(f"SELECT 종목명, 거래량 FROM Npaystocks WHERE 날짜 = '{last_date}'")
                rows = conn.execute(q_vol).fetchall()
                prev_vol_map = {row[0]: row[1] for row in rows}
    except Exception as e:
        print(f"⚠️ 전일 데이터 조회 패스: {e}")

    # 최종 DF 생성
    result_df = pd.DataFrame()
    result_df['날짜'] = [today] * len(df_clean)
    result_df['종목명'] = df_clean['Name']
    result_df['구분'] = df_clean['Market']
    result_df['업종명'] = df_clean.get('Sector', '')
    result_df['시가'] = df_clean['Open'].astype(int)
    result_df['고가'] = df_clean['High'].astype(int)
    result_df['저가'] = df_clean['Low'].astype(int)
    result_df['현재가'] = df_clean['Close'].astype(int)
    result_df['전일비'] = df_clean['Changes'].astype(int)
    result_df['등락률'] = df_clean['ChagesRatio'].astype(float)
    result_df['거래량'] = df_clean['Volume'].astype(int)
    result_df['전일거래량'] = result_df['종목명'].map(prev_vol_map).fillna(0).astype(int)
    result_df['시가총액'] = (df_clean['Marcap'] // 100000000).astype(int)
    result_df['상장주식수'] = df_clean['Stocks'].astype(int)
    result_df['외국인순매수'] = df_clean['외국인순매수']
    result_df['기관순매수'] = df_clean['기관순매수']
    result_df['개인순매수'] = df_clean['개인순매수']
    result_df['신용잔고율'] = 0.0

    # 저장 (to_sql 사용 - 리눅스에선 잘 됨!)
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM Npaystocks WHERE 날짜 = '{today}'"))
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False)
        
    print(f"✅ 성공! {len(result_df)}건 저장 완료.", flush=True)

except Exception as e:
    print(f"❌ 실패: {e}", flush=True)
    exit(1)