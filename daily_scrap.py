import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
import time
import requests
from io import StringIO
import concurrent.futures
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# [버전 6.3] 유연한 저장 모드 (수급 없으면 0으로 채우고 저장)
# ---------------------------------------------------------
print("🚀 [버전 6.3] 수급 수집기 (Partial Save Mode) 시작!", flush=True)

# 1. 날짜 설정 (자동)
today_str = datetime.now().strftime('%Y.%m.%d')
# today_str = "2026.02.04" # 테스트용
target_date_db = today_str.replace(".", "")

print(f"📅 수집 타겟 날짜: {today_str} (DB저장: {target_date_db})", flush=True)

# 2. KRX 전체 종목 리스트 (FDR) - 가격/거래량 정보는 여기서 옴
try:
    print("running fdr...")
    df_krx = fdr.StockListing('KRX')
    df_krx = df_krx.dropna(subset=['Name'])
    df_krx['Code'] = df_krx['Code'].astype(str)
    print(f"✅ KRX 종목 리스트 확보: {len(df_krx)}개 (가격 데이터 확보)", flush=True)
except Exception as e:
    print(f"❌ FDR 에러: {e}", flush=True)
    exit(1)

# 3. 네이버 금융 크롤링 함수
def scrap_naver_supply(code):
    url = f"https://finance.naver.com/item/frgn.naver?code={code}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=3)
        dfs = pd.read_html(StringIO(res.text), attrs={"class": "type2"}, flavor='lxml')
        if len(dfs) > 1:
            df = dfs[1].dropna(subset=[('날짜', '날짜')])
            # 오늘 날짜 행이 있는지 확인
            row = df[df[('날짜', '날짜')] == today_str]
            if not row.empty:
                foreign = int(row[('외국인', '순매매량')].values[0])
                agency = int(row[('기관', '순매매량')].values[0])
                individual = -(foreign + agency)
                return {
                    "Code": code, 
                    "외국인순매수": foreign, 
                    "기관순매수": agency, 
                    "개인순매수": individual
                }
    except:
        pass
    return None

# 4. 멀티스레딩 채굴
print("🕵️ 전 종목 수급 데이터 채굴 중...", flush=True)
supply_data = []
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    codes = df_krx['Code'].tolist()
    futures = {executor.submit(scrap_naver_supply, code): code for code in codes}
    completed = 0
    total = len(codes)
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result: supply_data.append(result)
        completed += 1
        if completed % 100 == 0:
            print(f"   👉 진행률: {completed}/{total}", end="\r")

print(f"\n✅ 수집 완료! {len(supply_data)}개 종목 수급 확보.", flush=True)

# 5. 데이터 병합 (★핵심 수정: 수급 없어도 죽지 않기★)
print("🔧 데이터 병합 중...", flush=True)

df_supply = pd.DataFrame(supply_data)

if df_supply.empty:
    print("⚠️ [알림] 오늘자 수급 데이터가 아직 없습니다. (0으로 채우고 가격만 저장합니다)")
    # 수급 데이터프레임이 비었으면, 그냥 df_krx를 복사해서 씀
    df_final = df_krx.copy()
    # 수급 컬럼 0으로 강제 생성
    df_final['외국인순매수'] = 0
    df_final['기관순매수'] = 0
    df_final['개인순매수'] = 0
else:
    # 수급 데이터가 있으면 병합
    df_final = pd.merge(df_krx, df_supply, on='Code', how='left')

# 컬럼명 정리
rename_map = {'Code': 'Symbol', 'Name': '종목명', 'Market': '구분', 'Sector': '업종명'}
df_final.rename(columns=rename_map, inplace=True)

# 업종명 없는 경우 처리
if '업종명' not in df_final.columns: df_final['업종명'] = ''

# 결측치(NaN) 0으로 채우기 (Merge 안 된 종목들 포함)
cols_to_fix = ['외국인순매수', '기관순매수', '개인순매수']
for col in cols_to_fix:
    if col not in df_final.columns: # 혹시 모르니 체크
        df_final[col] = 0
    df_final[col] = df_final[col].fillna(0).astype('int64')

# 최종 DF 생성
result_df = pd.DataFrame()
result_df['날짜'] = [target_date_db] * len(df_final)
result_df['종목명'] = df_final['종목명']
result_df['구분'] = df_final['구분']
result_df['업종명'] = df_final['업종명'].fillna('')

def to_int(series): return pd.to_numeric(series, errors='coerce').fillna(0).astype(int)

result_df['시가'] = to_int(df_final['Open'])
result_df['고가'] = to_int(df_final['High'])
result_df['저가'] = to_int(df_final['Low'])
result_df['현재가'] = to_int(df_final['Close'])
result_df['전일비'] = to_int(df_final['Changes'])
result_df['등락률'] = df_final['ChagesRatio'].fillna(0).astype(float)
result_df['거래량'] = to_int(df_final['Volume'])
result_df['전일거래량'] = 0 
result_df['시가총액'] = (df_final['Marcap'] // 100000000).fillna(0).astype(int)
result_df['상장주식수'] = to_int(df_final['Stocks'])
result_df['외국인순매수'] = df_final['외국인순매수']
result_df['기관순매수'] = df_final['기관순매수']
result_df['개인순매수'] = df_final['개인순매수']
result_df['신용잔고율'] = 0.0

print(f"📊 저장 대상: {len(result_df)}건 (수급 데이터 유무와 상관없이 저장)", flush=True)

# 6. DB 저장
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
db_auth_token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

if not raw_url:
    # 로컬 테스트용 (필요시 사용)
    pass 

if not raw_url or not db_auth_token:
    print("❌ 환경변수(TURSO_DB_URL, TURSO_AUTH_TOKEN)가 설정되지 않았습니다.")
    exit(1)

print(f"🔌 Turso DB 연결...", flush=True)

# URL 변환
clean_host = raw_url.replace("https://", "").replace("libsql://", "").replace("wss://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
connection_url = f"sqlite+libsql://{clean_host}/?secure=true"

try:
    engine = create_engine(connection_url, connect_args={"auth_token": db_auth_token})
    
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM Npaystocks WHERE 날짜 = '{target_date_db}'"))
        result_df.to_sql('Npaystocks', conn, if_exists='append', index=False, chunksize=500)
        
    print(f"\n✅ [완전 성공] DB 저장 완료! (날짜: {target_date_db})", flush=True)

except Exception as e:
    print(f"\n❌ DB 저장 실패: {e}", flush=True)
    exit(1)