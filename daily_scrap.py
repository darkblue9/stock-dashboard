import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
import time
import requests
from io import StringIO
import libsql_client
from libsql_client import Statement
import concurrent.futures

# ---------------------------------------------------------
# [버전 6.1] 최종 완결 (수급 성공 + 업종명 에러 해결)
# ---------------------------------------------------------
print("🚀 [버전 6.1] 수급 수집기 (Safety Mode) 시작!", flush=True)

# ★★★ 날짜 설정 ★★★
# target_date_str = datetime.now().strftime('%Y.%m.%d') 
target_date_str = "2026.02.02" 
target_date_db = target_date_str.replace(".", "")

print(f"📅 수집 타겟 날짜: {target_date_str} (DB저장: {target_date_db})", flush=True)

# 1. KRX 전체 종목 리스트 가져오기 (FDR)
try:
    print("running fdr...")
    df_krx = fdr.StockListing('KRX')
    df_krx = df_krx.dropna(subset=['Name']) # 이름 없는 데이터 제거
    df_krx['Code'] = df_krx['Code'].astype(str)
    print(f"✅ KRX 종목 리스트 확보: {len(df_krx)}개", flush=True)
except Exception as e:
    print(f"❌ FDR 에러: {e}", flush=True)
    exit(1)

# 2. 네이버 금융 수급 크롤링 함수
def scrap_naver_supply(code):
    url = f"https://finance.naver.com/item/frgn.naver?code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        res = requests.get(url, headers=headers, timeout=3)
        dfs = pd.read_html(StringIO(res.text), attrs={"class": "type2"}, flavor='lxml')
        
        if len(dfs) > 1:
            df = dfs[1]
            df = df.dropna(subset=[('날짜', '날짜')])
            
            # 타겟 날짜 행 찾기
            row = df[df[('날짜', '날짜')] == target_date_str]
            
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
    except Exception:
        pass
    return None

# 3. 멀티스레딩 채굴 (2800개 동시 진행)
print("🕵️ 전 종목 수급 데이터 채굴 중 (약 1~2분 소요)...", flush=True)

supply_data = []
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    codes = df_krx['Code'].tolist()
    futures = {executor.submit(scrap_naver_supply, code): code for code in codes}
    completed = 0
    total = len(codes)
    
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            supply_data.append(result)
        
        completed += 1
        if completed % 100 == 0:
            print(f"   👉 진행률: {completed}/{total} ({len(supply_data)}건 확보)", end="\r")

print(f"\n✅ 수급 데이터 수집 완료! 총 {len(supply_data)}개 종목 성공.", flush=True)

# 4. 데이터 병합 (여기가 에러 났던 곳!)
print("🔧 데이터 병합 중...", flush=True)

df_supply = pd.DataFrame(supply_data)
if df_supply.empty:
    print("❌ 수급 데이터를 하나도 못 가져왔어. 날짜를 확인해봐.")
    exit(1)

# 합치기
df_final = pd.merge(df_krx, df_supply, on='Code', how='left')

# [수정됨] 컬럼 정리 (안전하게 처리)
# 먼저 존재하는 컬럼만 이름 변경
rename_map = {'Code': 'Symbol', 'Name': '종목명', 'Market': '구분', 'Sector': '업종명'}
df_final.rename(columns=rename_map, inplace=True)

# ★★★ [핵심 수정] '업종명' 컬럼이 없으면 강제로 만듦 (KeyError 방지) ★★★
if '업종명' not in df_final.columns:
    print("⚠️ 경고: 'Sector' 정보가 없어서 빈칸으로 채웁니다.", flush=True)
    df_final['업종명'] = ''

# 결측치 0 처리
cols_to_fix = ['외국인순매수', '기관순매수', '개인순매수']
for col in cols_to_fix:
    df_final[col] = df_final[col].fillna(0).astype('int64')

# 최종 저장용 DataFrame 생성
result_df = pd.DataFrame()
result_df['날짜'] = [target_date_db] * len(df_final)
result_df['종목명'] = df_final['종목명']
result_df['구분'] = df_final['구분']
result_df['업종명'] = df_final['업종명'].fillna('') # 이제 안전함!

def to_int(series):
    return pd.to_numeric(series, errors='coerce').fillna(0).astype(int)

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

print(f"📊 최종 저장할 데이터: {len(result_df)}건", flush=True)

# 5. DB 저장
raw_url = "libsql://mystocks-lakemind9.aws-ap-northeast-1.turso.io"
db_auth_token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicm8iLCJpYXQiOjE3Njk0OTQyMDIsImlkIjoiYjA1OTY4NWItM2MzMC00NTg0LWE0M2YtM2I4ZWUyOWMwYTcwIiwicmlkIjoiM2E2NzQwYmQtOTRiZS00NjNkLWE2ZWYtN2ZlOGUzZGY1NTBlIn0.yELDul2Z-4mQHIkhDCsTTxM5ONvjHB48jONHdZkg-NCXxsAg00qmrYlVYsDrlbE2WUqPLvbl9WYED3RMbxwmAQ"

db_url = raw_url.replace("libsql://", "https://").replace("wss://", "https://")

print(f"🔌 Turso DB 저장 시작...", flush=True)

try:
    client = libsql_client.create_client_sync(url=db_url, auth_token=db_auth_token)
    
    # 기존 데이터 삭제
    client.execute(f"DELETE FROM Npaystocks WHERE 날짜 = '{target_date_db}'")
    
    # 배치 저장
    placeholders = ", ".join(["?"] * len(result_df.columns))
    sql = f"INSERT INTO Npaystocks ({', '.join(result_df.columns)}) VALUES ({placeholders})"
    
    values = result_df.values.tolist()
    stmts = []
    
    for row in values:
        safe_row = [
            int(x) if isinstance(x, (int, pd.Int64Dtype)) else 
            float(x) if isinstance(x, float) else 
            str(x) 
            for x in row
        ]
        stmts.append(Statement(sql, args=safe_row))
    
    batch_size = 50
    total_rows = len(stmts)
    
    for i in range(0, total_rows, batch_size):
        chunk = stmts[i : i + batch_size]
        client.batch(chunk)
        print(f"      ... {min(i + batch_size, total_rows)} / {total_rows} 완료", end="\r")

    client.close()
    print(f"\n✅ [성공] Turso DB 업데이트 완료! (날짜: {target_date_db})", flush=True)

except Exception as e:
    print(f"\n❌ DB 작업 실패: {e}", flush=True)