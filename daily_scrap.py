import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime
import os
import time
import requests
from io import StringIO
import libsql_client
from libsql_client import Statement
import concurrent.futures # 분신술(멀티스레딩) 도구

# ---------------------------------------------------------
# [버전 6.0] 네이버 금융 직접 타격 (PyKRX 제거 버전)
# ---------------------------------------------------------
print("🚀 [버전 6.0] 수급 수집기 (Naver Direct Scraping) 시작!", flush=True)

# ★★★ 날짜 설정 ★★★
# target_date_str = datetime.now().strftime('%Y.%m.%d') 
target_date_str = "2026.02.02" # 네이버는 'YYYY.MM.DD' 포맷을 씀
target_date_db = target_date_str.replace(".", "") # DB엔 'YYYYMMDD'로 저장

print(f"📅 수집 타겟 날짜: {target_date_str} (DB저장: {target_date_db})", flush=True)

# 1. KRX 전체 종목 리스트 가져오기 (FDR)
try:
    print("running fdr...")
    df_krx = fdr.StockListing('KRX')
    # 우선주 등 제외하고 본주만 추리고 싶으면 여기서 필터링 가능하지만, 일단 다 가져옴
    df_krx = df_krx.dropna(subset=['Name'])
    df_krx['Code'] = df_krx['Code'].astype(str)
    print(f"✅ KRX 종목 리스트 확보: {len(df_krx)}개", flush=True)
except Exception as e:
    print(f"❌ FDR 에러: {e}", flush=True)
    exit(1)

# 2. [핵심] 네이버 금융에서 수급 데이터 뜯어오는 함수
# (아까 성공한 verify_samsung.py 로직을 함수로 만듦)
def scrap_naver_supply(code):
    url = f"https://finance.naver.com/item/frgn.naver?code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        res = requests.get(url, headers=headers, timeout=3) # 3초 안에 응답 없으면 패스
        
        # HTML 파싱
        dfs = pd.read_html(StringIO(res.text), attrs={"class": "type2"}, flavor='lxml')
        
        if len(dfs) > 1:
            df = dfs[1]
            # 2단 컬럼 문제 해결 (Empty row 제거)
            df = df.dropna(subset=[('날짜', '날짜')])
            
            # 날짜 형식 맞추기 ('2026.02.02')
            # 해당 날짜 행 찾기
            row = df[df[('날짜', '날짜')] == target_date_str]
            
            if not row.empty:
                # 데이터 추출 (외국인, 기관, 개인은 계산)
                # 네이버는 '개인' 순매수를 따로 안 보여줄 때가 많아서
                # 보통 [기관 + 외국인 + 개인 = 0] 공식을 쓰거나, 일단 외/기만 가져옴
                # 여기서는 화면에 보이는 '외국인', '기관' 순매매량을 가져옴
                foreign = int(row[('외국인', '순매매량')].values[0])
                agency = int(row[('기관', '순매매량')].values[0])
                # 개인 = -(외국인 + 기관) 으로 추정 (정확하진 않지만 근사치)
                individual = -(foreign + agency) 
                
                return {
                    "Code": code,
                    "외국인순매수": foreign,
                    "기관순매수": agency,
                    "개인순매수": individual
                }
    except Exception:
        pass # 에러 나면 그냥 빈 값 리턴
    
    return None

# 3. 멀티스레딩으로 2800개 종목 동시 채굴
print("🕵️ 전 종목 수급 데이터 채굴 중 (약 1~2분 소요)...", flush=True)

supply_data = []
# 스레드 20개로 동시에 긁어옴 (너무 많으면 차단당하니 20개 적당)
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    # 종목 코드 리스트
    codes = df_krx['Code'].tolist()
    
    # 진행률 표시를 위한 세팅
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

# 4. 데이터 합체 (FDR + 수급)
print("🔧 데이터 병합 중...", flush=True)

# 수급 리스트를 DataFrame으로 변환
df_supply = pd.DataFrame(supply_data)
if df_supply.empty:
    print("❌ 수급 데이터를 하나도 못 가져왔어. 날짜를 확인해봐.")
    exit(1)

# FDR 데이터랑 합치기 (Left Join)
df_final = pd.merge(df_krx, df_supply, on='Code', how='left')

# 결측치(수급 없는 종목) 0 처리
cols_to_fix = ['외국인순매수', '기관순매수', '개인순매수']
for col in cols_to_fix:
    df_final[col] = df_final[col].fillna(0).astype('int64')

# 컬럼 정리
df_final.rename(columns={'Code': 'Symbol', 'Name': '종목명', 'Market': '구분', 'Sector': '업종명'}, inplace=True)

# 필요한 컬럼만 뽑아서 DB 저장용 DF 만들기
result_df = pd.DataFrame()
result_df['날짜'] = [target_date_db] * len(df_final)
result_df['종목명'] = df_final['종목명']
result_df['구분'] = df_final['구분']
result_df['업종명'] = df_final['업종명'].fillna('')

# 숫자형 변환 안전하게
def to_int(series):
    return pd.to_numeric(series, errors='coerce').fillna(0).astype(int)

result_df['시가'] = to_int(df_final['Open'])
result_df['고가'] = to_int(df_final['High'])
result_df['저가'] = to_int(df_final['Low'])
result_df['현재가'] = to_int(df_final['Close'])
result_df['전일비'] = to_int(df_final['Changes'])
result_df['등락률'] = df_final['ChagesRatio'].fillna(0).astype(float)
result_df['거래량'] = to_int(df_final['Volume'])
# 전일거래량은 일단 0 (필요하면 아까 로직 추가 가능하지만 일단 생략)
result_df['전일거래량'] = 0 
result_df['시가총액'] = (df_final['Marcap'] // 100000000).fillna(0).astype(int)
result_df['상장주식수'] = to_int(df_final['Stocks'])

result_df['외국인순매수'] = df_final['외국인순매수']
result_df['기관순매수'] = df_final['기관순매수']
result_df['개인순매수'] = df_final['개인순매수']
result_df['신용잔고율'] = 0.0

print(f"📊 최종 저장할 데이터: {len(result_df)}건", flush=True)

# 5. DB 저장 (HTTPS + Batch)
# 네가 준 정보 하드코딩 (테스트용)
raw_url = "libsql://mystocks-lakemind9.aws-ap-northeast-1.turso.io"
db_auth_token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicm8iLCJpYXQiOjE3Njk0OTQyMDIsImlkIjoiYjA1OTY4NWItM2MzMC00NTg0LWE0M2YtM2I4ZWUyOWMwYTcwIiwicmlkIjoiM2E2NzQwYmQtOTRiZS00NjNkLWE2ZWYtN2ZlOGUzZGY1NTBlIn0.yELDul2Z-4mQHIkhDCsTTxM5ONvjHB48jONHdZkg-NCXxsAg00qmrYlVYsDrlbE2WUqPLvbl9WYED3RMbxwmAQ"

db_url = raw_url.replace("libsql://", "https://").replace("wss://", "https://")

print(f"🔌 Turso DB 연결 및 저장 시작...", flush=True)

try:
    client = libsql_client.create_client_sync(url=db_url, auth_token=db_auth_token)
    
    # 1. 기존 데이터 삭제
    client.execute(f"DELETE FROM Npaystocks WHERE 날짜 = '{target_date_db}'")
    
    # 2. 배치 저장
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