import FinanceDataReader as fdr
from pykrx import stock
import pandas as pd

# 1. 날짜 고정 (아까 성공했던 날)
date = "20260129"

print(f"🚀 [진단] 삼성전자(005930) 데이터 합체 실험 ({date})", flush=True)

# ---------------------------------------------------------
# Step 1. 마스터 장부(FDR) 가져오기
# ---------------------------------------------------------
print("\n1. FDR에서 기본 정보 가져오는 중...", end="")
df_krx = fdr.StockListing('KRX')

# 삼성전자 찾기
df_master = df_krx[df_krx['Code'] == '005930'].copy()

if df_master.empty:
    print("❌ FDR에서 삼성전자를 못 찾음! (코드 문제?)")
    exit()

# 인덱스를 Code로 설정 (네 daily_scrap.py 방식)
df_master.set_index('Code', inplace=True)
master_idx = df_master.index[0]

print(" 완료!")
print(f"   👉 마스터측 주소(Index): '{master_idx}'")
print(f"   👉 주소 타입: {type(master_idx)}")

# ---------------------------------------------------------
# Step 2. 수급 데이터(PyKRX) 가져오기
# ---------------------------------------------------------
print("\n2. PyKRX에서 수급 데이터 가져오는 중...", end="")

# 네가 쓰는 방식대로 전체 조회
df_supply = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", investor="foreign")

# 삼성전자(005930)가 인덱스에 있는지 확인
if '005930' in df_supply.index:
    supply_idx = '005930'
    print(" 완료!")
    print(f"   👉 수급측 주소(Index): '{supply_idx}' (발견됨!)")
    
    # 실제 값 확인
    # 컬럼 찾기
    col_name = None
    for col in df_supply.columns:
        if "거래대금" in col or "순매수" in col:
            col_name = col
            break
    if not col_name: col_name = df_supply.columns[1] # 못 찾으면 2번째꺼
    
    val = df_supply.loc[supply_idx][col_name]
    print(f"   👉 가져온 값: {val} (컬럼명: {col_name})")
    
else:
    print(" ❌ 실패!")
    print(f"   ⚠️ PyKRX 인덱스엔 '005930'이 없음. 대신 이런 게 있음: {df_supply.index[:3]}")
    # 혹시 숫자로 되어 있나?
    if 5930 in df_supply.index:
         print("   👉 아하! 숫자로 되어 있네 (5930)")
    exit()

# ---------------------------------------------------------
# Step 3. 강제 합체 시도
# ---------------------------------------------------------
print("\n3. 합체 시도 (Merge)...")

try:
    # 데이터프레임에 컬럼 추가 시도
    # (여기서 인덱스 타입이 다르면 NaN(결측치)이 됨 -> 나중에 0으로 바뀜)
    df_master['외국인'] = df_supply[col_name]
    
    result_val = df_master['외국인'].iloc[0]
    print(f"   👉 합친 후 결과값: {result_val}")
    
    if pd.isna(result_val):
        print("   ❌ 실패! 값이 NaN(빈칸)이 됨. 주소 타입 불일치 확실함!")
    else:
        print("   ✅ 성공! 값이 제대로 들어감.")
        
except Exception as e:
    print(f"   💥 에러 발생: {e}")

print("\n🏁 진단 종료.")