from pykrx import stock
import pandas as pd

target_date = "20260129" # 문제가 된 29일
investor = "외국인"

print(f"🔍 [진단 시작] {target_date} {investor} 데이터 조회 테스트\n")

# [1] 기존 코드 방식 (오타)
print("❌ 1. 기존 방식 (실패 예상): (date, 'ALL', investor)")
try:
    # 종료일 자리에 'ALL'이 들어가 있음 -> 에러 나거나 빈 데이터
    df_bug = stock.get_market_net_purchases_of_equities_by_ticker(target_date, "ALL", investor=investor)
    print(f"   결과: {len(df_bug)}건 조회됨 (내용: {df_bug.head(1).values})")
except Exception as e:
    print(f"   결과: 💥 에러 발생! ({e})")

print("-" * 30)

# [2] 수정된 방식 (정석)
print("✅ 2. 수정 방식 (성공 예상): (date, date, 'ALL', investor)")
try:
    # 시작일과 종료일을 똑같이 넣어줘야 '하루치'를 정확히 가져옴
    df_fix = stock.get_market_net_purchases_of_equities_by_ticker(target_date, target_date, "ALL", investor=investor)
    print(f"   결과: {len(df_fix)}건 조회됨!")
    if not df_fix.empty:
        print(f"   --> 예시: {df_fix.iloc[0].name} 순매수 {df_fix.iloc[0][0]}")
except Exception as e:
    print(f"   결과: 💥 에러 발생! ({e})")