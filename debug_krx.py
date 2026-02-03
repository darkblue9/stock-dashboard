from pykrx import stock
import pandas as pd

# 2월 2일 데이터로 고정 (네가 확인하려는 그 날짜!)
date = "20260129"
investor = "foreign" # 외국인 형님들

print(f"🔍 [{date}] 외국인 수급 데이터 엑스레이 촬영 시작...", flush=True)

try:
    # 1. 데이터 호출 (날짜 두 번 넣는 건 이제 기본!)
    df = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", investor=investor)
    
    if df.empty:
        print("❌ 데이터가 텅 비었습니다! (장 안 열린 날? or 파라미터 에러?)")
    else:
        print(f"✅ 데이터 {len(df)}건 도착! 내용을 뜯어봅시다.")
        print("-" * 40)
        
        # [핵심 1] 컬럼 이름표 확인
        print(f"📋 컬럼 이름표(Columns): {df.columns.tolist()}")
        
        # [핵심 2] 첫 번째 종목 뜯어보기
        print("-" * 40)
        first_row = df.iloc[0]
        print(f"📊 첫 번째 종목({first_row.name})의 실제 값:")
        
        for i, col in enumerate(df.columns):
            val = first_row[col]
            print(f"   [{i}] {col} ==> {val} (타입: {type(val)})")
            
        print("-" * 40)
        
        # [핵심 3] 인덱스(종목코드) 확인
        print(f"🔑 인덱스(티커) 확인: '{df.index[0]}' (타입: {type(df.index[0])})")
        print("   (참고: 이게 문자열(str)이어야 병합이 잘 됨!)")

except Exception as e:
    print(f"💥 에러 발생: {e}")