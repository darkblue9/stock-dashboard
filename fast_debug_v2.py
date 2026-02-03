from pykrx import stock
import pandas as pd

# 테스트 날짜 (아까 성공했던 날 vs 문제의 날)
dates = ["20260129", "20260202"]
markets = ["KOSPI", "ALL"] # 범인은 이 안에 있다!

print("🚀 [진단 시작] 'KOSPI' vs 'ALL' 범인 색출", flush=True)

for date in dates:
    print(f"\n📅 날짜: {date} --------------------------")
    for mkt in markets:
        try:
            # 아까 성공했던 그 함수 그대로 사용!
            df = stock.get_market_net_purchases_of_equities_by_ticker(date, date, mkt, investor="foreign")
            
            if df.empty:
                print(f"   ❌ 시장='{mkt}': 데이터 없음 (0건)")
            else:
                # 데이터가 있으면 첫 번째 종목의 순매수 금액 확인
                val = 0
                if '순매수거래대금' in df.columns:
                    val = df.iloc[0]['순매수거래대금']
                elif len(df.columns) > 1:
                     val = df.iloc[0, 1] # 두번째 컬럼 강제 참조
                
                print(f"   ✅ 시장='{mkt}': {len(df)}건 조회 성공! (예시값: {val})")
                
        except Exception as e:
            print(f"   💥 시장='{mkt}': 에러 발생 ({e})")

print("\n🏁 진단 종료.")