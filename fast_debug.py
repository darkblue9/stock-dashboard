from pykrx import stock
import pandas as pd

# 테스트할 날짜들 (성공했던 날 vs 실패한 날)
dates_to_test = ["20260129", "20260202"]

# 테스트할 파라미터들
investors = ["foreign", "외국인"]

print("🚀 [초고속 진단] 삼성전자(005930) 딱 한 놈만 패봅니다.", flush=True)

for date in dates_to_test:
    print(f"\n📅 날짜: {date} 확인 중...")
    
    for inv in investors:
        try:
            # 전체 종목(ALL) 말고, 삼성전자 하나만 콕 집어서 가져오는 함수 사용 (훨씬 빠름/정확함)
            # 함수: stock.get_market_net_purchases_of_equities_by_date(from, to, ticker, investor)
            df = stock.get_market_net_purchases_of_equities_by_date(date, date, "005930", investor=inv)
            
            if not df.empty:
                val = df.iloc[0]['순매수거래대금'] if '순매수거래대금' in df.columns else "컬럼못찾음"
                if val == 0:
                    print(f"   ❌ [실패] {inv}: 데이터는 왔는데 값이 0임 (장 안 열림 or 데이터 누락)")
                else:
                    print(f"   ✅ [성공!] {inv}: {val} (드디어 찾았다!)")
                    print(f"      👉 컬럼명: {df.columns.tolist()}")
            else:
                print(f"   ❌ [실패] {inv}: 데이터 없음 (Empty)")
                
        except Exception as e:
            print(f"   💥 [에러] {inv}: {e}")

print("\n🏁 진단 종료.")