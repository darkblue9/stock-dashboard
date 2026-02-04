import requests
import pandas as pd
from bs4 import BeautifulSoup

# 삼성전자(005930) 투자자별 매매동향 페이지 (네이버 금융)
code = "005930"
url = f"https://finance.naver.com/item/frgn.naver?code={code}"

print(f"🕵️ [직접 접속] 네이버 금융 웹페이지 뚫는 중... ({url})")

# ★ 핵심: 우리는 파이썬이 아니라 '크롬'입니다 라고 거짓말 하기 (헤더 위장)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com/",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
}

try:
    # 1. 요청 보내기
    response = requests.get(url, headers=headers)
    response.raise_for_status() # 404나 500 에러면 즉시 중단
    print("✅ 접속 성공! (HTML 데이터 받아옴)")

    # 2. HTML 통째로 파싱 (pandas가 표를 자동으로 찾음)
    # 네이버 페이지에는 테이블이 여러 개 있는데, 그 중 '날짜'가 있는 3번째 테이블이 타겟임
    dfs = pd.read_html(response.text, attrs={"class": "type2"}, flavor='lxml')
    
    if len(dfs) > 1:
        # 투자자별 매매동향 테이블
        df = dfs[1] 
        
        # 데이터 정리 (결측치 제거)
        df = df.dropna(subset=['날짜'])
        
        print(f"\n📊 [데이터 획득 성공] 총 {len(df)}일치 데이터")
        print("--------------------------------------------------")
        print(df.head(5)) # 최신 5일치 출력
        print("--------------------------------------------------")
        
        # 오늘 날짜나 1월 29일 데이터 확인
        target_row = df[df['날짜'] == '2026.01.29']
        if not target_row.empty:
            print("\n🎯 2026.01.29 데이터 찾음!")
            print(target_row)
        else:
            print("\n⚠️ 2026.01.29 데이터가 화면엔 안 보임 (더 과거 데이터일 수 있음)")
            
    else:
        print("❌ 실패: 표(Table)를 못 찾았음.")

except Exception as e:
    print(f"\n❌ 에러 발생: {e}")