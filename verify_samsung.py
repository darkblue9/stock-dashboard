import requests
import pandas as pd
from io import StringIO

# 삼성전자
code = "005930"
url = f"https://finance.naver.com/item/frgn.naver?code={code}"

print(f"🕵️ [최종 확인] 삼성전자({code}) 진짜 데이터 채굴 시작...\n")

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

try:
    res = requests.get(url, headers=headers)
    res.raise_for_status()

    # 테이블 가져오기
    dfs = pd.read_html(StringIO(res.text), attrs={"class": "type2"}, flavor='lxml')
    
    if len(dfs) > 1:
        df = dfs[1] # 두 번째 테이블이 타겟
        
        # 1. 텅 빈 줄(NaN) 제거 (이게 중요!)
        # ('날짜', '날짜') 컬럼이 비어있는 행을 날려버림
        df = df.dropna(subset=[('날짜', '날짜')])
        
        # 2. 데이터가 진짜 있는지 확인
        if df.empty:
            print("❌ 데이터 없음 (빈 껍데기)")
        else:
            print(f"🎉 성공! 총 {len(df)}일치 데이터 확보함!")
            print("------------------------------------------------------")
            
            # 3. 보기 좋게 정리해서 출력
            # 2단 컬럼을 1단으로 단순화
            clean_df = pd.DataFrame()
            clean_df['날짜'] = df[('날짜', '날짜')]
            clean_df['종가'] = df[('종가', '종가')]
            clean_df['외국인순매수'] = df[('외국인', '순매매량')]
            clean_df['기관순매수'] = df[('기관', '순매매량')]
            
            # 최신 5일치 출력
            print(clean_df.head(5).to_string(index=False))
            print("------------------------------------------------------")
            
            # 오늘/어제 날짜 있는지 확인
            print("\n🔍 최신 날짜 데이터 확인:")
            first_row = clean_df.iloc[0]
            print(f"   👉 날짜: {first_row['날짜']}")
            print(f"   👉 외국인: {first_row['외국인순매수']}")
            print(f"   👉 기관: {first_row['기관순매수']}")

    else:
        print("❌ 테이블 못 찾음")

except Exception as e:
    print(f"❌ 에러: {e}")