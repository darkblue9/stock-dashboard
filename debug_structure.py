import requests
import pandas as pd
from io import StringIO

code = "005930" # 삼성전자
url = f"https://finance.naver.com/item/frgn.naver?code={code}"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"
}

print("🕵️ 구조 확인 시작...")
res = requests.get(url, headers=headers)

# FutureWarning 해결을 위해 StringIO 사용
dfs = pd.read_html(StringIO(res.text), attrs={"class": "type2"}, flavor='lxml')

print(f"✅ 찾은 테이블 개수: {len(dfs)}")

if len(dfs) > 0:
    # 우리가 원하는 건 보통 두 번째 테이블(인덱스 1)에 있음
    # 하지만 확실히 하기 위해 발견된 모든 테이블의 '헤더'를 찍어보자
    for i, df in enumerate(dfs):
        print(f"\n[테이블 {i}] --------------------------------")
        print("👉 컬럼 이름들:", df.columns.tolist())
        print("👉 데이터 예시(첫줄):")
        print(df.head(1))
else:
    print("❌ 테이블을 하나도 못 찾음 (HTML 구조가 바뀐듯)")