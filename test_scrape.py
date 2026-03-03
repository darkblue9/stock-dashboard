import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
print('fetching', url)
df = pd.read_html(url)[0]
print(df.head())
print('columns', df.columns)
