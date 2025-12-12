import os
import libsql_client
from sqlalchemy import create_engine, text

# --- 환경변수 로드 ---
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

print("="*50)
print("🩺 [DB 접속 진단 키트 v3.0] 가동")
print("="*50)

if not raw_url or not token:
    print("❌ [치명적] 환경변수(Secrets)가 비어있음!")
    exit(1)

# 1. URL 세탁 (https:// 제거 -> 도메인만 남김)
# 예: mystocks.turso.io
clean_host = raw_url.replace("libsql://", "").replace("wss://", "").replace("https://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
if "?" in clean_host: clean_host = clean_host.split("?")[0]

print(f"🔹 타겟 호스트: {clean_host}")

# ---------------------------------------------------------
# [TEST 1] 드라이버 직접 접속 (기준점)
# ---------------------------------------------------------
print("\n🔍 [TEST 1] 드라이버 직접 접속...")
try:
    # 성공했던 방식 그대로!
    client = libsql_client.create_client_sync(url=f"https://{clean_host}", auth_token=token)
    rs = client.execute("SELECT 1")
    print(f"   ✅ 성공! (이게 되면 계정은 문제없음)")
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    exit(1)

# ---------------------------------------------------------
# [TEST 2] SQLAlchemy 엔진 접속 (여기가 핵심!)
# ---------------------------------------------------------
print("\n🔍 [TEST 2] SQLAlchemy 엔진 접속 시도...")
try:
    # [전략] URL에는 주소만! 토큰은 connect_args 주머니에 담기!
    
    # 1. URL: 오직 위치만 적음 (토큰 없음)
    sa_url = f"sqlite+libsql://{clean_host}/?secure=true"
    
    # 2. Args: 토큰은 따로 전달 (이름표: auth_token)
    # 아까 'url' 키워드 에러는 여기서 url을 또 줘서 생긴 거임. 이번엔 토큰만 줌.
    engine_args = {
        "auth_token": token 
    }
    
    print(f"   👉 URL: {sa_url}")
    print(f"   👉 Args: {{'auth_token': '******'}} (토큰 별도 주입)")

    engine = create_engine(sa_url, connect_args=engine_args)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        print(f"   ✅ 성공! 응답값: {result[0]}")
        
    print("\n🎉 [해결] 드디어 문이 열렸다! 이 방식을 쓰면 된다.")
    
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    # 만약 'auth_token'도 아니라면 'authToken'일 수도 있음 (힌트 제공용)
    if "unexpected keyword argument" in str(e):
        print("   👉 힌트: 변수명 문제일 수 있음.")
    exit(1)
