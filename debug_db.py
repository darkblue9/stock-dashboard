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
print("\n🔍 [TEST 1] 드라이버 직접 접속 (재확인)...")
try:
    # 성공했던 방식 그대로!
    client = libsql_client.create_client_sync(url=f"https://{clean_host}", auth_token=token)
    rs = client.execute("SELECT 1")
    print(f"   ✅ 성공! (이게 되면 계정은 문제없음)")
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    exit(1)

# ---------------------------------------------------------
# [TEST 2] SQLAlchemy 엔진 접속 (이게 목표!)
# ---------------------------------------------------------
print("\n🔍 [TEST 2] SQLAlchemy 엔진 접속 시도...")
try:
    # [핵심 변경사항]
    # 1. URL에는 오직 '프로토콜'과 '주소'만 넣는다. (토큰 X, 파라미터 X)
    sa_url = f"sqlite+libsql://{clean_host}/?secure=true"
    
    # 2. 토큰은 connect_args라는 별도 주머니에 'auth_token'이라는 이름으로 넣는다.
    # (TEST 1에서 성공한 변수명과 똑같이 맞춤!)
    engine_args = {
        "auth_token": token 
    }
    
    engine = create_engine(sa_url, connect_args=engine_args)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        print(f"   ✅ 성공! 응답값: {result[0]}")
        
    print("\n🎉 [해결] 드디어 문이 열렸다! 이 코드를 daily_scrap.py에 적용하면 됨.")
    
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    print("   👉 로그를 자세히 보여줘. 변수명 문제일 가능성이 99%야.")
    exit(1)
