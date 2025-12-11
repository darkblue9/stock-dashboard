import os
import libsql_client
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# --- 환경변수 로드 ---
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

print("="*50)
print("🩺 [DB 접속 진단 키트] 가동 시작")
print("="*50)

# 0. 기초 데이터 확인
if not raw_url or not token:
    print("❌ [치명적] 환경변수(Secrets)가 비어있음!")
    exit(1)

print(f"🔹 원본 URL: {raw_url}")
print(f"🔹 토큰 길이: {len(token)} (정상범위: 200자 이상)")

# URL 정리 (https://...turso.io 형태로 통일)
base_host = raw_url.replace("libsql://", "").replace("wss://", "").replace("https://", "")
if "/" in base_host: base_host = base_host.split("/")[0]
if "?" in base_host: base_host = base_host.split("?")[0]

https_url = f"https://{base_host}"
print(f"🔹 타겟 호스트: {base_host}")

# ---------------------------------------------------------
# [TEST 1] 맨손 검사 (libsql_client 직접 사용)
# 목적: ID/PW가 진짜 맞는지 확인 (가장 확실함)
# ---------------------------------------------------------
print("\n🔍 [TEST 1] 드라이버 직접 접속 시도...")
try:
    # Turso는 HTTP 모드(https://)를 가장 좋아함
    client = libsql_client.create_client_sync(url=https_url, auth_token=token)
    rs = client.execute("SELECT 1 as val")
    print(f"   ✅ 성공! 응답값: {rs.rows[0]}")
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    print("   👉 결론: 계정 정보(URL/토큰) 자체가 틀렸거나, 방화벽 문제임.")
    exit(1) # 여기서 안 되면 뒤에는 볼 것도 없음

# ---------------------------------------------------------
# [TEST 2] 장비 검사 (SQLAlchemy 연동)
# 목적: 네 코드에서 쓰는 방식이 먹히는지 확인
# ---------------------------------------------------------
print("\n🔍 [TEST 2] SQLAlchemy 엔진 접속 시도...")
try:
    # 1. 토큰 인코딩
    encoded_token = quote_plus(token)
    
    # 2. 커넥션 스트링 조립 (가장 표준적인 방식)
    # sqlite+libsql://:토큰@호스트/?secure=true
    sa_url = f"sqlite+libsql://:{encoded_token}@{base_host}/?secure=true"
    
    # 3. 접속 테스트
    engine = create_engine(sa_url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        print(f"   ✅ 성공! 응답값: {result[0]}")
        
    print("\n🎉 [진단 완료] 모든 접속 테스트 통과! 이제 코드 합치면 됨.")
    
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    print("   👉 결론: ID/PW는 맞는데, SQLAlchemy 연결 문자열 만드는 방식이 틀림.")
