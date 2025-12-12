import os
import libsql_client
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# --- 환경변수 로드 ---
raw_url = os.environ.get("TURSO_DB_URL", "").strip()
token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

print("="*50)
print("🩺 [DB 접속 진단 키트 v2.0] 가동 시작")
print("="*50)

if not raw_url or not token:
    print("❌ [치명적] 환경변수(Secrets)가 비어있음!")
    exit(1)

# URL 정리 (https://...turso.io -> ...turso.io)
# 도메인만 남겨야 SQLAlchemy가 주소를 조립할 수 있음
clean_host = raw_url.replace("libsql://", "").replace("wss://", "").replace("https://", "")
if "/" in clean_host: clean_host = clean_host.split("/")[0]
if "?" in clean_host: clean_host = clean_host.split("?")[0]

print(f"🔹 타겟 호스트: {clean_host}")

# ---------------------------------------------------------
# [TEST 1] 맨손 검사 (libsql_client 직접 사용)
# 지난번에 성공했으니, 이번에도 무조건 성공해야 함 (기준점)
# ---------------------------------------------------------
print("\n🔍 [TEST 1] 드라이버 직접 접속 시도...")
try:
    # Turso는 https 프로토콜을 선호함
    https_url = f"https://{clean_host}"
    client = libsql_client.create_client_sync(url=https_url, auth_token=token)
    rs = client.execute("SELECT 1 as val")
    print(f"   ✅ 성공! 응답값: {rs.rows[0]}")
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    exit(1)

# ---------------------------------------------------------
# [TEST 2] SQLAlchemy 엔진 접속 시도
# 전략: 토큰을 '비밀번호' 자리에 넣기 (가장 표준적인 방식)
# ---------------------------------------------------------
print("\n🔍 [TEST 2] SQLAlchemy 엔진 접속 시도...")
try:
    # 1. 토큰 인코딩 (특수문자 방지)
    encoded_token = quote_plus(token)
    
    # 2. 커넥션 스트링 조립
    # 문법: sqlite+libsql://:비밀번호@호스트/?secure=true
    # 설명: ID 자리는 비우고(:), 비밀번호 자리에 토큰을 넣음
    # 주의: connect_args는 일절 사용하지 않음 (충돌 방지)
    sa_url = f"sqlite+libsql://:{encoded_token}@{clean_host}?secure=true"
    
    print(f"   👉 접속 URL 생성 완료 (토큰 포함됨)")
    
    # 3. 엔진 생성
    engine = create_engine(sa_url)
    
    # 4. 접속 테스트
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).fetchone()
        print(f"   ✅ 성공! 응답값: {result[0]}")
        
    print("\n🎉 [진단 완료] 드디어 SQLAlchemy 문이 열렸다!")
    print("이제 이 코드를 daily_scrap.py에 복사하면 됨.")
    
except Exception as e:
    print(f"   ❌ 실패! (원인: {e})")
    print("   👉 여전히 연결 문자열 문제임.")
    exit(1)
