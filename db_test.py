import os
import libsql_client

# 1. 환경변수 가져오기
url = os.environ.get("TURSO_DB_URL", "").strip()
token = os.environ.get("TURSO_AUTH_TOKEN", "").strip()

print("=== [진단 모드] 연결 정보 확인 ===")

# 2. URL 확인
if not url:
    print("❌ URL이 비어있습니다!")
    exit(1)
else:
    # URL이 올바른지 눈으로 확인 (보안상 앞부분만 출력해도 됨)
    print(f"👉 URL: {url}") 

# 3. 토큰 확인 (가장 의심스러운 부분)
if not token:
    print("❌ 토큰이 비어있습니다!")
    exit(1)
else:
    # [중요] 토큰의 앞 10자리만 출력해서, 네가 가진 진짜 토큰과 맞는지 비교해봐.
    # 혹시 여기에 URL이 들어가 있거나, 이상한 글자가 없는지 확인!
    print(f"👉 토큰(앞 10자리): {token[:10]}...")
    print(f"👉 토큰 전체 길이: {len(token)}")

# 4. 가장 기본 라이브러리(libsql_client)로 접속 시도
print("\n=== 접속 시도 중... ===")
try:
    # 라이브러리 기본 함수 사용 (SQLAlchemy 아님)
    client = libsql_client.create_client_sync(url=url, auth_token=token)
    
    # 간단한 쿼리 실행
    rs = client.execute("SELECT 1 as test")
    
    print(f"✅ 접속 성공! 결과값: {rs.rows}")
    print("결론: 계정과 토큰은 정상임. 라이브러리 문제였음.")
    
except Exception as e:
    print("❌ 접속 실패 (치명적 오류)")
    print(f"에러 메시지: {e}")
    print("결론: 토큰이 만료됐거나, URL 형식이 Turso가 원하는 게 아님.")
