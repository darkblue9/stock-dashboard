import streamlit as st
import pandas as pd
import libsql_experimental as libsql

# -------------------------------------------------------------------
# 1. 페이지 설정
# -------------------------------------------------------------------
st.set_page_config(
    page_title="나의 보물창고",
    page_icon="💰",
    layout="wide"
)

# -------------------------------------------------------------------
# 2. DB 연결 함수
# -------------------------------------------------------------------
def get_connection():
    url = st.secrets["db"]["url"]
    auth_token = st.secrets["db"]["auth_token"]
    return libsql.connect("pykrx.db", sync_url=url, auth_token=auth_token)

# -------------------------------------------------------------------
# 3. 데이터 로드 (오늘 & 어제 동시 로딩)
# -------------------------------------------------------------------
@st.cache_data(ttl=300) # 5분마다 갱신
def load_latest_two_days():
    conn = get_connection()
    
    # 날짜 목록 가져오기 (내림차순)
    date_query = "SELECT DISTINCT 날짜 FROM Npaystocks ORDER BY 날짜 DESC LIMIT 2"
    date_rows = conn.execute(date_query).fetchall()
    
    if not date_rows:
        return None, None, None, None
        
    dates = [str(row[0]) for row in date_rows]
    latest_date = dates[0]  # 오늘 (또는 가장 최신)
    prev_date = dates[1] if len(dates) > 1 else None # 어제 (또는 그 전)

    # 오늘 데이터 가져오기
    query_latest = f"SELECT * FROM Npaystocks WHERE 날짜 = '{latest_date}'"
    df_latest = pd.read_sql(query_latest, conn) # pandas read_sql 사용 (더 편함)

    # 어제 데이터 가져오기
    df_prev = pd.DataFrame()
    if prev_date:
        query_prev = f"SELECT * FROM Npaystocks WHERE 날짜 = '{prev_date}'"
        df_prev = pd.read_sql(query_prev, conn)
        
    return df_latest, latest_date, df_prev, prev_date

# -------------------------------------------------------------------
# 4. 데이터 전처리 (방탄 조끼)
# -------------------------------------------------------------------
def process_data(df):
    if df.empty:
        return df

    # 숫자 변환 & 결측치 처리
    numeric_cols = ['현재가', '등락률', '거래량', '전일거래량', '시가', '고가', '저가', '외국인순매수', '기관순매수']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # 0 나누기 방지
    if '전일거래량' in df.columns:
        df['전일거래량'] = df['전일거래량'].replace(0, 1)

    # 파생 지표
    df['거래량비율'] = df['거래량'] / df['전일거래량']
    
    return df

# -------------------------------------------------------------------
# 5. 메인 화면
# -------------------------------------------------------------------
def main():
    st.title("💰 주식 보물창고 (Ver 2.2)")

    try:
        # 데이터 2일치 한 번에 로딩
        df_today, date_today, df_yesterday, date_yesterday = load_latest_two_days()
        
        if df_today is None:
            st.warning("데이터가 없습니다.")
            return

        # 전처리
        df_today = process_data(df_today)
        if not df_yesterday.empty:
            df_yesterday = process_data(df_yesterday)

    except Exception as e:
        st.error(f"오류 발생: {e}")
        return

    # 상단 정보 바
    st.info(f"📊 **오늘 데이터:** {date_today} (장중) | 🔙 **어제 데이터:** {date_yesterday if date_yesterday else '없음'}")

    # 탭 구성 (원하는 대로 탭 추가!)
    tabs = st.tabs([
        "🔥 돈냄새 (오늘)", 
        "🐜 개미털기 (오늘)", 
        "🐜 개미털기 (어제)", 
        "🤝 쌍끌이 (어제)",
        "📋 전체 목록"
    ])

    # ----------------------------------------------------------------
    # TAB 1: 돈냄새 (오늘) - 실시간 단타용
    # ----------------------------------------------------------------
    with tabs[0]:
        st.markdown(f"### 🔥 오늘({date_today}) 거래량 폭발 종목")
        st.caption("※ 장중 실시간 거래량을 반영합니다.")
        
        df_money = df_today[df_today['거래량비율'] >= 5.0].copy()
        df_money = df_money.sort_values(by='거래량비율', ascending=False)
        
        if df_money.empty:
            st.info("아직 거래량이 터진 종목이 없습니다.")
        else:
            st.dataframe(
                df_money[['종목명', '현재가', '등락률', '거래량', '전일거래량', '거래량비율']],
                column_config={
                    "현재가": st.column_config.NumberColumn(format="%d원"),
                    "등락률": st.column_config.NumberColumn(format="%.2f%%"),
                    "거래량비율": st.column_config.NumberColumn(format="%.1f배"),
                },
                use_container_width=True,
                hide_index=True
            )

    # ----------------------------------------------------------------
    # TAB 2: 개미털기 (오늘) - 장중 추정
    # ----------------------------------------------------------------
    with tabs[1]:
        st.markdown(f"### 🐜 오늘({date_today}) 개미털기 의심 (실시간)")
        st.caption("※ 주의: 장중에는 외국인/기관 수급 데이터가 0으로 잡히거나 부정확할 수 있습니다.")
        
        condition_ant = (df_today['등락률'] < 0) & ((df_today['외국인순매수'] > 0) | (df_today['기관순매수'] > 0))
        df_ant_today = df_today[condition_ant].copy()
        df_ant_today = df_ant_today.sort_values(by='외국인순매수', ascending=False)

        if df_ant_today.empty:
            st.info("오늘 데이터 기준으로는 아직 포착된 종목이 없습니다. (수급 데이터 집계 지연 가능성)")
        else:
            st.dataframe(
                df_ant_today[['종목명', '현재가', '등락률', '외국인순매수', '기관순매수']],
                column_config={
                    "현재가": st.column_config.NumberColumn(format="%d원"),
                    "등락률": st.column_config.NumberColumn(format="%.2f%%"),
                },
                use_container_width=True,
                hide_index=True
            )

    # ----------------------------------------------------------------
    # TAB 3: 개미털기 (어제) - 확정 데이터 (핵심!)
    # ----------------------------------------------------------------
    with tabs[2]:
        if df_yesterday.empty:
            st.warning("어제 데이터가 없습니다.")
        else:
            st.markdown(f"### 🔙 어제({date_yesterday}) 개미털기 확정 (매집 완료)")
            st.caption("※ 어제 가격은 내렸지만 형님들이 몰래 사둔 종목입니다. 오늘 반등하는지 보세요!")
            
            condition_ant_prev = (df_yesterday['등락률'] < 0) & ((df_yesterday['외국인순매수'] > 0) | (df_yesterday['기관순매수'] > 0))
            df_ant_prev = df_yesterday[condition_ant_prev].copy()
            df_ant_prev = df_ant_prev.sort_values(by='외국인순매수', ascending=False)

            if df_ant_prev.empty:
                st.info("어제 조건에 맞는 종목이 없었습니다.")
            else:
                st.dataframe(
                    df_ant_prev[['종목명', '현재가', '등락률', '외국인순매수', '기관순매수']],
                    column_config={
                        "현재가": st.column_config.NumberColumn(format="%d원"),
                        "등락률": st.column_config.NumberColumn(format="%.2f%%"),
                    },
                    use_container_width=True,
                    hide_index=True
                )

    # ----------------------------------------------------------------
    # TAB 4: 쌍끌이 (어제) - 확정 데이터
    # ----------------------------------------------------------------
    with tabs[3]:
        if df_yesterday.empty:
            st.warning("어제 데이터가 없습니다.")
        else:
            st.markdown(f"### 🤝 어제({date_yesterday}) 외국인+기관 쌍끌이")
            
            condition_double = (df_yesterday['외국인순매수'] > 0) & (df_yesterday['기관순매수'] > 0)
            df_double = df_yesterday[condition_double].copy()
            df_double['합산매수'] = df_double['외국인순매수'] + df_double['기관순매수']
            df_double = df_double.sort_values(by='합산매수', ascending=False)

            if df_double.empty:
                st.info("쌍끌이 종목이 없습니다.")
            else:
                st.dataframe(
                    df_double[['종목명', '현재가', '등락률', '외국인순매수', '기관순매수']],
                    use_container_width=True,
                    hide_index=True
                )

    # ----------------------------------------------------------------
    # TAB 5: 전체 데이터
    # ----------------------------------------------------------------
    with tabs[4]:
        st.dataframe(df_today, use_container_width=True)

if __name__ == "__main__":
    main()