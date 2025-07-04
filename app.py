# test_db_connection.py
import pymysql
import streamlit as st
import time

# --- DB 연결 설정 (Streamlit Secrets에서 로드) ---
# .streamlit/secrets.toml 파일에 다음 형식으로 저장되어 있어야 합니다:
# [mysql]
# host = "quant.mysql.database.azure.com"
# user = "quant"
# password = "a303737!"
# database = "stock_db"
# charset = "utf8"
# connect_timeout = 10
# read_timeout = 10
# write_timeout = 10

# Streamlit 환경에서 DB_CONFIG를 안전하게 불러오는 함수
def get_db_config():
    if st.secrets.get("mysql"):
        config = {
            'host': st.secrets["mysql"]["host"],
            'user': st.secrets["mysql"]["user"],
            'password': st.secrets["mysql"]["password"],
            'database': st.secrets["mysql"]["database"],
            'charset': st.secrets["mysql"]["charset"],
            'connect_timeout': st.secrets["mysql"].get("connect_timeout", 10),
            'read_timeout': st.secrets["mysql"].get("read_timeout", 10),
            'write_timeout': st.secrets["mysql"].get("write_timeout", 10),
        }
        return config
    else:
        st.error("DB 연결 정보(secrets.toml)를 찾을 수 없습니다. 설정 파일을 확인해주세요.")
        return None

DB_CONFIG = get_db_config()

# --- PyMySQL 연결을 안정적으로 만드는 헬퍼 함수 ---
@st.cache_resource # Streamlit에서 리소스(DB 연결) 캐싱
def get_safe_pymysql_connection():
    """
    안정적인 PyMySQL 연결을 시도하고 반환합니다.
    연결 실패 시 여러 번 재시도합니다.
    """
    if not DB_CONFIG:
        return None

    for attempt in range(3): # 테스트 목적이므로 재시도 횟수 3회로 줄임
        try:
            conn = pymysql.connect(**DB_CONFIG)
            st.success(f"PyMySQL 연결 성공 (시도 {attempt + 1}회).")
            return conn
        except pymysql.err.OperationalError as op_e:
            st.warning(f"PyMySQL 연결 시도 {attempt + 1}회 실패: {op_e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1)) # 재시도 전 대기 시간
                st.info("PyMySQL 연결 재시도 중...")
            else:
                st.error(f"PyMySQL 연결에 여러 번 실패했습니다. 마지막 오류: {op_e}")
                return None
        except Exception as e:
            st.error(f"예상치 못한 연결 오류 발생: {e}")
            return None
    return None

# --- Streamlit 앱의 메인 로직 ---
st.set_page_config(layout="wide", page_title="Azure MySQL 연결 테스트")

st.title("🔗 Azure MySQL 연결 및 데이터 조회 테스트")
st.markdown("이 앱은 Azure MySQL 데이터베이스에 연결하고, 주요 테이블의 데이터 존재 여부를 확인합니다.")

conn = None
try:
    conn = get_safe_pymysql_connection()

    if conn:
        st.subheader("✅ 데이터베이스 연결 상태: 성공")
        cursor = conn.cursor()

        # 1. kor_ticker 테이블 행 개수 확인
        st.markdown("---")
        st.subheader("📊 `kor_ticker` 테이블 확인")
        try:
            cursor.execute("SELECT COUNT(*) FROM kor_ticker")
            ticker_count = cursor.fetchone()[0]
            st.write(f"`kor_ticker` 테이블에 총 **{ticker_count}**개의 종목이 있습니다.")
            if ticker_count == 0:
                st.warning("경고: `kor_ticker` 테이블이 비어 있습니다. 종목 데이터가 필요합니다.")
        except pymysql.Error as e:
            st.error(f"`kor_ticker` 테이블 조회 오류: {e}")

        # 2. kor_fs 테이블 행 개수 확인
        st.markdown("---")
        st.subheader("📈 `kor_fs` 테이블 확인")
        try:
            cursor.execute("SELECT COUNT(*) FROM kor_fs")
            fs_count = cursor.fetchone()[0]
            st.write(f"`kor_fs` 테이블에 총 **{fs_count}**개의 재무 데이터 행이 있습니다.")
            if fs_count == 0:
                st.warning("경고: `kor_fs` 테이블이 비어 있습니다. 재무 데이터가 필요합니다.")
        except pymysql.Error as e:
            st.error(f"`kor_fs` 테이블 조회 오류: {e}")

        # 3. 특정 종목 데이터 조회 테스트 (삼성전자 예시)
        st.markdown("---")
        st.subheader("🔍 특정 종목 데이터 조회 테스트 (예: 삼성전자 005930)")
        test_stock_code = '005930'
        
        # kor_ticker에서 종가 가져오기
        try:
            query_price = f"SELECT 종가 FROM kor_ticker WHERE 종목코드 = '{test_stock_code}' LIMIT 1"
            cursor.execute(query_price)
            price_result = cursor.fetchone()
            if price_result and price_result[0] is not None:
                st.write(f"종목코드 `{test_stock_code}`의 종가: **{price_result[0]:,.0f}** 원")
            else:
                st.warning(f"경고: 종목코드 `{test_stock_code}`의 종가 데이터를 찾을 수 없습니다.")
        except pymysql.Error as e:
            st.error(f"종가 조회 오류: {e}")

        # kor_fs에서 당기순이익 가져오기 (가장 최근 연도)
        try:
            # 가장 최근 기준일 찾기 (kor_fs 테이블에서)
            query_latest_date = f"SELECT MAX(기준일) FROM kor_fs WHERE 종목코드 = '{test_stock_code}' AND 공시구분 = 'y'"
            cursor.execute(query_latest_date)
            latest_date_result = cursor.fetchone()
            latest_base_date = None
            if latest_date_result and latest_date_result[0]:
                latest_base_date = latest_date_result[0].strftime('%Y-%m-%d')
                st.info(f"종목코드 `{test_stock_code}`의 최근 연간 재무 기준일: `{latest_base_date}`")
            else:
                st.warning(f"경고: 종목코드 `{test_stock_code}`에 대한 연간 재무 데이터의 최근 기준일을 찾을 수 없습니다.")

            if latest_base_date:
                query_net_income = f"SELECT 값 FROM kor_fs WHERE 종목코드 = '{test_stock_code}' AND 기준일 = '{latest_base_date}' AND 계정 = '당기순이익' AND 공시구분 = 'y' LIMIT 1"
                cursor.execute(query_net_income)
                net_income_result = cursor.fetchone()
                if net_income_result and net_income_result[0] is not None:
                    st.write(f"종목코드 `{test_stock_code}`의 최근 당기순이익: **{net_income_result[0]:,.0f}** 억원")
                else:
                    st.warning(f"경고: 종목코드 `{test_stock_code}`의 `{latest_base_date}` 기준 '당기순이익' 데이터를 찾을 수 없습니다.")
            
        except pymysql.Error as e:
            st.error(f"당기순이익 조회 오류: {e}")

    else:
        st.subheader("❌ 데이터베이스 연결 상태: 실패")
        st.error("데이터베이스에 연결할 수 없습니다. 위의 로그를 확인하고 DB 설정 및 방화벽을 점검해주세요.")

except Exception as e:
    st.error(f"앱 실행 중 예상치 못한 오류 발생: {e}")
finally:
    if conn and conn.open:
        conn.close()
        st.info("PyMySQL 연결이 닫혔습니다.")

st.sidebar.info("이 앱은 Azure MySQL 연결 및 데이터 조회 테스트를 위한 것입니다.")
st.sidebar.markdown("---")
st.sidebar.markdown("© 2025 Value Analyzer Test")
