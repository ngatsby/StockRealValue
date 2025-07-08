# app.py (혹은 test_all_tables.py 등으로 저장)
import pymysql
import streamlit as st
import time
import pandas as pd # pandas 추가

# --- DB 연결 설정 (Streamlit Secrets에서 로드) ---
# .streamlit/secrets.toml 파일에 다음 형식으로 저장되어 있어야 합니다:
# [mysql]
# host = "quant.mysql.database.azure.com"
# user = "quant@quant" # 실제 Azure DB 서버 이름을 포함한 전체 사용자명 (예: youruser@yourserver)
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

    for attempt in range(3):
        try:
            conn = pymysql.connect(**DB_CONFIG)
            st.success(f"PyMySQL 연결 성공 (시도 {attempt + 1}회).")
            return conn
        except pymysql.err.OperationalError as op_e:
            st.warning(f"PyMySQL 연결 시도 {attempt + 1}회 실패: {op_e}")
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                st.info("PyMySQL 연결 재시도 중...")
            else:
                st.error(f"PyMySQL 연결에 여러 번 실패했습니다. 마지막 오류: {op_e}")
                return None
        except Exception as e:
            st.error(f"예상치 못한 연결 오류 발생: {e}")
            return None
    return None

# --- Streamlit 앱의 메인 로직 ---
st.set_page_config(layout="wide", page_title="Azure MySQL 테이블 데이터 확인")

st.title("🔗 Azure MySQL 테이블 데이터 확인")
st.markdown("데이터베이스에 연결하여 각 테이블의 상위 5개 행을 조회합니다.")

conn = None
try:
    conn = get_safe_pymysql_connection()

    if conn:
        st.subheader("✅ 데이터베이스 연결 상태: 성공")
        
        target_tables = ['kor_ticker', 'kor_fs', 'kor_value', 'kor_price']
        
        for table_name in target_tables:
            st.markdown("---")
            st.subheader(f"📊 `{table_name}` 테이블 상위 5줄 조회")
            
            try:
                # pandas를 사용하여 쿼리 결과를 DataFrame으로 바로 가져옵니다.
                # 이는 PyMySQL Cursor를 직접 사용하는 것보다 편리하고, Streamlit display에 더 적합합니다.
                query = f"SELECT * FROM `{table_name}` LIMIT 5"
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    st.dataframe(df, use_container_width=True)
                    st.write(f"총 `{len(df)}`개의 행을 가져왔습니다 (상위 5개).")
                else:
                    st.warning(f"경고: `{table_name}` 테이블에 데이터가 없거나, 상위 5줄을 가져오지 못했습니다.")
            except Exception as e:
                st.error(f"`{table_name}` 테이블 조회 오류: {e}")
                st.info("테이블 이름이 정확한지, 그리고 해당 테이블에 접근 권한이 있는지 확인해주세요.")

    else:
        st.subheader("❌ 데이터베이스 연결 상태: 실패")
        st.error("데이터베이스에 연결할 수 없습니다. 위의 로그를 확인하고 DB 설정 및 방화벽을 점검해주세요.")

except Exception as e:
    st.error(f"앱 실행 중 예상치 못한 오류 발생: {e}")
finally:
    if conn and conn.open:
        conn.close()
        st.info("PyMySQL 연결이 닫혔습니다.")

st.sidebar.info("이 앱은 Azure MySQL 테이블 데이터 존재 및 형식 확인을 위한 것입니다.")
st.sidebar.markdown("---")
st.sidebar.markdown(f"© {datetime.datetime.now().year} Quant Analyzer Test")
