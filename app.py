import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql # Required by SQLAlchemy for mysql+pymysql
import statsmodels.api as sm
from scipy.stats import zscore
import matplotlib.pyplot as plt # Although not used for display here, kept for completeness

@st.cache_resource
def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for your Azure MySQL database.
    Caches the engine for performance in Streamlit sessions.
    """
    if not st.secrets.get("mysql"):
        st.error("DB connection secrets not found. Please configure .streamlit/secrets.toml.")
        return None

    db_config = st.secrets["mysql"]
    
    try:
        connection_string = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:3306/{db_config['database']}"
        )
        
        engine = create_engine(
            connection_string,
            pool_recycle=300,  # Recycle connections after 300 seconds (5 minutes)
            pool_pre_ping=True # Test connections before use
        )
        st.success("Database engine created successfully.")
        return engine
    except Exception as e:
        st.error(f"Error creating database engine: {e}")
        st.info("Please check your .streamlit/secrets.toml for correct Azure MySQL credentials and server firewall rules.")
        return None

# --- 2. Data Loading Functions ---
@st.cache_data(ttl=3600) # Cache data for 1 hour
def load_all_data(engine):
    """
    Loads all necessary dataframes from the MySQL database.
    Returns a dictionary of dataframes.
    """
    data = {}
    
    if engine is None:
        st.error("Cannot load data: Database engine is not initialized.")
        return data

    st.info("Loading `kor_ticker`...")
    try:
        data['ticker_list'] = pd.read_sql("""
            SELECT *
            FROM kor_ticker
            WHERE 기준일 = (SELECT MAX(기준일) FROM kor_ticker)
            AND 종목구분 = '보통주';
        """, con=engine)
        st.success(f"Loaded {len(data['ticker_list'])} ordinary tickers.")
    except Exception as e:
        st.error(f"Failed to load `kor_ticker`: {e}")
        data['ticker_list'] = pd.DataFrame()

    st.info("Loading `kor_fs` (Financial Statements)...")
    try:
        # Assuming '공시구분' = 'q' for quarterly data
        data['fs_list'] = pd.read_sql("""
            SELECT *
            FROM kor_fs
            WHERE 계정 IN ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
            AND 공시구분 = 'q';
        """, con=engine)
        st.success(f"Loaded {len(data['fs_list'])} financial statement entries.")
    except Exception as e:
        st.error(f"Failed to load `kor_fs`: {e}")
        data['fs_list'] = pd.DataFrame()

    st.info("Loading `kor_value` (Valuation Data)...")
    try:
        data['value_list'] = pd.read_sql("""
            SELECT *
            FROM kor_value
            WHERE 기준일 = (SELECT MAX(기준일) FROM kor_value);
        """, con=engine)
        st.success(f"Loaded {len(data['value_list'])} valuation entries.")
    except Exception as e:
        st.error(f"Failed to load `kor_value`: {e}")
        data['value_list'] = pd.DataFrame()

    st.info("Loading `kor_price` (Last 1 year of prices)...")
    try:
        # Changed '날짜' to '기준일' based on common table naming and previous discussions
        data['price_list'] = pd.read_sql("""
            SELECT 기준일 AS 날짜, 종가, 종목코드
            FROM kor_price
            WHERE 기준일 >= (SELECT MAX(기준일) - INTERVAL 1 YEAR FROM kor_price);
        """, con=engine)
        st.success(f"Loaded {len(data['price_list'])} price entries.")
    except Exception as e:
        st.error(f"Failed to load `kor_price`: {e}")
        data['price_list'] = pd.DataFrame()

    st.info("Loading `kor_sector`...")
    try:
        data['sector_list'] = pd.read_sql("""
            SELECT *
            FROM kor_sector
            WHERE 기준일 = (SELECT MAX(기준일) FROM kor_sector);
        """, con=engine)
        st.success(f"Loaded {len(data['sector_list'])} sector entries.")
    except Exception as e:
        st.error(f"Failed to load `kor_sector`: {e}")
        data['sector_list'] = pd.DataFrame()
        
    return data

# --- 3. Streamlit Application ---

st.set_page_config(layout="wide", page_title="재무 및 가치 데이터 로더")

st.title("📊 데이터베이스에서 모든 데이터 불러오기")
st.markdown("Azure Cloud MySQL에서 `kor_ticker`, `kor_fs`, `kor_value`, `kor_price`, `kor_sector` 테이블의 데이터를 불러와 표시합니다.")

# Get the database engine
engine_instance = get_db_engine()

# Load all dataframes
loaded_data = load_all_data(engine_instance)

# Ensure the engine connection pool is closed after loading data (good practice)
if engine_instance:
    engine_instance.dispose()

st.subheader("데이터 로딩 결과:")

if not all(df.empty for df in loaded_data.values()):
    st.success("모든 데이터 로딩 시도 완료!")
else:
    st.warning("일부 또는 모든 데이터프레임이 비어 있습니다. 위에 표시된 오류 메시지를 확인하세요.")

# Display head of each loaded DataFrame (for debugging/verification)
for df_name, df in loaded_data.items():
    if not df.empty:
        st.write(f"### {df_name} ({len(df)} rows)")
        st.dataframe(df.head())
    else:
        st.write(f"### {df_name} (No data loaded or DataFrame is empty)")

# Example of how you might start processing (similar to your original snippet)
# This part is just for demonstration; you'd integrate it into your full analysis.
st.subheader("예시: Ticker와 Value 데이터 병합")
if 'ticker_list' in loaded_data and 'value_list' in loaded_data and \
   not loaded_data['ticker_list'].empty and not loaded_data['value_list'].empty:
    
    value_list_df = loaded_data['value_list'].copy()
    # Apply your value cleaning logic
    value_list_df.loc[value_list_df['값'] <= 0, '값'] = np.nan
    value_pivot = value_list_df.pivot(index='종목코드', columns='지표', values='값')
    
    # Ensure '종목코드' is string type for merging if needed (good practice for IDs)
    loaded_data['ticker_list']['종목코드'] = loaded_data['ticker_list']['종목코드'].astype(str)
    value_pivot.index = value_pivot.index.astype(str)

    data_bind = loaded_data['ticker_list'][['종목코드', '종목명', '종가']].merge(
                                                    value_pivot,
                                                    how='left',
                                                    on='종목코드')
    st.dataframe(data_bind.head())
else:
    st.info("`ticker_list` 또는 `value_list`가 없어 병합 예시를 표시할 수 없습니다.")


st.sidebar.markdown("---")
st.sidebar.info("이 앱은 Azure MySQL에서 다양한 주식 데이터를 불러오는 예시입니다.")
st.sidebar.markdown("© 2025 Data Loader")
