import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql # Required by SQLAlchemy for MySQL+PyMySQL

# --- 1. Database Connection Configuration ---
# It's crucial to correctly configure your .streamlit/secrets.toml
#
# Example .streamlit/secrets.toml content:
# [mysql]
# host = "your_azure_mysql_server_name.mysql.database.azure.com" # e.g., "quant.mysql.database.azure.com"
# user = "your_username@your_server_name"                         # e.g., "quant@quant" (username@servername)
# password = "your_db_password"
# database = "your_database_name"                                # e.g., "stock_db"
# charset = "utf8mb4" # Use utf8mb4 for broader character support
# connect_timeout = 10
# read_timeout = 10
# write_timeout = 10

@st.cache_resource
def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for your Azure MySQL database.
    Caches the engine for performance in Streamlit.
    """
    if not st.secrets.get("mysql"):
        st.error("DB connection secrets not found. Please configure .streamlit/secrets.toml.")
        return None

    db_config = st.secrets["mysql"]
    
    try:
        # Construct the connection string using details from secrets
        # Azure MySQL usernames typically require 'username@servername' format.
        # Ensure 'user' in secrets.toml is already in this format.
        connection_string = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:3306/{db_config['database']}"
        )
        
        engine = create_engine(
            connection_string,
            pool_recycle=300,  # Recycle connections after 300 seconds (5 minutes)
            pool_pre_ping=True # Test connections before use
        )
        return engine
    except Exception as e:
        st.error(f"Error creating database engine: {e}")
        st.info("Please check your .streamlit/secrets.toml for correct Azure MySQL credentials.")
        return None

# --- 2. Streamlit App Layout and Data Loading ---

st.set_page_config(layout="wide", page_title="전 종목 가치 지표 조회")

st.title("📊 전 종목 최신 가치 지표")
st.markdown("Azure Cloud MySQL 데이터베이스에서 전 종목의 최신 기본 정보와 가치 지표를 불러옵니다.")

# Get the database engine
db_engine = get_db_engine()

if db_engine is None:
    st.stop() # Stop the app if DB connection fails

# --- Data Loading Logic ---
st.info("데이터를 불러오는 중입니다. 잠시만 기다려 주세요...")

try:
    # Load ticker list
    # Assuming 'kor_ticker' stores '종가' as well, or you plan to merge 'kor_price' later.
    # For now, fetching '종가' from kor_ticker as it's often present there.
    ticker_list_query = """
    SELECT 종목코드, 종목명, 종가
    FROM kor_ticker
    WHERE 기준일 = (SELECT MAX(기준일) FROM kor_ticker)
    AND 종목구분 = '보통주';
    """
    ticker_list = pd.read_sql(ticker_list_query, con=db_engine)
    
    if ticker_list.empty:
        st.warning("`kor_ticker` 테이블에서 종목 정보를 찾을 수 없습니다. 데이터가 비어 있거나 '보통주'가 없습니다.")
        st.stop()

    # Load valuation list
    # Assuming 'kor_value' has '종목코드', '기준일', '지표' (e.g., 'PBR', 'PER'), '값'
    value_list_query = """
    SELECT 종목코드, 지표, 값
    FROM kor_value
    WHERE 기준일 = (SELECT MAX(기준일) FROM kor_value);
    """
    value_list = pd.read_sql(value_list_query, con=db_engine)

    if value_list.empty:
        st.warning("`kor_value` 테이블에서 가치 지표를 찾을 수 없습니다. 데이터가 비어 있습니다.")
    
    # Close the engine's connection pool after loading data
    db_engine.dispose()
    st.success("데이터 불러오기 및 DB 연결 종료 완료!")

    # --- Data Processing ---
    if not value_list.empty:
        # Replace non-positive values with NaN for ratios (e.g., PER can be negative)
        # Note: Depending on your data, you might want to adjust this.
        # For simplicity, I'm using your original logic.
        value_list.loc[value_list['값'] <= 0, '값'] = np.nan
        
        # Pivot the value_list to have indicators as columns
        value_pivot = value_list.pivot(index='종목코드', columns='지표', values='값')
    else:
        value_pivot = pd.DataFrame() # Create an empty DataFrame if no value data

    # Merge ticker information with pivoted valuation data
    data_bind = ticker_list[['종목코드', '종목명', '종가']].merge(value_pivot,
                                                        how='left',
                                                        on='종목코드')

    # --- Display Results ---
    st.subheader("📋 전체 종목 가치 지표")
    
    # Optional: Display some statistics or shape
    st.write(f"총 {len(data_bind)}개 종목의 데이터를 불러왔습니다.")

    # Format numeric columns for better display
    numeric_cols = data_bind.select_dtypes(include=np.number).columns
    for col in numeric_cols:
        if col in ['종가']: # Format as integer for price
            data_bind[col] = data_bind[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else None)
        else: # Format other numeric values (like ratios) with 2 decimal places
            data_bind[col] = data_bind[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else None)

    st.dataframe(data_bind, use_container_width=True)

    # --- Download as CSV ---
    @st.cache_data
    def convert_df_to_csv(df):
        # IMPORTANT: Avoid converting to string format for CSV export
        # because the user might want to perform calculations later.
        # Convert the original data_bind BEFORE formatting.
        return df.to_csv(index=False, encoding='utf-8-sig')

    # Create a clean DataFrame for CSV download (without display formatting)
    original_data_for_download = ticker_list[['종목코드', '종목명', '종가']].merge(
                                                value_list.pivot(index='종목코드', columns='지표', values='값'),
                                                how='left',
                                                on='종목코드')
    
    csv_file = convert_df_to_csv(original_data_for_download)

    st.download_button(
        label="데이터 다운로드 (CSV)",
        data=csv_file,
        file_name=f"all_stock_valuation_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

except Exception as e:
    st.error(f"데이터 처리 중 오류가 발생했습니다: {e}")
    st.write("SQL 쿼리 또는 데이터베이스 스키마를 확인해 주세요.")

st.sidebar.markdown("---")
st.sidebar.info("데이터 로딩 시간은 DB 크기와 네트워크 속도에 따라 달라질 수 있습니다.")
st.sidebar.markdown("© 2025 Value Analyzer")
