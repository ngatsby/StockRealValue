import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql # Required by SQLAlchemy for mysql+pymysql
import statsmodels.api as sm
from scipy.stats import zscore
import matplotlib.pyplot as plt # Kept for potential future use, not directly displayed here

# --- 1. Database Connection Configuration ---
# IMPORTANT: Configure your .streamlit/secrets.toml file with your Azure MySQL credentials.
#
# Example .streamlit/secrets.toml content:
# [mysql]
# host = "your_azure_mysql_server_name.mysql.database.azure.com"
# user = "your_username@your_server_name" # e.g., "quant@quant" (username@servername for Azure)
# password = "your_db_password"
# database = "your_database_name"
# charset = "utf8mb4" # Recommended for broader character support
# connect_timeout = 10
# read_timeout = 10
# write_timeout = 10

@st.cache_resource
def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine for your Azure MySQL database.
    Caches the engine to prevent recreating it on every Streamlit rerun.
    """
    if not st.secrets.get("mysql"):
        st.error("DB connection secrets not found. Please configure .streamlit/secrets.toml.")
        return None

    db_config = st.secrets["mysql"]
    
    try:
        connection_string = (
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:3306/{db_config['database']}?charset={db_config['charset']}"
        )
        
        engine = create_engine(
            connection_string,
            pool_recycle=300,  # Recycle connections after 300 seconds (5 minutes)
            pool_pre_ping=True, # Test connections before use
            pool_timeout=60 # Timeout for getting a connection from the pool
        )
        st.success("Database engine created successfully.")
        return engine
    except Exception as e:
        st.error(f"Error creating database engine: {e}")
        st.info("Please check your .streamlit/secrets.toml for correct Azure MySQL credentials and server firewall rules.")
        return None

# --- 2. Data Loading Functions ---
# Use st.cache_data to cache the loaded dataframes.
# This means data is fetched from DB only when the function arguments change or after ttl.
@st.cache_data(ttl=3600) # Cache raw data for 1 hour
def load_raw_data(engine):
    """
    Loads all necessary raw dataframes from the MySQL database.
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
        data['ticker_list']['종목코드'] = data['ticker_list']['종목코드'].astype(str) # Ensure string type for merging
        st.success(f"Loaded {len(data['ticker_list'])} ordinary tickers from kor_ticker.")
    except Exception as e:
        st.error(f"Failed to load `kor_ticker`: {e}")
        data['ticker_list'] = pd.DataFrame()

    st.info("Loading `kor_fs` (Financial Statements)...")
    try:
        data['fs_list'] = pd.read_sql("""
            SELECT 종목코드, 기준일, 계정, 값, 공시구분
            FROM kor_fs
            WHERE 계정 IN ('당기순이익', '매출총이익', '영업활동으로인한현금흐름', '자산', '자본')
            AND 공시구분 = 'q';
        """, con=engine)
        data['fs_list']['종목코드'] = data['fs_list']['종목코드'].astype(str)
        st.success(f"Loaded {len(data['fs_list'])} financial statement entries from kor_fs.")
    except Exception as e:
        st.error(f"Failed to load `kor_fs`: {e}")
        data['fs_list'] = pd.DataFrame()

    st.info("Loading `kor_value` (Valuation Data)...")
    try:
        data['value_list'] = pd.read_sql("""
            SELECT 종목코드, 기준일, 지표, 값
            FROM kor_value
            WHERE 기준일 = (SELECT MAX(기준일) FROM kor_value);
        """, con=engine)
        data['value_list']['종목코드'] = data['value_list']['종목코드'].astype(str)
        st.success(f"Loaded {len(data['value_list'])} valuation entries from kor_value.")
    except Exception as e:
        st.error(f"Failed to load `kor_value`: {e}")
        data['value_list'] = pd.DataFrame()

    st.info("Loading `kor_price` (Last 1 year of prices)...")
    try:
        # Assuming '기준일' is the date column in kor_price
        data['price_list'] = pd.read_sql("""
            SELECT 기준일 AS 날짜, 종가, 종목코드
            FROM kor_price
            WHERE 기준일 >= (SELECT MAX(기준일) - INTERVAL 1 YEAR FROM kor_price);
        """, con=engine)
        data['price_list']['종목코드'] = data['price_list']['종목코드'].astype(str)
        data['price_list']['날짜'] = pd.to_datetime(data['price_list']['날짜']) # Convert to datetime
        st.success(f"Loaded {len(data['price_list'])} price entries from kor_price.")
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
        # Assuming CMP_CD is the stock code in kor_sector
        data['sector_list']['CMP_CD'] = data['sector_list']['CMP_CD'].astype(str) 
        st.success(f"Loaded {len(data['sector_list'])} sector entries from kor_sector.")
    except Exception as e:
        st.error(f"Failed to load `kor_sector`: {e}")
        data['sector_list'] = pd.DataFrame()
        
    return data

# --- 3. Data Processing Functions ---
@st.cache_data(ttl=3600) # Cache processed data for 1 hour
def process_financial_statements(fs_df):
    """Processes fs_list to calculate TTM, ROE, GPA, CFO."""
    if fs_df.empty:
        return pd.DataFrame()

    st.info("Processing financial statements (FS TTM, ROE, GPA, CFO)...")
    
    fs_list_sorted = fs_df.sort_values(['종목코드', '계정', '기준일'])
    
    # Calculate TTM (Trailing Twelve Months) for income statement items
    # and average of last 4 quarters for balance sheet items (Assets, Equity)
    # Groupby '종목코드' and '계정' and then apply rolling window
    fs_list_sorted['ttm_temp'] = fs_list_sorted.groupby(['종목코드', '계정'])['값'].rolling(
        window=4, min_periods=4).sum().reset_index(level=[0,1], drop=True)
    
    # Apply special handling for '자산' and '자본' (average of 4 quarters)
    fs_list_sorted['ttm'] = np.where(fs_list_sorted['계정'].isin(['자산', '자본']),
                                     fs_list_sorted['ttm_temp'] / 4, 
                                     fs_list_sorted['ttm_temp'])
    
    # Get the latest TTM value for each stock and account
    fs_list_clean = fs_list_sorted.groupby(['종목코드', '계정']).tail(1).copy()

    # Pivot to make accounts into columns
    fs_list_pivot = fs_list_clean.pivot(index='종목코드', columns='계정', values='ttm')

    # Calculate ROE, GPA, CFO
    fs_list_pivot['ROE'] = fs_list_pivot['당기순이익'] / fs_list_pivot['자본']
    fs_list_pivot['GPA'] = fs_list_pivot['매출총이익'] / fs_list_pivot['자산']
    fs_list_pivot['CFO'] = fs_list_pivot['영업활동으로인한현금흐름'] / fs_list_pivot['자산']
    
    st.success("Financial statements processed.")
    return fs_list_pivot

@st.cache_data(ttl=3600) # Cache processed data for 1 hour
def process_valuation_data(value_df):
    """Processes value_list to pivot and handle non-positive values."""
    if value_df.empty:
        return pd.DataFrame()

    st.info("Processing valuation data...")
    value_list_clean = value_df.copy()
    # Replace non-positive values with NaN for valuation ratios
    value_list_clean.loc[value_list_clean['값'] <= 0, '값'] = np.nan
    value_pivot = value_list_clean.pivot(index='종목코드', columns='지표', values='값')
    st.success("Valuation data processed.")
    return value_pivot

@st.cache_data(ttl=3600) # Cache processed data for 1 hour
def calculate_k_ratio(price_df, ticker_df):
    """Calculates K_ratio for each stock."""
    if price_df.empty or ticker_df.empty:
        return pd.DataFrame()

    st.info("Calculating K_ratio...")
    
    # Ensure '날짜' is set as index and data is sorted for pivoting
    # Check if '날짜' is already the index, if not, set it
    if '날짜' not in price_df.columns and price_df.index.name == '날짜':
        price_pivot = price_df.copy() # Already pivoted
    else:
        # Assuming '날짜' is a column after load_raw_data
        price_pivot = price_df.pivot(index='날짜', columns='종목코드', values='종가')
    
    # Drop columns with all NaNs that might result from pivoting
    price_pivot = price_pivot.dropna(axis=1, how='all')

    if price_pivot.empty:
        st.warning("Price pivot is empty after dropping NaNs. Cannot calculate K_ratio.")
        return pd.DataFrame()

    ret = price_pivot.pct_change().iloc[1:] # Daily returns
    ret_cum = np.log(1 + ret).cumsum() # Cumulative log returns

    x = np.array(range(len(ret_cum)))
    # Reshape x to be 2D array if ret_cum has multiple columns for OLS
    x = sm.add_constant(x) # Add a constant for OLS

    k_ratio = {}
    
    # Filter tickers that actually exist in price_pivot columns
    tickers_in_price = [t for t in ticker_df['종목코드'].unique() if t in price_pivot.columns]

    for ticker in tickers_in_price:
        try:
            # Select the column for the current ticker and ensure it's a DataFrame
            y = ret_cum[[ticker]].dropna() # Drop NaNs for that specific ticker's returns
            if y.empty: # Skip if no valid returns for this ticker
                k_ratio[ticker] = np.nan
                continue
            
            # Align x with y's length after dropping NaNs
            current_x = x[:len(y)]

            reg = sm.OLS(y, current_x).fit()
            # K-ratio is often defined as beta / std_err (t-statistic of slope)
            # Ensure we're getting the slope's t-statistic, which is at index 1 (after constant)
            res = float(reg.tvalues[1]) 
        except Exception as e:
            # st.warning(f"Could not calculate K_ratio for {ticker}: {e}") # Too verbose for many stocks
            res = np.nan
        k_ratio[ticker] = res

    k_ratio_bind = pd.DataFrame.from_dict(k_ratio, orient='index').reset_index()
    k_ratio_bind.columns = ['종목코드', 'K_ratio']
    st.success("K_ratio calculated.")
    return k_ratio_bind

@st.cache_data(ttl=3600) # Cache the merged dataframe
def merge_all_data(raw_data_dict, fs_pivot, value_pivot, k_ratio_df):
    """Merges all processed data into a single DataFrame."""
    st.info("Merging all dataframes...")
    
    ticker_list = raw_data_dict.get('ticker_list', pd.DataFrame())
    sector_list = raw_data_dict.get('sector_list', pd.DataFrame())
    price_list = raw_data_dict.get('price_list', pd.DataFrame()) # Used for 12M return

    if ticker_list.empty:
        st.error("Ticker list is empty. Cannot merge.")
        return pd.DataFrame()

    # Calculate 12M return from price_list
    ret_list = pd.DataFrame()
    if not price_list.empty:
        # Pivot price_list to get returns
        price_pivot_for_ret = price_list.pivot(index='날짜', columns='종목코드', values='종가')
        price_pivot_for_ret = price_pivot_for_ret.dropna(axis=1, how='all') # Drop columns with no price data
        if not price_pivot_for_ret.empty:
            # Calculate 12M return based on first and last available price for each stock
            # Handle cases where there might not be 12 months of data
            ret_data = {}
            for col in price_pivot_for_ret.columns:
                series = price_pivot_for_ret[col].dropna()
                if len(series) >= 2: # Need at least start and end price
                    ret_data[col] = (series.iloc[-1] / series.iloc[0]) - 1
                else:
                    ret_data[col] = np.nan
            ret_list = pd.DataFrame(data=ret_data.values(), index=ret_data.keys(), columns=['12M'])
            ret_list.index.name = '종목코드'
            ret_list = ret_list.reset_index() # Convert index to a column for merging
            ret_list['종목코드'] = ret_list['종목코드'].astype(str) # Ensure string type

    data_bind = ticker_list[['종목코드', '종목명']].copy()
    
    # Merge with sector information
    if not sector_list.empty:
        data_bind = data_bind.merge(
            sector_list[['CMP_CD', 'SEC_NM_KOR']],
            how='left',
            left_on='종목코드',
            right_on='CMP_CD'
        )
        data_bind.loc[data_bind['SEC_NM_KOR'].isnull(), 'SEC_NM_KOR'] = '기타'
        data_bind = data_bind.drop(['CMP_CD'], axis=1)
    else:
        data_bind['SEC_NM_KOR'] = '기타' # Default if no sector data

    # Merge with financial statements pivot
    if not fs_pivot.empty:
        data_bind = data_bind.merge(
            fs_pivot[['ROE', 'GPA', 'CFO']], 
            how='left',
            on='종목코드'
        )

    # Merge with valuation pivot
    if not value_pivot.empty:
        data_bind = data_bind.merge(
            value_pivot, 
            how='left',
            on='종목코드'
        )

    # Merge with 12M return list
    if not ret_list.empty:
        data_bind = data_bind.merge(
            ret_list, 
            how='left',
            on='종목코드'
        )
    else:
        data_bind['12M'] = np.nan # Add column even if no data

    # Merge with K_ratio
    if not k_ratio_df.empty:
        data_bind = data_bind.merge(
            k_ratio_df,
            how='left',
            on='종목코드'
        )
    else:
        data_bind['K_ratio'] = np.nan # Add column even if no data

    st.success("All dataframes merged.")
    return data_bind

# --- 4. Factor Calculation Functions ---
def col_clean(df_series, cutoff=0.01, asc=False):
    """Applies winsorization (trimming) and Z-score ranking."""
    if df_series.empty or df_series.isnull().all():
        return pd.Series(np.nan, index=df_series.index) # Return NaN series if empty or all NaN

    q_low = df_series.quantile(cutoff)
    q_hi = df_series.quantile(1 - cutoff)

    df_trim = df_series[(df_series > q_low) & (df_series < q_hi)].copy()

    if asc:
        df_z_score = df_trim.rank(axis=0, ascending=True, na_option='keep').apply(
            zscore, nan_policy='omit')
    else: # Default is descending for 'good' metrics
        df_z_score = df_trim.rank(axis=0, ascending=False, na_option='keep').apply(
            zscore, nan_policy='omit')
    
    return df_z_score

@st.cache_data(ttl=3600) # Cache factor scores
def calculate_factors(data_df):
    """Calculates Quality, Value, and Momentum Z-scores."""
    if data_df.empty or 'SEC_NM_KOR' not in data_df.columns:
        st.error("Dataframe is empty or missing 'SEC_NM_KOR' for factor calculation.")
        return data_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    st.info("Calculating Quality, Value, and Momentum factors...")

    # Set index for grouping
    df_indexed = data_df.set_index(['종목코드', 'SEC_NM_KOR'])
    data_bind_group = df_indexed.groupby('SEC_NM_KOR')

    # Quality Factor
    quality_cols = ['ROE', 'GPA', 'CFO']
    # Check if all quality columns exist before applying
    if all(col in df_indexed.columns for col in quality_cols):
        z_quality = data_bind_group[quality_cols].apply(lambda x: x.apply(lambda col: col_clean(col, 0.01, False)))
        # Sum Z-scores and convert back to DataFrame
        z_quality_sum = z_quality.sum(axis=1, skipna=False).to_frame('z_quality')
        data_df = data_df.merge(z_quality_sum, how='left', on=['종목코드', 'SEC_NM_KOR'])
    else:
        st.warning(f"Missing one or more quality columns: {quality_cols}. Quality factor not calculated.")
        data_df['z_quality'] = np.nan # Add the column with NaNs

    # Value Factor
    value_asc_cols = ['PBR', 'PCR', 'PER', 'PSR'] # Lower is better
    value_desc_cols = ['DY'] # Higher is better
    
    # Filter columns that actually exist
    existing_value_asc = [col for col in value_asc_cols if col in df_indexed.columns]
    existing_value_desc = [col for col in value_desc_cols if col in df_indexed.columns]

    z_value_parts = []
    if existing_value_asc:
        value_1 = data_bind_group[existing_value_asc].apply(lambda x: x.apply(lambda col: col_clean(col, 0.01, True)))
        z_value_parts.append(value_1)
    if existing_value_desc:
        value_2 = data_bind_group[existing_value_desc].apply(lambda x: x.apply(lambda col: col_clean(col, 0.01, False)))
        z_value_parts.append(value_2)

    if z_value_parts:
        # Concatenate and then sum if multiple parts
        if len(z_value_parts) > 1:
            z_value_combined = pd.concat(z_value_parts, axis=1)
        else:
            z_value_combined = z_value_parts[0]
        
        z_value_sum = z_value_combined.sum(axis=1, skipna=False).to_frame('z_value')
        data_df = data_df.merge(z_value_sum, how='left', on=['종목코드', 'SEC_NM_KOR'])
    else:
        st.warning(f"Missing all value columns. Value factor not calculated.")
        data_df['z_value'] = np.nan

    # Momentum Factor
    momentum_cols = ['12M', 'K_ratio']
    if all(col in df_indexed.columns for col in momentum_cols):
        z_momentum = data_bind_group[momentum_cols].apply(lambda x: x.apply(lambda col: col_clean(col, 0.01, False)))
        z_momentum_sum = z_momentum.sum(axis=1, skipna=False).to_frame('z_momentum')
        data_df = data_df.merge(z_momentum_sum, how='left', on=['종목코드', 'SEC_NM_KOR'])
    else:
        st.warning(f"Missing one or more momentum columns: {momentum_cols}. Momentum factor not calculated.")
        data_df['z_momentum'] = np.nan # Add the column with NaNs

    st.success("Factors calculated.")
    return data_df

# --- 5. Portfolio Selection ---
@st.cache_data(ttl=3600)
def select_portfolio(data_df_with_factors, top_n_stocks=20):
    """Selects top N stocks based on QVM score."""
    st.info(f"Selecting top {top_n_stocks} stocks...")

    # Filter for necessary Z-scores
    factor_columns = ['z_quality', 'z_value', 'z_momentum']
    # Check if all factor columns exist and are not all NaN
    existing_factors = [col for col in factor_columns if col in data_df_with_factors.columns and not data_df_with_factors[col].isnull().all()]

    if not existing_factors:
        st.warning("No valid factor scores (z_quality, z_value, z_momentum) available for portfolio selection.")
        # Create an empty 'qvm' and 'invest' column to avoid errors downstream
        data_df_with_factors['qvm'] = np.nan
        data_df_with_factors['invest'] = 'N'
        return data_df_with_factors

    # Set weights. Sum of weights should be 1 if factors are independent.
    # If not all factors exist, adjust weights or proceed with existing ones.
    # For now, if a factor column is missing or all NaN, it won't be part of the sum.
    # The sum will only be over the existing_factors.
    weights_map = {
        'z_quality': 0.3, 
        'z_value': 0.3, 
        'z_momentum': 0.3
    }
    
    # Adjust weights if some factors are missing or all NaN
    active_weights = {col: weights_map[col] for col in existing_factors}
    if sum(active_weights.values()) == 0:
        st.warning("All factor scores are NaN. Cannot calculate QVM score.")
        data_df_with_factors['qvm'] = np.nan
        data_df_with_factors['invest'] = 'N'
        return data_df_with_factors

    # Normalize weights to sum to 1 if not all factors are present and equally weighted
    total_active_weight = sum(active_weights.values())
    if total_active_weight > 0:
        normalized_weights = {col: weight / total_active_weight for col, weight in active_weights.items()}
    else:
        normalized_weights = {} # Should not happen if existing_factors is not empty

    # Calculate combined QVM score
    # Use .copy() to avoid SettingWithCopyWarning
    data_df_copy = data_df_with_factors.copy()
    data_df_copy['qvm'] = 0.0
    for col, weight in normalized_weights.items():
        data_df_copy['qvm'] += data_df_copy[col].fillna(0) * weight # Fillna(0) for missing factor scores

    # Rank and select top N stocks
    # Handle NaN qvm values by pushing them to the end of the rank
    data_df_copy['qvm_rank'] = data_df_copy['qvm'].rank(ascending=False, na_option='last')
    data_df_copy['invest'] = np.where(data_df_copy['qvm_rank'] <= top_n_stocks, 'Y', 'N')

    st.success(f"Portfolio selection complete. Top {top_n_stocks} stocks identified.")
    return data_df_copy

# --- 6. Streamlit Application Main Logic ---

st.set_page_config(layout="wide", page_title="퀀트 투자 모델")

st.title("📈 주식 퀀트 투자 모델 (QVM)")
st.markdown("""
이 애플리케이션은 Azure Cloud MySQL 데이터베이스에서 재무 데이터, 가치 지표, 가격 데이터 및 섹터 정보를 불러와 
**퀄리티(Quality)**, **가치(Value)**, **모멘텀(Momentum)** 팩터 점수를 계산하고, 
최종 QVM 점수를 기반으로 상위 투자 종목을 선정합니다.
""")

# --- Setup for user input (if any, e.g., for top N stocks) ---
st.sidebar.header("모델 설정")
top_n_stocks = st.sidebar.slider("상위 투자 종목 수", min_value=5, max_value=100, value=20, step=5)
# You could add weights adjustment here if you want:
# st.sidebar.subheader("팩터 가중치 (총합 1로 자동 조정)")
# w_quality = st.sidebar.slider("퀄리티 가중치", 0.0, 1.0, 0.3, 0.05)
# w_value = st.sidebar.slider("가치 가중치", 0.0, 1.0, 0.3, 0.05)
# w_momentum = st.sidebar.slider("모멘텀 가중치", 0.0, 1.0, 0.3, 0.05)
# wts_input = [w_quality, w_value, w_momentum]


if st.button("모델 실행 및 결과 보기"):
    st.header("⏳ 데이터 로딩 및 모델 계산 중...")
    
    # Get DB Engine
    engine_instance = get_db_engine()
    if engine_instance is None:
        st.stop() # Stop if DB engine failed to initialize

    # Load Raw Data
    raw_data = load_raw_data(engine_instance)

    # Dispose engine after raw data loading (important for resource management)
    engine_instance.dispose()

    if any(df.empty for df in raw_data.values()):
        st.error("필요한 원본 데이터 로딩에 실패했습니다. 위의 오류 메시지를 확인하세요.")
        st.stop()

    # Process Data
    fs_pivot_df = process_financial_statements(raw_data['fs_list'])
    value_pivot_df = process_valuation_data(raw_data['value_list'])
    k_ratio_df = calculate_k_ratio(raw_data['price_list'], raw_data['ticker_list'])

    # Merge Data
    data_merged = merge_all_data(raw_data, fs_pivot_df, value_pivot_df, k_ratio_df)

    if data_merged.empty:
        st.error("데이터 병합에 실패했습니다. 원본 데이터 및 병합 로직을 확인하세요.")
        st.stop()
    
    st.subheader("📊 병합된 원본 데이터 (일부)")
    st.dataframe(data_merged.head())

    # Calculate Factors
    data_with_factors = calculate_factors(data_merged)

    st.subheader("📈 팩터 점수 (일부)")
    # Display selected factor columns for verification
    factor_display_cols = ['종목명', '종목코드', 'SEC_NM_KOR', 'ROE', 'GPA', 'CFO', 
                           'PBR', 'PER', 'PCR', 'PSR', 'DY', '12M', 'K_ratio',
                           'z_quality', 'z_value', 'z_momentum']
    st.dataframe(data_with_factors[[col for col in factor_display_cols if col in data_with_factors.columns]].head())

    # Select Portfolio
    final_portfolio_df = select_portfolio(data_with_factors, top_n_stocks=top_n_stocks)

    st.header("✅ 모델 결과: QVM 상위 투자 종목")
    
    # Filter for selected stocks
    invest_stocks = final_portfolio_df[final_portfolio_df['invest'] == 'Y'].sort_values('qvm_rank').copy()

    if not invest_stocks.empty:
        st.success(f"총 {len(invest_stocks)}개의 투자 종목이 선정되었습니다.")
        
        # Display selected columns and format numbers
        display_cols = ['종목명', '종목코드', 'SEC_NM_KOR', 'qvm', 'qvm_rank'] + [col for col in factor_display_cols if col not in ['종목명', '종목코드', 'SEC_NM_KOR']]

        # Select and format columns for display
        display_df = invest_stocks[[col for col in display_cols if col in invest_stocks.columns]].copy()
        
        # Numeric formatting for display only
        for col in ['ROE', 'GPA', 'CFO', 'PBR', 'PER', 'PCR', 'PSR', 'DY', '12M', 'K_ratio', 
                    'z_quality', 'z_value', 'z_momentum', 'qvm']:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.4f}" if pd.notna(x) else None)
        
        st.dataframe(display_df, use_container_width=True)

        # Download CSV of selected stocks (with original numeric values for calculation)
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False, encoding='utf-8-sig')

        csv_data = convert_df_to_csv(invest_stocks.drop(columns=['qvm_rank', 'invest'], errors='ignore')) # Drop temp columns

        st.download_button(
            label="선정된 종목 다운로드 (CSV)",
            data=csv_data,
            file_name=f"qvm_model_selection_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv"
        )
    else:
        st.warning("선정된 투자 종목이 없습니다. 데이터나 모델 설정(예: top N 값)을 확인하세요.")

st.sidebar.markdown("---")
st.sidebar.info("퀀트 모델 계산 시간은 데이터 양과 DB 연결 속도에 따라 달라질 수 있습니다.")
st.sidebar.markdown(f"현재 시각: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S KST')}")
st.sidebar.markdown("© 2025 Quant Model")
