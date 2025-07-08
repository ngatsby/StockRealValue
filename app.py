import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import time
import streamlit as st # Streamlit 라이브러리 임포트

# tqdm은 Streamlit 환경에서 콘솔 출력으로만 작동하므로, 실제 진행 바를 위해 Streamlit의 st.progress를 사용합니다.
# from tqdm.notebook import tqdm

# --- DB 연결 설정 (Azure MySQL에 맞게 변경) ---
# DB_CONFIG는 Streamlit Secrets를 통해 관리하는 것이 보안상 안전합니다.
# .streamlit/secrets.toml 파일에 다음 형식으로 저장:
# [mysql]
# host = "quant.mysql.database.azure.com"
# user = "quant@quant"
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

DB_CONFIG = get_db_config() # Streamlit Secrets에서 DB_CONFIG 로드

# SQLAlchemy 엔진 설정 (종목코드 리스트 로딩에 사용)
# DB_CONFIG가 로드된 후에 엔진을 생성합니다.
if DB_CONFIG:
    engine = create_engine(
        f'mysql+pymysql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:3306/{DB_CONFIG["database"]}',
        pool_recycle=300,
        pool_pre_ping=True,
        pool_timeout=60
    )
else:
    engine = None # DB_CONFIG가 없으면 엔진을 생성하지 않음

# --- PyMySQL 연결을 안정적으로 만드는 헬퍼 함수 ---
@st.cache_resource # Streamlit에서 리소스(DB 연결) 캐싱
def get_safe_pymysql_connection():
    """
    안정적인 PyMySQL 연결을 시도하고 반환합니다.
    연결 실패 시 여러 번 재시도합니다.
    """
    if not DB_CONFIG:
        return None

    for attempt in range(5):
        try:
            conn = pymysql.connect(**DB_CONFIG)
            # st.write(f"PyMySQL 연결 성공 (시도 {attempt + 1}회).") # Streamlit 앱에 너무 많은 로그 출력 방지
            return conn
        except pymysql.err.OperationalError as op_e:
            st.warning(f"PyMySQL 연결 시도 {attempt + 1}회 실패: {op_e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
                st.info("PyMySQL 연결 재시도 중...")
            else:
                st.error(f"PyMySQL 연결에 여러 번 실패했습니다. 마지막 오류: {op_e}")
                return None
    return None


def get_financial_data(cursor, stock_code, base_date, account_name, public_type='y'):
    query = f"""
        SELECT 값
        FROM kor_fs
        WHERE 종목코드 = '{stock_code}' 
          AND 기준일 = '{base_date}' 
          AND 계정 = '{account_name}'
          AND 공시구분 = '{public_type}'
        LIMIT 1
    """
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        if result and result[0] is not None:
            return float(result[0])
    except pymysql.Error as e:
        # st.error(f"DB 조회 오류 (financial): {stock_code}, {account_name} - {e}") # 너무 많은 로그 방지
        pass
    return None

def get_ticker_data(cursor, stock_code, column_name):
    query = f"""
        SELECT `{column_name}`
        FROM kor_ticker
        WHERE 종목코드 = '{stock_code}' 
        LIMIT 1
    """
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        if result and result[0] is not None:
            return float(result[0])
    except pymysql.Error as e:
        # st.error(f"DB 조회 오류 (ticker): {stock_code}, {column_name} - {e}") # 너무 많은 로그 방지
        pass
    return None

def calculate_intrinsic_value_per_share(stock_code, base_date, bond_10yr_rate_input, inflation_rate_input):
    """
    주어진 종목코드와 기준일에 대해 주당 내재가치를 계산하고, 추가 지표를 반환합니다.
    """
    conn = None
    data_for_return = {
        '내재가치': np.nan,
        '종가': np.nan,
        '워렌버핏DCF_적정주가': np.nan,
        '계산상태': '실패',
        '실패사유': '초기화'
    }

    try:
        conn = get_safe_pymysql_connection()
        if conn is None:
            data_for_return['실패사유'] = "DB 연결 실패"
            return data_for_return

        cursor = conn.cursor()

        UNIT_MULTIPLIER_FS = 100000000 # 억원 -> 원

        # --- 1. 조정자본총계 계산 ---
        total_assets_raw = get_financial_data(cursor, stock_code, base_date, '자산') 
        if total_assets_raw is None:
            data_for_return['실패사유'] = "자산 데이터 없음"
            return data_for_return
        total_assets = total_assets_raw * UNIT_MULTIPLIER_FS

        total_liabilities_raw = get_financial_data(cursor, stock_code, base_date, '부채') 
        if total_liabilities_raw is None:
            data_for_return['실패사유'] = "부채 데이터 없음"
            return data_for_return
        total_liabilities = total_liabilities_raw * UNIT_MULTIPLIER_FS

        goodwill = 0 
        other_long_term_assets = 0
        operating_assets = total_assets - (goodwill + other_long_term_assets)

        other_long_term_liabilities = 0
        deferred_tax_liabilities_raw = get_financial_data(cursor, stock_code, base_date, '이연법인세부채')
        deferred_tax_liabilities = deferred_tax_liabilities_raw * UNIT_MULTIPLIER_FS if deferred_tax_liabilities_raw is not None else 0
        
        operating_liabilities = total_liabilities - (other_long_term_liabilities + deferred_tax_liabilities)
        
        adjusted_capital = operating_assets - operating_liabilities
        if adjusted_capital == 0:
            data_for_return['실패사유'] = "조정자본총계 0"
            return data_for_return


        # --- 2. 주주이익 계산 ---
        net_income_raw = get_financial_data(cursor, stock_code, base_date, '당기순이익')
        depreciation_raw = get_financial_data(cursor, stock_code, base_date, '감가상각비')
        capex_raw = get_financial_data(cursor, stock_code, base_date, '유형자산의증가')

        net_income = net_income_raw * UNIT_MULTIPLIER_FS if net_income_raw is not None else 0
        depreciation = depreciation_raw * UNIT_MULTIPLIER_FS if depreciation_raw is not None else 0
        capex = capex_raw * UNIT_MULTIPLIER_FS if capex_raw is not None else 0

        shareholder_profit = net_income + depreciation - capex
        # if shareholder_profit < 0 and abs(shareholder_profit) > adjusted_capital * 0.5:
        #    # return None, "주주이익 음수" # 필요에 따라 음수 주주이익 제외 가능
        #    pass


        # --- 3. 자본효율 계산 ---
        if adjusted_capital == 0:
            data_for_return['실패사유'] = "조정자본총계 0 (재확인)"
            return data_for_return
        capital_efficiency = shareholder_profit / adjusted_capital

        # --- 4. 할인율 계산 ---
        bond_10yr_rate = bond_10yr_rate_input / 100.0 
        inflation_rate = inflation_rate_input / 100.0
        discount_rate = bond_10yr_rate + inflation_rate
        if discount_rate == 0:
            data_for_return['실패사유'] = "할인율 0"
            return data_for_return

        # --- 5. 자본배수 계산 ---
        capital_multiplier = capital_efficiency / discount_rate
        if np.isinf(capital_multiplier) or np.isnan(capital_multiplier):
            data_for_return['실패사유'] = "자본배수 비정상"
            return data_for_return

        # --- 6. 주식수 (자사주 제외) 계산 및 추가 지표 가져오기 ---
        current_price = get_ticker_data(cursor, stock_code, '종가')
        market_cap = get_ticker_data(cursor, stock_code, '시가총액') 

        data_for_return['종가'] = current_price # 종가 저장

        if current_price is None or market_cap is None:
            data_for_return['실패사유'] = "종가/시가총액 데이터 없음"
            return data_for_return

        if current_price == 0:
            data_for_return['실패사유'] = "종가 0"
            return data_for_return
        
        total_shares = market_cap / current_price
        shares_excluding_treasury = total_shares

        if shares_excluding_treasury == 0:
            data_for_return['실패사유'] = "주식수 0"
            return data_for_return

        # --- 최종 내재가치 계산 ---
        intrinsic_value_per_share = (adjusted_capital / shares_excluding_treasury) * capital_multiplier
        data_for_return['내재가치'] = intrinsic_value_per_share

        # --- 워렌 버핏 DCF 적정주가 계산 (간이 모델) ---
        # 간단하게 최근 EPS에 ROE(성장률 가정)를 반영한 후 요구수익률로 할인
        # EPS = 당기순이익 / 발행주식수 (여기서는 total_shares 사용)
        eps = net_income / shares_excluding_treasury if shares_excluding_treasury != 0 else 0

        # 성장률 가정: 여기서는 ROE를 성장률의 대용으로 사용합니다.
        # 더 정교한 분석을 위해서는 과거 EPS/매출액 성장률, 애널리스트 성장률 추정치 등을 사용해야 합니다.
        roe_raw = get_financial_data(cursor, stock_code, base_date, '자본', public_type='y') # ROE를 구하려면 자본총계도 필요
        if roe_raw is not None and adjusted_capital != 0:
            # ROE는 (당기순이익 / 자본총계) * 100
            # 재무제표의 '자본' 계정을 사용하여 ROE를 간략하게 계산 (혹은 kor_fs에 ROE 계정이 있다면 직접 가져옴)
            # 여기서는 편의상 자본효율(capital_efficiency)을 성장률로 가정합니다.
            # capital_efficiency는 주주이익 / 조정자본총계 이므로, ROE와 유사한 개념으로 볼 수 있습니다.
            assumed_growth_rate = capital_efficiency # 소수점 형태 (예: 0.15)
        else:
            assumed_growth_rate = 0.05 # 기본 성장률 5% 가정 (데이터 없을 경우)
        
        # 요구수익률: 채권 10년물 금리를 요구수익률로 가정 (또는 사용자가 입력한 할인율)
        required_rate_of_return = discount_rate # 위에서 계산한 할인율과 동일하게 사용

        if required_rate_of_return == 0:
            data_for_return['실패사유'] = "DCF: 요구수익률 0"
            return data_for_return

        # 간이 DCF 모델: EPS / (요구수익률 - 성장률)
        # 단, 요구수익률 > 성장률 이어야 함.
        if required_rate_of_return <= assumed_growth_rate:
            # 성장률이 요구수익률보다 높거나 같으면 무한대 값.
            # 이 경우 안정적인 DCF 모델 계산이 불가능하므로, 다른 방식으로 처리.
            # 여기서는 특정 큰 값 또는 NaN으로 처리합니다.
            data_for_return['워렌버핏DCF_적정주가'] = np.nan
            data_for_return['실패사유'] = "DCF: 성장률이 요구수익률보다 높음"
        else:
            # 영구성장률은 대략 인플레이션율 또는 그 이하로 가정 (예: 0.02)
            perpetual_growth_rate = inflation_rate # 인플레이션율 사용

            # Gordon Growth Model (고든 성장 모델) 간략화
            # P = D1 / (r - g) 에서 D1을 EPS로 대체 (배당 대신 이익 전체를 주주에게 귀속)
            # D1 = EPS * (1 + g) -> 여기서는 현재 EPS에 성장률을 바로 적용 (EPS * (1+assumed_growth_rate))
            # 1년 후 예상 EPS
            expected_eps_next_year = eps * (1 + assumed_growth_rate)
            
            # 고든 성장 모델
            # (만약 요구수익률이 영구성장률보다 낮거나 같으면 역시 문제 발생)
            if required_rate_of_return > perpetual_growth_rate:
                buffett_dcf_value = expected_eps_next_year / (required_rate_of_return - perpetual_growth_rate)
                data_for_return['워렌버핏DCF_적정주가'] = buffett_dcf_value
            else:
                data_for_return['워렌버핏DCF_적정주가'] = np.nan
                data_for_return['실패사유'] = "DCF: 요구수익률 <= 영구성장률"


        data_for_return['계산상태'] = '성공'
        data_for_return['실패사유'] = ''
        return data_for_return

    except pymysql.Error as err:
        data_for_return['실패사유'] = f"DB 오류: {err}"
        return data_for_return
    except ConnectionError as conn_err:
        data_for_return['실패사유'] = f"연결 오류: {conn_err}"
        return data_for_return
    except Exception as e:
        data_for_return['실패사유'] = f"일반 오류: {e}"
        return data_for_return
    finally:
        if conn and conn.open:
            conn.close()


# --- Streamlit 앱의 메인 로직 ---
st.set_page_config(layout="wide", page_title="내재가치 및 워렌 버핏 DCF 계산기")

st.title("💰 주식 내재가치 분석기")
st.markdown("Azure Cloud MySQL 데이터베이스에서 재무 데이터를 가져와 주식의 내재가치와 워렌 버핏식 DCF 적정주가를 계산합니다.")

# 사용자 입력 위젯
st.sidebar.header("설정")
calculation_base_date = st.sidebar.text_input("기준일 (YYYY-MM-DD)", value=datetime.date.today().strftime('%Y-%m-%d'))
user_bond_10yr_rate = st.sidebar.slider("10년 국채 금리 (%)", min_value=0.5, max_value=10.0, value=3.0, step=0.1)
user_inflation_rate = st.sidebar.slider("인플레이션율 (%)", min_value=0.0, max_value=5.0, value=2.0, step=0.1)

# 계산 시작 버튼
if st.sidebar.button("내재가치 계산 시작"):
    if not DB_CONFIG or not engine:
        st.error("데이터베이스 연결 설정이 올바르지 않습니다. `secrets.toml` 파일을 확인해주세요.")
    else:
        st.header(f"📈 계산 결과 ({calculation_base_date} 기준)")
        
        # 1. 전체 종목코드 불러오기
        st.info("전체 종목코드 불러오는 중...")
        stock_codes_df = pd.DataFrame()
        try:
            # pool_pre_ping 설정된 engine 사용
            stock_codes_df = pd.read_sql(
                f"""
                SELECT 종목코드, 종목명, 종가
                FROM kor_ticker
                WHERE 종목구분 = '보통주' AND 기준일 = (SELECT MAX(기준일) FROM kor_ticker);
                """,
                con=engine
            )
            stock_codes = stock_codes_df['종목코드'].tolist()
            stock_info_dict = stock_codes_df.set_index('종목코드').to_dict('index') # 모든 ticker 정보 딕셔너리로 저장
            st.success(f"총 {len(stock_codes)}개의 종목코드 불러오기 완료.")
        except Exception as e:
            st.error(f"오류: 종목코드 불러오기 실패 - {e}")
            stock_codes = []
        finally:
            if engine:
                engine.dispose() # SQLAlchemy 엔진의 연결 풀 정리
                # st.info("SQLAlchemy 엔진 해제 완료.") # Streamlit에서 너무 많은 로그 방지

        if not stock_codes:
            st.warning("계산할 종목이 없어 작업을 종료합니다.")
        else:
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()

            st.write(f"\n각 종목별 내재가치 계산 중... (기준일: {calculation_base_date})")
            for i, stock_code in enumerate(stock_codes):
                status_text.text(f"진행률: {i+1}/{len(stock_codes)} - 현재 계산 중: {stock_info_dict.get(stock_code, {}).get('종목명', stock_code)}")
                progress_bar.progress((i + 1) / len(stock_codes))

                calculated_data = calculate_intrinsic_value_per_share(
                    stock_code,
                    calculation_base_date,
                    user_bond_10yr_rate,
                    user_inflation_rate
                )
                
                # 기본 정보 추가
                row_data = {
                    '종목코드': stock_code,
                    '종목명': stock_info_dict.get(stock_code, {}).get('종목명', '알 수 없음'),
                    '종가': stock_info_dict.get(stock_code, {}).get('종가', np.nan)
                }
                # calculate_intrinsic_value_per_share 함수에서 반환된 데이터 병합
                row_data.update(calculated_data)
                
                results.append(row_data)
            
            progress_bar.empty()
            status_text.empty()
            st.success("모든 종목에 대한 계산 완료!")

            results_df = pd.DataFrame(results)
            
            # 내재가치-종가비율(%) 계산
            results_df['내재가치-종가비율(%)'] = results_df.apply(
                lambda row: ((row['내재가치'] - row['종가']) / row['종가']) * 100 if row['종가'] not in [0, np.nan] else np.nan,
                axis=1
            )
            # 워렌버핏DCF_적정주가-종가비율(%) 계산
            results_df['워렌버핏DCF-종가비율(%)'] = results_df.apply(
                lambda row: ((row['워렌버핏DCF_적정주가'] - row['종가']) / row['종가']) * 100 if row['종가'] not in [0, np.nan] else np.nan,
                axis=1
            )


            # 컬럼 순서 조정
            output_columns = [
                '종목명', '종목코드', '종가', '내재가치', '내재가치-종가비율(%)', 
                '워렌버핏DCF_적정주가', '워렌버핏DCF-종가비율(%)',
                '계산상태', '실패사유'
            ]
            final_df = results_df[output_columns].copy()

            # 숫자 포맷팅 (소수점 2자리)
            # 주의: NaN 값은 포매팅하지 않도록 처리
            for col in ['종가', '내재가치', '워렌버핏DCF_적정주가']:
                final_df[col] = final_df[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else None)
            
            for col in ['내재가치-종가비율(%)', '워렌버핏DCF-종가비율(%)']:
                 final_df[col] = final_df[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else None)


            st.dataframe(final_df, use_container_width=True) # 결과 데이터프레임 표시

            # CSV 다운로드 버튼
            csv_data = final_df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="결과 다운로드 (CSV)",
                data=csv_data,
                file_name=f"intrinsic_value_analysis_{datetime.date.today().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

st.sidebar.info("계산 시간은 종목 수와 DB 연결 속도에 따라 달라질 수 있습니다.")
st.sidebar.markdown("---")
st.sidebar.markdown("© 2025 Value Analyzer")
