# 패키지 불러오기
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import time
from tqdm.notebook import tqdm # 진행률 바를 위해 tqdm 추가

# --- DB 연결 설정 (Azure MySQL에 맞게 변경) ---
DB_CONFIG = {
    'host': 'quant.mysql.database.azure.com', # <-- Azure MySQL 서버 호스트 주소
    'user': 'quant', # <-- Azure MySQL 사용자명 형식 (사용자@서버이름)
    'password': 'a303737!', # <-- **당신의 실제 비밀번호로 교체하세요!**
    'database': 'stock_db',
    'charset': 'utf8',
    'connect_timeout': 10, # 연결 시도 최대 10초 대기
    'read_timeout': 10,    # 데이터 읽기 시 최대 10초 대기
    'write_timeout': 10    # 데이터 쓰기 시 최대 10초 대기
}

# SQLAlchemy 엔진 설정 (종목코드 리스트 로딩에 사용)
engine = create_engine(
    f'mysql+pymysql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:3306/{DB_CONFIG["database"]}',
    pool_recycle=300,  # 5분(300초)마다 연결 재활용
    pool_pre_ping=True, # 연결 사용 전 유효성 검사
    pool_timeout=60 # 연결 풀에서 연결을 얻어올 때 최대 60초 대기
)

# --- PyMySQL 연결을 안정적으로 만드는 헬퍼 함수 ---
def get_safe_pymysql_connection():
    """
    안정적인 PyMySQL 연결을 시도하고 반환합니다.
    연결 실패 시 여러 번 재시도합니다.
    """
    for attempt in range(5): # 최대 5번 연결 시도
        try:
            conn = pymysql.connect(**DB_CONFIG)
            # print(f"PyMySQL 연결 성공 (시도 {attempt + 1}회).") # 너무 많은 로그 방지
            return conn
        except pymysql.err.OperationalError as op_e:
            print(f"PyMySQL 연결 시도 {attempt + 1}회 실패: {op_e}")
            if attempt < 4:
                time.sleep(5 * (attempt + 1))
                print("PyMySQL 연결 재시도 중...")
            else:
                raise ConnectionError(f"PyMySQL 연결에 여러 번 실패했습니다. 마지막 오류: {op_e}") from op_e
    return None


def get_financial_data(cursor, stock_code, base_date, account_name, public_type='y'):
    """
    kor_fs 테이블에서 특정 계정의 값을 가져오는 헬퍼 함수.
    공시구분(public_type)을 지정할 수 있으며, 값이 없을 경우 None 반환.
    """
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
        print(f"DB 조회 오류 (financial): {stock_code}, {account_name} - {e}")
    return None

def get_ticker_data(cursor, stock_code, column_name):
    """
    kor_ticker 테이블에서 특정 컬럼의 값을 가져오는 헬퍼 함수.
    기준일 조건 없이 종목코드로만 조회하며, 값이 없을 경우 None 반환.
    """
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
        print(f"DB 조회 오류 (ticker): {stock_code}, {column_name} - {e}")
    return None


def calculate_intrinsic_value_per_share(stock_code, base_date, bond_10yr_rate_input, inflation_rate_input):
    """
    주어진 종목코드와 기준일에 대해 주당 내재가치를 계산합니다.
    (함수 로직은 이전과 동일)
    """
    conn = None
    try:
        conn = get_safe_pymysql_connection()
        if conn is None:
            # print(f"데이터베이스 연결 실패. {stock_code} 계산 건너뜜.") # 너무 많은 로그 방지
            return None, "DB 연결 실패"

        cursor = conn.cursor()

        # print(f"--- 데이터 조회 및 계산 시작 (종목코드: {stock_code}, 기준일: {base_date}) ---") # 너무 많은 로그 방지

        UNIT_MULTIPLIER_FS = 100000000 # 억원 -> 원

        # 1. 조정자본총계 계산
        total_assets_raw = get_financial_data(cursor, stock_code, base_date, '자산')
        if total_assets_raw is None:
            # print(f"오류: {stock_code} '자산' 데이터 없음. 조정자본총계 계산 불가.")
            return None, "자산 데이터 없음"
        total_assets = total_assets_raw * UNIT_MULTIPLIER_FS

        total_liabilities_raw = get_financial_data(cursor, stock_code, base_date, '부채')
        if total_liabilities_raw is None:
            # print(f"오류: {stock_code} '부채' 데이터 없음. 조정자본총계 계산 불가.")
            return None, "부채 데이터 없음"
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
            # print(f"오류: {stock_code} 조정자본총계가 0. 계산 불가.")
            return None, "조정자본총계 0"


        # 2. 주주이익 계산
        net_income_raw = get_financial_data(cursor, stock_code, base_date, '당기순이익')
        depreciation_raw = get_financial_data(cursor, stock_code, base_date, '감가상각비')
        capex_raw = get_financial_data(cursor, stock_code, base_date, '유형자산의증가')

        net_income = net_income_raw * UNIT_MULTIPLIER_FS if net_income_raw is not None else 0
        depreciation = depreciation_raw * UNIT_MULTIPLIER_FS if depreciation_raw is not None else 0
        capex = capex_raw * UNIT_MULTIPLIER_FS if capex_raw is not None else 0

        shareholder_profit = net_income + depreciation - capex
        if shareholder_profit < 0 and abs(shareholder_profit) > adjusted_capital * 0.5: # 너무 큰 음의 주주이익은 문제로 간주
            # print(f"경고: {stock_code} 주주이익이 크게 음수입니다. 계산 결과가 왜곡될 수 있음.")
            # return None, "주주이익 음수" # 필요에 따라 음수 주주이익 제외 가능
            pass


        # 3. 자본효율 계산
        if adjusted_capital == 0: # 위에 이미 체크했지만, 혹시 몰라서 한번 더 체크
            return None, "조정자본총계 0"
        capital_efficiency = shareholder_profit / adjusted_capital

        # 4. 할인율 계산
        bond_10yr_rate = bond_10yr_rate_input / 100.0
        inflation_rate = inflation_rate_input / 100.0
        discount_rate = bond_10yr_rate + inflation_rate
        if discount_rate == 0:
            # print(f"오류: {stock_code} 할인율이 0. 계산 불가.")
            return None, "할인율 0"

        # 5. 자본배수 계산
        capital_multiplier = capital_efficiency / discount_rate
        if np.isinf(capital_multiplier) or np.isnan(capital_multiplier):
            # print(f"경고: {stock_code} 자본배수가 무한대 또는 NaN입니다. 계산 불가.")
            return None, "자본배수 비정상"

        # 6. 주식수 (자사주 제외) 계산
        current_price = get_ticker_data(cursor, stock_code, '종가')
        market_cap = get_ticker_data(cursor, stock_code, '시가총액')

        if current_price is None or market_cap is None:
            # print(f"오류: {stock_code} 종가/시가총액 데이터 없음. 주식수 계산 불가.")
            return None, "종가/시가총액 데이터 없음"

        if current_price == 0:
            # print(f"오류: {stock_code} 종가가 0. 주식수 계산 불가.")
            return None, "종가 0"

        total_shares = market_cap / current_price
        shares_excluding_treasury = total_shares

        if shares_excluding_treasury == 0:
            # print(f"오류: {stock_code} 자사주 제외 주식수가 0. 계산 불가.")
            return None, "주식수 0"

        # 최종 주당 내재가치 계산
        intrinsic_value_per_share = (adjusted_capital / shares_excluding_treasury) * capital_multiplier

        return intrinsic_value_per_share, None # 성공 시 계산된 값과 None 반환

    except pymysql.Error as err:
        # print(f"데이터베이스 오류: {stock_code} - {err}")
        return None, f"DB 오류: {err}"
    except ConnectionError as conn_err:
        # print(f"연결 오류: {stock_code} - {conn_err}")
        return None, f"연결 오류: {conn_err}"
    except Exception as e:
        # print(f"예상치 못한 오류: {stock_code} - {e}")
        return None, f"일반 오류: {e}"
    finally:
        if conn and conn.open:
            conn.close()
            # print(f"PyMySQL 연결 닫힘 ({stock_code}).") # 너무 많은 로그 방지


# --- 메인 실행 로직 ---
if __name__ == "__main__":
    print("--- 전체 종목 내재가치 계산 및 CSV 내보내기 시작 ---")

    # 고정된 기준일 설정 (예: 최근 연말)
    calculation_base_date = '2024-12-31'

    # 사용자 직접 입력 금리 및 인플레이션율
    user_bond_10yr_rate = 3.0 # 10년 국채 금리 약 3%
    user_inflation_rate = 2.0 # 물가상승률 2%

    # 1. 전체 종목코드 불러오기 (SQLAlchemy 엔진 사용)
    print("전체 종목코드 불러오는 중...")
    stock_codes_df = pd.DataFrame()
    try:
        # pool_pre_ping 설정된 engine 사용
        stock_codes_df = pd.read_sql(
            """
            SELECT 종목코드, 종목명
            FROM kor_ticker
            WHERE 종목구분 = '보통주' AND 기준일 = (SELECT MAX(기준일) FROM kor_ticker);
            """,
            con=engine
        )
        stock_codes = stock_codes_df['종목코드'].tolist()
        stock_names = stock_codes_df.set_index('종목코드')['종목명'].to_dict()
        print(f"총 {len(stock_codes)}개의 종목코드 불러오기 완료.")
    except Exception as e:
        print(f"오류: 종목코드 불러오기 실패 - {e}")
        stock_codes = [] # 오류 시 빈 리스트로 초기화
    finally:
        # SQLAlchemy 엔진의 연결 풀 정리
        if engine:
            engine.dispose()
            print("SQLAlchemy 엔진 해제 완료.")

    if not stock_codes:
        print("계산할 종목이 없어 작업을 종료합니다.")
    else:
        # 내재가치 결과를 저장할 리스트
        results = []

        # 2. 각 종목에 대해 내재가치 계산
        print(f"\n각 종목별 내재가치 계산 중... (기준일: {calculation_base_date}, 10년물 금리: {user_bond_10yr_rate}%, 인플레이션: {user_inflation_rate}%)")
        for stock_code in tqdm(stock_codes, desc="내재가치 계산"):
            intrinsic_value, error_reason = calculate_intrinsic_value_per_share(
                stock_code,
                calculation_base_date,
                user_bond_10yr_rate,
                user_inflation_rate
            )

            stock_name = stock_names.get(stock_code, "알 수 없음") # 종목명 가져오기
            results.append({
                '종목코드': stock_code,
                '종목명': stock_name,
                '기준일': calculation_base_date,
                '내재가치': intrinsic_value if intrinsic_value is not None else np.nan, # 계산 실패 시 NaN
                '계산상태': '성공' if intrinsic_value is not None else '실패',
                '실패사유': error_reason if intrinsic_value is None else ''
            })

            # 중간에 진행 상황을 보여주기 위한 로그 (너무 많으면 주석 처리)
            # if intrinsic_value is not None:
            #     print(f"  > {stock_code} ({stock_name}): 내재가치 {intrinsic_value:,.2f} 원")
            # else:
            #     print(f"  > {stock_code} ({stock_name}): 계산 실패 - {error_reason}")

        # 3. 결과 DataFrame 생성 및 CSV 파일로 저장
        print("\n계산 결과 DataFrame 생성 중...")
        results_df = pd.DataFrame(results)

        # CSV 파일명 생성 (오늘 날짜 포함)
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        output_filename = f'intrinsic_value_results_{today_str}.csv'

        results_df.to_csv(output_filename, index=False, encoding='utf-8-sig') # 한글 깨짐 방지

        print(f"\n--- 전체 내재가치 계산 완료 ---")
        print(f"결과가 '{output_filename}' 파일로 저장되었습니다.")
        print(f"성공적으로 계산된 종목 수: {results_df['내재가치'].notna().sum()} / {len(results_df)}")

    print("\n스크립트 실행 완료.")
