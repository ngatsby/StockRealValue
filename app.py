import streamlit as st
import pymysql
from sqlalchemy import create_engine
import pandas as pd # pandas는 여기서는 직접적으로 사용되지 않지만, import 그대로 둡니다.
import numpy as np # numpy도 마찬가지
import datetime

# ----------------- 기존 DB 연결 및 계산 함수 코드 -----------------

# DB 연결 설정
# Streamlit 배포 시 환경 변수 사용을 권장하지만, 일단은 하드코딩으로 진행합니다.
# 실제 배포 시에는 st.secrets를 사용하는 것이 안전합니다.
DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '3037',
    'database': 'stock_db',
    'charset': 'utf8'
}

# SQLAlchemy 엔진 설정 (여기서는 직접 사용하지 않음)
# Streamlit 앱에서는 세션마다 DB 연결을 새로 하므로, 이 엔진은 크게 의미가 없을 수 있습니다.
# 하지만 코드 구조 유지를 위해 남겨둡니다.
engine = create_engine(f'mysql+pymysql://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:3306/{DB_CONFIG["database"]}')

# @st.cache_data 데코레이터를 사용하여 DB 조회 결과를 캐싱합니다.
# 동일한 인자로 호출될 경우 DB에 다시 접근하지 않고 캐시된 결과를 반환하여 성능을 높입니다.
@st.cache_data(ttl=3600) # 1시간 (3600초) 동안 캐시 유지
def get_financial_data(stock_code, base_date, account_name, public_type='y'):
    """
    kor_fs 테이블에서 특정 계정의 값을 가져오는 헬퍼 함수.
    공시구분(public_type)을 지정할 수 있으며, 값이 없을 경우 None 반환.
    Streamlit 캐싱을 위해 cursor 인자를 제거하고 내부에서 conn/cursor를 관리합니다.
    """
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = f"""
            SELECT 값
            FROM kor_fs
            WHERE 종목코드 = '{stock_code}'
              AND 기준일 = '{base_date}'
              AND 계정 = '{account_name}'
              AND 공시구분 = '{public_type}'
            LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        if result and result['값'] is not None:
            return float(result['값'])
        return None
    except pymysql.Error as err:
        st.error(f"데이터베이스 오류 (kor_fs): {err}")
        return None
    except Exception as e:
        st.error(f"예상치 못한 오류 (kor_fs): {e}")
        return None
    finally:
        if conn and conn.open:
            conn.close()

@st.cache_data(ttl=3600) # 1시간 (3600초) 동안 캐시 유지
def get_ticker_data(stock_code, column_name):
    """
    kor_ticker 테이블에서 특정 컬럼의 값을 가져오는 헬퍼 함수.
    기준일 조건 없이 종목코드로만 조회하며, 값이 없을 경우 None 반환.
    Streamlit 캐싱을 위해 cursor 인자를 제거하고 내부에서 conn/cursor를 관리합니다.
    """
    conn = None
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        query = f"""
            SELECT `{column_name}`
            FROM kor_ticker
            WHERE 종목코드 = '{stock_code}'
            LIMIT 1
        """
        cursor.execute(query)
        result = cursor.fetchone()
        if result and result[column_name] is not None:
            return float(result[column_name])
        return None
    except pymysql.Error as err:
        st.error(f"데이터베이스 오류 (kor_ticker): {err}")
        return None
    except Exception as e:
        st.error(f"예상치 못한 오류 (kor_ticker): {e}")
        return None
    finally:
        if conn and conn.open:
            conn.close()

def calculate_intrinsic_value_per_share(stock_code, base_date, bond_10yr_rate_input, inflation_rate_input):
    """
    주어진 종목코드와 기준일에 대해 주당 내재가치를 계산합니다.
    Streamlit 앱에서는 에러 메시지를 st.error로 출력합니다.
    """
    
    # 캐싱된 헬퍼 함수들을 사용하므로, calculate_intrinsic_value_per_share 자체는 캐싱하지 않습니다.
    # 이 함수는 사용자의 입력값에 따라 매번 다른 결과를 반환할 수 있기 때문입니다.

    st.info(f"--- 데이터 조회 및 계산 시작 (종목코드: {stock_code}, 기준일: {base_date}) ---")

    # kor_fs의 '값'이 '억원' 단위라고 가정하고, '원' 단위로 변환
    UNIT_MULTIPLIER_FS = 100000000 # 억원 -> 원

    # 1. 조정자본총계 계산
    total_assets_raw = get_financial_data(stock_code, base_date, '자산') 
    if total_assets_raw is None:
        st.error(f"오류: '자산' 데이터(공시구분 'y')를 찾을 수 없거나 값이 없습니다. 조정자본총계 계산 불가.")
        return None
    total_assets = total_assets_raw * UNIT_MULTIPLIER_FS # 원 단위로 변환
    st.write(f"  - 총자산 (kor_fs에서 조회): {total_assets:,.0f} (원 단위)")


    total_liabilities_raw = get_financial_data(stock_code, base_date, '부채') 
    if total_liabilities_raw is None:
        st.error(f"오류: '부채' 데이터(공시구분 'y')를 찾을 수 없거나 값이 없습니다. 조정자본총계 계산 불가.")
        return None
    total_liabilities = total_liabilities_raw * UNIT_MULTIPLIER_FS # 원 단위로 변환
    st.write(f"  - 총부채 (kor_fs에서 조회): {total_liabilities:,.0f} (원 단위)")


    # 운영자산 계산: 자산 - (영업권 + 기타장기자산)
    goodwill = 0 
    other_long_term_assets = 0
    operating_assets = total_assets - (goodwill + other_long_term_assets)
    st.write(f"  - 운영자산 ({total_assets:,.0f} (자산) - (영업권 {goodwill:,.0f} (없음) + 기타장기자산 {other_long_term_assets:,.0f} (없음))) = {operating_assets:,.0f} (원 단위)")

    # 운영부채 계산: 부채 - (기타장기부채 + 이연법인세부채)
    other_long_term_liabilities = 0
    deferred_tax_liabilities_raw = get_financial_data(stock_code, base_date, '이연법인세부채')
    deferred_tax_liabilities = deferred_tax_liabilities_raw * UNIT_MULTIPLIER_FS if deferred_tax_liabilities_raw is not None else 0
    
    if deferred_tax_liabilities_raw is None:
         st.warning(f"  - 경고: '이연법인세부채' 데이터(공시구분 'y')를 찾을 수 없거나 값이 없어 0으로 처리합니다.")

    operating_liabilities = total_liabilities - (other_long_term_liabilities + deferred_tax_liabilities)
    st.write(f"  - 운영부채 ({total_liabilities:,.0f} (부채) - (기타장기부채 {other_long_term_liabilities:,.0f} (없음) + 이연법인세부채 {deferred_tax_liabilities:,.0f})) = {operating_liabilities:,.0f} (원 단위)")
    
    adjusted_capital = operating_assets - operating_liabilities
    st.write(f"  - 조정자본총계 (운영자산 {operating_assets:,.0f} - 운영부채 {operating_liabilities:,.0f}) = {adjusted_capital:,.0f} (원 단위)")


    # 2. 주주이익 계산 (자본효율 계산에 필요)
    net_income_raw = get_financial_data(stock_code, base_date, '당기순이익')
    depreciation_raw = get_financial_data(stock_code, base_date, '감가상각비')
    capex_raw = get_financial_data(stock_code, base_date, '유형자산의증가') # CAPEX로 가정

    net_income = net_income_raw * UNIT_MULTIPLIER_FS if net_income_raw is not None else 0
    depreciation = depreciation_raw * UNIT_MULTIPLIER_FS if depreciation_raw is not None else 0
    capex = capex_raw * UNIT_MULTIPLIER_FS if capex_raw is not None else 0

    if net_income_raw is None: st.warning(f"  - 경고: '당기순이익' 데이터(공시구분 'y')를 찾을 수 없어 주주이익 계산에 0으로 처리합니다.")
    if depreciation_raw is None: st.warning(f"  - 경고: '감가상각비' 데이터(공시구분 'y')를 찾을 수 없어 주주이익 계산에 0으로 처리합니다.")
    if capex_raw is None: st.warning(f"  - 경고: '유형자산의증가' 데이터(공시구분 'y')를 찾을 수 없어 주주이익 계산에 0으로 처리합니다.")

    shareholder_profit = net_income + depreciation - capex
    st.write(f"  - 주주이익 (당기순이익 {net_income:,.0f} + 감가상각비 {depreciation:,.0f} - CAPEX(유형자산의증가) {capex:,.0f}) = {shareholder_profit:,.0f} (원 단위)")

    # 3. 자본효율 계산: 주주이익 / 조정자본총계
    if adjusted_capital == 0:
        st.error("오류: 조정자본총계가 0이므로 자본효율을 계산할 수 없습니다. (나누기 0 오류 방지)")
        return None
    capital_efficiency = shareholder_profit / adjusted_capital
    st.write(f"  - 자본효율 (주주이익 {shareholder_profit:,.0f} / 조정자본총계 {adjusted_capital:,.0f}) = {capital_efficiency:,.4f}")

    # 4. 할인율 계산: 채권 10년물 금리 + 인플레이션
    # 인자로 받은 값을 소수점으로 변환하여 사용 (예: 3.0% -> 0.030)
    bond_10yr_rate = bond_10yr_rate_input / 100.0 
    inflation_rate = inflation_rate_input / 100.0

    discount_rate = bond_10yr_rate + inflation_rate
    st.write(f"  - 할인율 (채권 10년물 금리 {bond_10yr_rate * 100:,.2f}% + 인플레이션 {inflation_rate * 100:,.2f}%) = {discount_rate * 100:,.2f}%")

    if discount_rate == 0:
        st.error("오류: 계산된 할인율이 0이므로 자본배수를 계산할 수 없습니다. (나누기 0 오류 방지)")
        return None

    # 5. 자본배수 계산: 자본효율 / 할인율 (공식 변경)
    capital_multiplier = capital_efficiency / discount_rate
    st.write(f"  - 자본배수 (자본효율 {capital_efficiency:,.4f} / 할인율 {discount_rate:,.4f}) = {capital_multiplier:,.4f}")


    # 6. 주식수 (자사주 제외) 계산
    # kor_ticker 테이블에서 '종가'와 '시가총액' 조회하여 주식수 계산 (기준일 조건 제거)
    current_price = get_ticker_data(stock_code, '종가')
    market_cap = get_ticker_data(stock_code, '시가총액') 

    if current_price is None:
        st.error("오류: 'kor_ticker' 테이블에서 종가 데이터를 찾을 수 없습니다. 주식수 계산 불가.")
        return None
    if market_cap is None:
        st.error("오류: 'kor_ticker' 테이블에서 시가총액 데이터를 찾을 수 없습니다. 주식수 계산 불가.")
        return None

    if current_price == 0:
        st.error("오류: 종가가 0이므로 주식수를 계산할 수 없습니다. (나누기 0 오류 방지)")
        return None
    
    total_shares = market_cap / current_price # 주식수 (단위: 주)
    shares_excluding_treasury = total_shares # 자사주 정보가 없으므로 총 주식수를 사용
    st.write(f"  - 총 주식수 (시가총액 {market_cap:,.0f} / 종가 {current_price:,.0f}) = {total_shares:,.0f} 주")

    if shares_excluding_treasury == 0:
        st.error("오류: 자사주 제외 주식수가 0이므로 주당 내재가치를 계산할 수 없습니다. (나누기 0 오류 방지)")
        return None

    # 최종 주당 내재가치 계산
    intrinsic_value_per_share = (adjusted_capital / shares_excluding_treasury) * capital_multiplier
    
    return intrinsic_value_per_share


# ----------------- Streamlit UI 구성 -----------------

st.set_page_config(page_title="주당 내재가치 계산기", layout="centered")
st.title("💰 주당 내재가치 계산기")
st.markdown("---")

st.header("입력 값")

# 종목코드 입력
stock_code_input = st.text_input("종목코드", value="005930", help="예: 005930 (삼성전자)")

# 기준일 입력 (kor_fs 조회용)
base_date_input = st.date_input("기준일 (YYYY-MM-DD)", value=datetime.date(2024, 12, 31), help="재무 데이터 조회 기준일")
# datetime.date 객체를 문자열로 변환하여 함수에 전달
base_date_str = base_date_input.strftime('%Y-%m-%d')

# 10년물 금리 및 인플레이션율 입력
col1, col2 = st.columns(2)
with col1:
    bond_10yr_rate_input = st.number_input(
        "채권 10년물 금리 (%)",
        min_value=0.0,
        max_value=20.0,
        value=3.0,
        step=0.1,
        help="예: 3.0 (3.0% 의미)"
    )
with col2:
    inflation_rate_input = st.number_input(
        "물가상승률 (%)",
        min_value=0.0,
        max_value=10.0,
        value=2.0,
        step=0.1,
        help="예: 2.0 (2.0% 의미)"
    )

st.markdown("---")

# 계산 실행 버튼
if st.button("내재가치 계산하기"):
    st.header("계산 과정 및 결과")
    # 스피너를 통해 계산 중임을 표시
    with st.spinner("데이터를 조회하고 내재가치를 계산 중입니다..."):
        intrinsic_value = calculate_intrinsic_value_per_share(
            stock_code_input,
            base_date_str,
            bond_10yr_rate_input,
            inflation_rate_input
        )

    st.markdown("---")
    st.header("최종 결과")
    if intrinsic_value is not None:
        st.success(f"종목코드 **{stock_code_input}**의 {base_date_str} 기준 주당 내재가치: **{intrinsic_value:,.2f} 원**")
        
        # 현재 주가를 입력받아 괴리율 계산 및 설명
        st.subheader("현재 주가 입력 및 괴리율 분석")
        current_market_price = st.number_input("현재 시장 주가 (원)", min_value=0, value=59100, step=100)
        
        if current_market_price > 0:
            price_difference = current_market_price - intrinsic_value
            if intrinsic_value != 0:
                deviation_ratio = (price_difference / intrinsic_value) * 100
                st.info(f"현재 주가 ({current_market_price:,.0f} 원)와 내재가치 ({intrinsic_value:,.0f} 원)의 차이: {price_difference:,.0f} 원")
                st.info(f"괴리율: {deviation_ratio:,.2f}%")

                st.subheader("괴리율 설명:")
                if deviation_ratio > 100: # 내재가치보다 2배 이상 높은 경우
                    st.warning("현재 주가가 계산된 내재가치보다 **매우 높습니다.** 이는 여러 요인으로 설명될 수 있습니다:")
                    st.markdown("""
                    - **성장 기대감**: 시장은 해당 기업의 미래 성장 가능성, 혁신적인 기술, 신사업 진출 등에 큰 기대를 하고 있을 수 있습니다.
                    - **프리미엄**: 독점적 시장 지위, 강력한 브랜드 가치, 우수한 경영진 등에 대한 프리미엄이 반영되었을 수 있습니다.
                    - **시장 심리**: 단기적인 시장의 과열, 특정 섹터에 대한 집중적인 투자, 투기적 수요 등이 주가를 끌어올렸을 수 있습니다.
                    - **모델의 한계**: 현재 사용된 내재가치 평가 모델이 기업의 모든 가치(무형자산, 성장성 등)를 충분히 반영하지 못했을 가능성이 있습니다. 특히, `자본배수`의 독특한 정의나 `주주이익` 계산 방식이 실제 기업의 가치 창출을 완전히 반영하지 못할 수 있습니다.
                    - **데이터의 시차**: `kor_fs` 데이터는 보통 분기 또는 연간 단위로 업데이트되므로, 최신 시장 상황을 반영하지 못할 수 있습니다.
                    """)
                elif deviation_ratio < -50: # 내재가치보다 50% 이상 낮은 경우
                    st.success("현재 주가가 계산된 내재가치보다 **매우 낮습니다.** 이는 다음과 같은 이유일 수 있습니다:")
                    st.markdown("""
                    - **과도한 비관론**: 시장이 기업의 현재 어려움이나 부정적인 전망에 과도하게 반응하여 주가가 저평가되었을 수 있습니다.
                    - **미래 불확실성**: 규제 변화, 산업 구조 변화, 경쟁 심화 등 불확실성 요인이 주가에 부정적으로 작용했을 수 있습니다.
                    - **숨겨진 가치**: 시장이 아직 인식하지 못하는 기업의 숨겨진 자산이나 잠재력이 있을 수 있습니다.
                    - **모델의 한계**: 마찬가지로 모델이 기업의 가치를 과소평가했을 가능성도 있습니다.
                    """)
                else:
                    st.info("현재 주가와 내재가치 간의 괴리가 상대적으로 합리적인 수준일 수 있습니다. 하지만 여전히 다음을 고려해야 합니다:")
                    st.markdown("""
                    - **가정의 적절성**: 사용된 채권 금리, 인플레이션율, 그리고 `자본효율`, `자본배수` 공식에 대한 가정이 해당 기업과 시장에 적절한지 재검토가 필요합니다.
                    - **시장 효율성**: 주식 시장은 항상 합리적으로 움직이지 않으며, 단기적으로는 수급이나 심리에 의해 가격이 변동할 수 있습니다.
                    """)
            else:
                st.warning("내재가치가 0이므로 괴리율을 계산할 수 없습니다. 모델 계산 과정에서 오류가 발생했거나, 데이터가 불충분할 수 있습니다.")
    else:
        st.error(f"종목코드 {stock_code_input}의 {base_date_str} 기준 주당 내재가치를 계산하지 못했습니다. 입력 값과 로그를 확인해주세요.")

st.markdown("---")
st.caption("본 계산기는 제공된 재무 데이터와 공식에 기반하며, 실제 투자 결정에는 추가적인 분석과 전문가의 조언이 필요합니다.")