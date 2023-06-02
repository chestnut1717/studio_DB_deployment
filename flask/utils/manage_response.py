import pandas as pd
from typing import List, Dict, Tuple
from utils.db_connector import DBManagement
import os
import math

current_file_path = os.path.abspath(__file__)
root_path = os.path.dirname(os.path.dirname(current_file_path))

# rds에서 주소에 대한 정보 가져오기
def request_to_rds(facilities_type: List[str], lat: float, lon: float, radius_meter: int) -> List:

    # Connect to RDS
    path = os.path.join(root_path, "secret_key", "db_info.txt")
    db_info_dict = DBManagement.get_db_info(path)
    dbm = DBManagement(**db_info_dict)
    print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

    # Make POINT type location based on EPSG4326 
    location = f'ST_GeomFromText("POINT({lat} {lon})", 4326)'

    # Query 저장
    radius_query_list = []

    for facility in facilities_type:
        ## bus같은경우는 주소가 저장 안되어있으니 일단 NULL로 다 채우기
        if facility == 'bus':
            radius_query = f"""
            SELECT StationName AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, NULL AS address, lat, lon
            FROM {facility}
            WHERE ST_Contains(ST_Buffer({location}, {radius_meter}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius_meter}
            """
        elif facility == 'metro':
            radius_query = f"""
            SELECT StationName AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, roadAddress AS address, lat, lon
            FROM {facility}
            WHERE ST_Contains(ST_Buffer({location}, {radius_meter}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius_meter}
            """
        else:
            radius_query = f"""
            SELECT bplcNm AS Name, '{facility}' AS Kind, ST_Distance_Sphere({location}, coordinates) AS distance, rdnWhlAddr AS address, lat, lon
            FROM {facility}
            WHERE ST_Contains(ST_Buffer({location}, {radius_meter}), coordinates) AND ST_Distance_Sphere({location}, coordinates) < {radius_meter}
            """
        
        radius_query_list.append(radius_query)

    radius_query = " UNION ALL".join(radius_query_list) + "ORDER BY distance;"

    # execute
    dbm.cursor.execute(radius_query)
    query_result = dbm.cursor.fetchall()
    total_count = len(query_result)

    facility_body = {facility : {"count": 0, "place": []} for facility in facilities_type}
    
    for row in query_result:
        kind = row[1]
        facility_body[kind]['place'].append(
                                            {'name': row[0],
                                            'distance':int(row[2]),
                                            'address': row[3],
                                            'lat': row[4],
                                            'lon': row[5]
                                            })
        facility_body[kind]['count'] += 1

    hashtag_list = find_hashtag(facility_body)
    response_list = [total_count, facility_body, hashtag_list]

    return response_list

def initialize_dataframe(data_dict: dict, key: str) -> pd.DataFrame:
    try:
        dataframe = pd.DataFrame(data_dict[key]['place'])
    except:
        dataframe = pd.DataFrame()
    
    return dataframe

def find_hashtag(location_dict: dict ) -> List[str]:

    hashtag_list = []

    # 데이터 초기화
    dataframes = {}
    categories = ['hospital', 'pharmacy', 'laundry', 'hair', 'gym', 'mart', 'convenience', 'cafe', 'bus', 'metro']

    for category in categories:
        dataframes[category] = initialize_dataframe(location_dict, category)

    
    # 헬스장
    if not dataframes['gym'].empty and dataframes['gym']['name'].str.contains('헬스|짐|gym|피트니스|휘트니스|fitness|PT|피티', regex=True).any():
        hashtag_list.append("#헬스장")
        
    # 코인빨래방
    if not dataframes['laundry'].empty and dataframes['laundry']['name'].str.contains('코인|크린토피아|셀프|24', regex=True).any():
        hashtag_list.append("#코인빨래방")
    
    #대형쇼핑몰
    if len(dataframes['mart']) >= 1:
        hashtag_list.append("#마트/쇼핑몰")
    
    #편세권
    if len(dataframes['convenience']) >= 3:
        hashtag_list.append("#편세권")
    
    # 스세권
    if not dataframes['cafe'].empty and dataframes['cafe']['name'].str.contains('스타벅스', regex=True).any():
        hashtag_list.append("#스세권")
        
    # 역세권
    if len(dataframes['metro']) >= 3:
        hashtag_list.append("#초역세권")
    elif len(dataframes['metro']) >= 1:
        hashtag_list.append('#역세권')
        
        
    return hashtag_list

def calculate_score(facilities_type: List[str], total_count: int, facility_body: pd.DataFrame) ->Tuple[Dict, float]:

    # 가중치로 활용하기 위한 각 업종별 전체 데이터 개수(나중에 자동화 하기)
    weight = {
            'bus': 200000,
            'cafe': 51000,
            'convenience': 52000,
            'gym': 35000,
            'hair': 180000,
            'hospital': 70000,
            'mart':2700,
            'laundry': 20000,
            'pharmacy': 24000
            }
    
    # 지하철은 제외
    if 'metro' in facilities_type:
        facilities_type.remove('metro')


    weight_series = pd.Series(weight)

    cnt_series = pd.Series( {type: facility_body[type]['count'] for type in facilities_type} )
    print('count')
    print(cnt_series)
    
    # 전체 중 비율 고려한 수치 (30%)
    rate_series = (cnt_series / total_count * 100) * 0.3
    print('비율')
    print(rate_series)

    import numpy as np
    # 가중치 고려한 보정 개수 수치 (70%)
    weighted_cnt_series = (cnt_series * ( sum(weight_series) / weight_series[facilities_type] )) * 0.7
    weighted_cnt_series = weighted_cnt_series.apply(lambda x: np.log(x) / np.log(2) if x > 1 else 0)
    print('가중치')
    print(weighted_cnt_series)

    # 개별 score - 소수 첫째자리까지 반올림
    individual_score = dict(round(rate_series + weighted_cnt_series, 1))
    # 총 점수 = 평균 - 소수 첫째자리까지 반올림
    total_score = round(sum(individual_score.values()) / len(facilities_type), 1)
    print('total')
    print(individual_score)
    print('-'* 30)



    return individual_score, total_score

