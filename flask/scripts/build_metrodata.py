import sys
import os
import warnings
warnings.filterwarnings(action='ignore')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import time
import math
from tqdm import tqdm

from utils.load_data import RequestMetroData
from utils.preprocess import MetroDataPreprocess
from utils.db_connector import DBManagement

current_file_path = os.path.abspath(__file__)
root_path = os.path.dirname(os.path.dirname(current_file_path))


if __name__ == "__main__":
    # key 파일 열기
    auth_key_metro_path = os.path.join(root_path, "secret_key", "auth_key_metro.txt")

    with open(auth_key_metro_path, 'r') as f:
        key_list = f.readlines()
        auth_key_metro = key_list[0].split(":")[1].strip()
    
    # 1. search line for each metro company
    req_metro = RequestMetroData(auth_key_metro)
    result = req_metro.get_metro_cd()

    # 2. reques API server
    metro_df = req_metro.get_apidata()
    print('지하철데이터 수집 완료')

    # 3. preprocess
    metro_columns_path = os.path.join(root_path, "info", "columns", "metro_columns.csv")
    MetroDataPreprocess.set_columns_dict(path=metro_columns_path)
    metro_df_after = MetroDataPreprocess.preprocess(metro_df)

    # 4. data to db

    ## db connection
    db_info_path = os.path.join(root_path, 'secret_key', 'db_info.txt')
    db_info_dict = DBManagement.get_db_info(db_info_path)
    dbm = DBManagement(**db_info_dict)
    print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

    ## table 정보가 담겨있는 파일 path
    metro_table_path = os.path.join(root_path,"info", 'schema', 'metro_table.csv')
    metro_table_df = dbm.set_table(metro_table_path)

    ## take columns which to DB
    metro_table_columns = dbm.get_columns(metro_table_df)

    metro_df_after.iloc[:, :-2] = metro_df_after.iloc[:, :-2].applymap(DBManagement.replace_quote)
    
    ## Create table
    table_name = 'metro'
    dbm.create_table(table_name, metro_table_df)
    dbm.commit()

    ## insert DB
    pivot = 40
    n = 0

    # 딱 떨어지는 경우도 있기 때문에 이렇게 해줘야 함
    iteration = math.ceil(len(metro_df_after) / pivot)

    for _ in tqdm(range(iteration)):
        tmp_df = metro_df_after.iloc[n:n+pivot]
        dbm.insert_record(table_name, tmp_df, metro_table_columns)
        n += pivot
        if _ % 60 == 0:
            time.sleep(1)

    dbm.commit()

    dbm.create_spatial_index(table_name, 'coordinates')
    dbm.commit()

    print("지하철데이터 작업 완료")
    dbm.cursor.close()



