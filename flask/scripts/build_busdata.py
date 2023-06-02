import sys
import os
import warnings
warnings.filterwarnings(action='ignore')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import time
import math
from tqdm import tqdm

from utils.load_data import RequestSeoulBusData, RequestOtherBusData
from utils.preprocess import SeoulBusDataPreprocess, OtherBusDataPreprocess
from utils.db_connector import DBManagement

current_file_path = os.path.abspath(__file__)
root_path = os.path.dirname(os.path.dirname(current_file_path))


if __name__ == "__main__":
    # key 파일 열기
    auth_key_bus_path = os.path.join(root_path, "secret_key", "auth_key_bus.txt")

    with open(auth_key_bus_path, 'r') as f:
        key_list = f.readlines()
        auth_key_seoul = key_list[0].split(":")[1].strip()
        auth_key_other = key_list[1].split(":")[1].strip()

    # 원래 데이터 column들 중, 필요한 column만 명세해둔 파일 path
    seoul_bus_columns_path = os.path.join(root_path, "info", "columns", "seoul_bus_columns.csv")
    other_bus_columns_path = os.path.join(root_path, "info", "columns", "other_bus_columns.csv")

    # table 정보가 담겨있는 파일 path
    bus_table_path = os.path.join(root_path,"info", "schema", "bus_table.csv")

    # 1. data request to bus API
    ## get total bus dataframe
    req_bus_seoul = RequestSeoulBusData(auth_key_seoul)
    req_bus_other = RequestOtherBusData(auth_key_other)

    other_bus_df = req_bus_other.get_apidata()
    seoul_bus_df = req_bus_seoul.get_apidata()

    # # 2. data preprocess
    SeoulBusDataPreprocess.set_columns_dict(seoul_bus_columns_path)
    seoul_bus_df_after = SeoulBusDataPreprocess.preprocess(seoul_bus_df)


    OtherBusDataPreprocess.set_columns_dict(other_bus_columns_path)
    other_bus_df_after = OtherBusDataPreprocess.preprocess(other_bus_df)

    # 3. data concat
    total_bus_df = pd.concat([seoul_bus_df_after, other_bus_df_after]).reset_index(drop=True)

    # 4. data to csv
    bus_csv_path = os.path.join(root_path, "data", "bus_data", "bus_station.csv")
    total_bus_df.to_csv(bus_csv_path, index=False)

    # 5. data to DB

    ## db connection
    db_info_path = os.path.join(root_path, 'secret_key', 'db_info.txt')
    db_info_dict = DBManagement.get_db_info(db_info_path)
    dbm = DBManagement(**db_info_dict)
    print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

    ## Bring table dataframe to make table
    bus_table_df = dbm.set_table(bus_table_path)

    ## take columns which to DB
    bus_table_columns = dbm.get_columns(bus_table_df)

    # Create table
    table_name = 'bus'
    dbm.create_table(table_name, bus_table_df)
    dbm.commit()

    total_bus_df.iloc[:, :-2] = total_bus_df.iloc[:, :-2].applymap(DBManagement.replace_quote)

    # DB에 데이터 넣기
    pivot = 40
    n = 0
    iteration = math.ceil(len(total_bus_df) / pivot)

    for _ in tqdm(range(iteration)):
        tmp_df = total_bus_df.iloc[n:n+pivot]
        dbm.insert_record(table_name, tmp_df, bus_table_columns)
        n += pivot
        if _ % 60 == 0:
            time.sleep(1)

    dbm.commit()

    # create spatial index
    dbm.create_spatial_index(table_name, 'coordinates')
    dbm.commit()

    print("버스데이터 작업 완료")
    dbm.cursor.close()



