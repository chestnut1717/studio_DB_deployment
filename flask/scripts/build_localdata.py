"""
- Build Localdata to MySQL
- target : Bulk data(.csv)
"""

import sys
import os
import warnings
warnings.filterwarnings(action='ignore')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import time
import math
from tqdm import tqdm

from utils.load_data import RequestLocalData
from utils.preprocess import LocalDataPreprocess
from utils.db_connector import DBManagement

current_file_path = os.path.abspath(__file__)
root_path = os.path.dirname(os.path.dirname(current_file_path))


if __name__ == "__main__":

    # db connection
    db_info_path = os.path.join(root_path, 'secret_key', 'db_info.txt')
    db_info_dict = DBManagement.get_db_info(db_info_path)
    dbm = DBManagement(**db_info_dict)
    print(f'성공적으로 MySQL {db_info_dict["database"]} 데이터베이스에 연결 완료')

    # bring info for localdata table columns
    localdata_columns_path = os.path.join(root_path, "info", "columns", "localdata_columns.csv")
    LocalDataPreprocess.set_columns_dict(localdata_columns_path)

    # table 정보가 담겨있는 파일 path
    localdata_table_path = os.path.join(root_path, "info", "schema", "localdata_table.csv")
    localdata_table_df = dbm.set_table(localdata_table_path)
    localdata_table_columns = dbm.get_columns(localdata_table_df)

    # excel_data_path
    excel_data_path = os.path.join(root_path, 'data', 'local_excel_data')
    csv_data_path = os.path.join(root_path, 'data', 'local_csv_data')
    folder_names_list = RequestLocalData.get_folder_names(excel_data_path)

    # 데이터 전처리
    for folder_name in folder_names_list:
        # 파일 경로
        file_path = f"{os.path.join(csv_data_path, folder_name)}.csv"

        # 데이터 로드
        data_df = RequestLocalData.get_csvdata(path = file_path)

        # 전처리
        data_preprocess_df = LocalDataPreprocess.preprocess_bulk(data_df)

        # table 생성
        table_name = folder_name[3:]
        dbm.create_table(table_name, localdata_table_df)
        dbm.commit()
        
        # replace_quote
        data_preprocess_df.iloc[:, :-2] = data_preprocess_df.iloc[:, :-2].applymap(DBManagement.replace_quote)

        # DB에 데이터 넣기
        pivot = 40
        n = 0
        iteration = math.ceil(len(data_preprocess_df) / pivot)
        
        for _ in tqdm(range(iteration)):
            tmp_df = data_preprocess_df.iloc[n:n+pivot]
            dbm.insert_record(table_name, tmp_df, localdata_table_columns)
            n += pivot
            if _ % 60 == 0:
                time.sleep(1)

        dbm.commit()
        
        # create spatial index
        dbm.create_spatial_index(table_name, 'coordinates')
        dbm.commit()
        

        print(f"{folder_name} 작업 완료")

    print('localdata 작업 완료')
    dbm.cursor.close()