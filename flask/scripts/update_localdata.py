"""
- Update Localdata to request API
- period : per day
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
    localdata_table_path = os.path.join(root_path,"info", "schema", "localdata_table.csv")
    localdata_table_df = dbm.set_table(localdata_table_path)
    localdata_table_columns = dbm.get_columns(localdata_table_df)

    # excel_data_path
    excel_data_path = os.path.join(root_path, 'data', 'local_excel_data')
    csv_data_path = os.path.join(root_path, 'data', 'local_csv_data')
    folder_names_list = RequestLocalData.get_folder_names(excel_data_path)

    # key 파일 열기
    auth_key_local_path = os.path.join(root_path, "secret_key", "auth_key_local.txt")

    with open(auth_key_local_path, 'r') as f:
        key_list = f.readlines()
        auth_key_local = key_list[0].split(":")[1].strip()

    # API 호출코드

    # request 객체
    api_request = RequestLocalData(auth_key_local)

    for folder_name in folder_names_list:
        
        # csv파일에 해당하는 서비스이름 파일 호출
        path = os.path.join(root_path, 'data', 'local_excel_data', folder_name)
        service_names = RequestLocalData.get_service_names(path=path)

        # 하나의 folder name에 서비스의 데이터가 있는 경우 count
        cnt = 0
        service_dataframe = pd.DataFrame()
        # 각 서비스별로 api 요청
        for service_name in service_names:

            info = {'authKey': api_request.auth_key,
                    'resultType': 'json',
                    'lastModTsBgn' : api_request.start_date,
                    'lastModTsEnd' : api_request.end_date,
                    'pageIndex' : 1,
                    'pageSize': 500,
                    'opnSvcId': service_name}
            
            api_df = api_request.get_apidata(info=info)
            
            if api_df is None:
                continue

            api_preprocess_df = LocalDataPreprocess.preprocess(api_df)
            service_dataframe = service_dataframe.append(api_preprocess_df, ignore_index=True)
            cnt += 1
            
            time.sleep(1.5)
        
        # 한 데이터 자체가 비어있으면, 다음 인허가데이터로 넘어가기
        if len(service_dataframe) <= 0:
            print(f"{folder_name}에는 데이터가 없습니다.")
            continue

        # 초기 값 넣기
        service_dataframe.reset_index(drop=True, inplace=True)

        # table name
        table_name = folder_name[3:]

        print(f"갱신 전 전체 데이터 개수 : {dbm.table_size(table_name)}")
        # # 테이블에 넣기 전, quote 처리
        service_dataframe.iloc[:, :-2] = service_dataframe.iloc[:, :-2].applymap(DBManagement.replace_quote)
        # # 삭제할 데이터, 추가할 데이터를 골라낸다
        deleted_df = service_dataframe[~service_dataframe['trdStateGbn'].isin(['"1"', '"2"'])]
        updated_df = service_dataframe[service_dataframe['trdStateGbn'].isin(['"1"', '"2"'])]


        # deleted
        values_all = deleted_df[['opnSfTeamCode', 'mgtNo', 'opnSvcId']].to_records(index=False).tolist()

        for opnSfTeamCode, mgtNo, opnSvcId in values_all:
            dbm.delete_record(table_name = table_name, opnSfTeamCode=opnSfTeamCode, mgtNo=mgtNo, opnSvcId=opnSvcId)
            dbm.commit()
        

        # DB 데이터 갱신
        pivot = min(40, len(updated_df)) if len(updated_df) > 0 else 40
        n = 0
        iteration = math.ceil(len(updated_df) / pivot)
        
        for _ in tqdm(range(iteration)):
            tmp_df = updated_df.iloc[n:n+pivot]
            dbm.update_record(table_name, tmp_df, localdata_table_columns)
            dbm.commit()
            n += pivot
            if _ % 60 == 0:
                time.sleep(1)
        

        print(f"갱신 후 전체 데이터 개수: {dbm.table_size(table_name)}")
        print(f"{folder_name} 완료\n")

    dbm.cursor.close()



