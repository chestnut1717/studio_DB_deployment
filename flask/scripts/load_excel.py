"""
- Convert Bulk Excel localdata(.xlsx) to .csv)
- encoding : cp949
- source = https://www.localdata.go.kr/devcenter/dataDown.do?menuNo=20001
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from utils.load_data import RequestLocalData

current_file_path = os.path.abspath(__file__)
root_path = os.path.dirname(os.path.dirname(current_file_path))

if __name__ == "__main__":
    
    # path

    excel_data_path = os.path.join(root_path, 'data', 'local_excel_data')
    csv_data_path = os.path.join(root_path, 'data', 'local_csv_data')

    # extract folder name in excel_data_path to traverse inside each folder
    folder_names_list = RequestLocalData.get_folder_names(excel_data_path)


    # per service(ex.hospital, hair, ...)
    for folder_name in folder_names_list:
        dataframe_list = []
        folder_path = os.path.join(excel_data_path, folder_name)

        # 각 폴더 내 파일들 찾기 위함(ex. 1_hospital 내 병원 excel파일들)
        file_list = RequestLocalData.get_folder_names(folder_path)

        for file_name in file_list:

            # 한 파일이 여러 sheet로 구성되어 있을 경우, 모든 sheet 모음
            base = pd.DataFrame()
            file_path = os.path.join(folder_path, file_name)
            for i in range(10):
                try:
                    file = pd.read_excel(file_path, sheet_name = i)
                    base = base.append(file, ignore_index=True)
                except:
                    break

            dataframe_list.append(base)

        # file들 다 채우면 통합할 일만 남았다       
        base = pd.DataFrame()

        for tmp in dataframe_list:
            base = base.append(tmp, ignore_index=True)

        # index reset
        base.reset_index(drop=True, inplace=True)

        # save csv file
        csv_filename = f"{os.path.join(csv_data_path, folder_name)}1.csv"
        base.to_csv(csv_filename, index=False,encoding='cp949')

        del dataframe_list

        print(f"{folder_name} 작업 완료")