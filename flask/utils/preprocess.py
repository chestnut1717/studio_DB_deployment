import numpy as np
import pandas as pd
import pyproj

import re
from typing import List, Dict
from abc import *

class ManageLonLat():

    # return dataframe except rows which have invalid range lon and lat
    @classmethod
    def return_vaild_data(cls, df:pd.DataFrame) -> pd.DataFrame:
        df['lat'] = df['lat'].astype('float64')
        df['lon'] = df['lon'].astype('float64')
        df = df[(df['lat'] > 33.1) & (df['lat'] < 38.7) & (df['lon']> 124.59) & (df['lat'] < 131.88)]
        return df
    
    @ classmethod
    def project_array(cls, coord: np.array, input_type: str, output_type: str) -> np.array:
        """
        좌표계 변환 함수
        - coord: x, y 좌표 정보가 담긴 NumPy Array
        - p1_type: 입력 좌표계 정보 ex) epsg:2097
        - p2_type: 출력 좌표계 정보 ex) epsg:4326
        """
        # 보정된 중부원점(EPSG:5174)
        input = pyproj.Proj(init=input_type)
        output = pyproj.Proj(init=output_type)
        fx, fy = pyproj.transform(input, output, coord[:, 0], coord[:, 1])
        return np.dstack([fx, fy])[0]  


# 데이터 전처리
class DataPreprocess(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @staticmethod
    @abstractmethod
    def column_realign(df:pd.DataFrame, columns_dict: Dict) -> pd.DataFrame:
        """
        Realign columns to insert DB
        """
        pass

    @staticmethod
    @abstractmethod
    def replace_string(df: pd.DataFrame, replace_column: List[str]) -> pd.DataFrame:
        """
        Replace string by column to prevent error
        """
        pass

    @staticmethod
    @abstractmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str) -> pd.DataFrame:
        """
        Type change by column
        """
        pass

    @staticmethod
    @abstractmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace missing value to NaN(np.nan)
        """
        pass

    @staticmethod
    @abstractmethod
    def set_columns_dict(path: str) -> None:
        """
        Set columns how to fit
        """
        pass


class LocalDataPreprocess(DataPreprocess):
    columns_dict = {}

    # Decorator
    def preprocess_decorator(func):
        def decorated(df: pd.DataFrame) -> pd.DataFrame:
            """
            Preprocess Localdata
            1. Column realign
            2. Replace_string
            3. bulk => closed shop remove / not => type change
            4. Filter Cafe, Convenience store
            5. Replace NaN
            6. change epgs:5174 to ordinary coordinates(epsg:4326)
            7. remove data which have invalid lat, lon
            """
            
            # 1. Column realign 
            df = LocalDataPreprocess.column_realign(df, LocalDataPreprocess.columns_dict)
            df_name = df['opnSvcId'][0]

            # 2. Replace string by column
            ## 업체명, 주소에 따옴표로 인한 문제 발생 => 잘 치환
            replace_columns = ['bplcNm', 'siteWhlAddr', 'rdnWhlAddr']
            df = LocalDataPreprocess.replace_string(df, replace_columns)

            # 3. bulk => closed shop remove / not => type change
            df = func(df)

            # 4. Filter Cafe, Convenience store
            ## 휴게음식점인 경우에만 카페 필터링
            if df_name == '07_24_05_P':
                df = LocalDataPreprocess.filter_cafe(df)
            
            ## 담배소매업일 경우에만 편의점 필터링
            if df_name == '11_43_02_P':
                df = LocalDataPreprocess.filter_convenience_store(df)

            
            # 5. Replace NaN
            df = LocalDataPreprocess.replace_nan(df)

            # 6. change epgs:5174 to ordinary coordinates(epsg:4326)
            original_coord = np.array(df.loc[:, ['x', 'y']])
            input_type = "epsg:5174"
            output_type = "epsg:4326"

            transformed_coord = ManageLonLat.project_array(original_coord, input_type, output_type)
            df.loc[:, ['lon', 'lat']] = transformed_coord

            # 7.remove data which have invalid lat, lon
            df = ManageLonLat.return_vaild_data(df)

            df.reset_index(drop=True, inplace=True)

            return df
        
        return decorated

    @staticmethod
    @preprocess_decorator
    def preprocess_bulk(df: pd.DataFrame) -> pd.DataFrame:
        df = LocalDataPreprocess.remove_closed_shop(df)
        
        return df
    
    # decorator을 쓸 때, 무조건 @staticmethod가 상단에 있어야 한다!
    @staticmethod
    @preprocess_decorator
    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        # API로 가져온 데이터이면,특정 column의 type값을 바꿔줘야 함
        change_cols = ['trdStateGbn', 'dtlStateGbn']
        df = LocalDataPreprocess.type_change(df, change_cols, change_type=int)

        return df

    @staticmethod
    def column_realign(df:pd.DataFrame, columns_dict: Dict) -> pd.DataFrame:
        """
        Realign columns to insert DB
        """
        columns_values = list(columns_dict.values())
        new_df = df.rename(columns = columns_dict)[columns_values]
        
        return new_df        
    
    @staticmethod
    def replace_string(df: pd.DataFrame, replace_columns: List[str]) -> pd.DataFrame:
        for column in replace_columns:
            df[column] = df[column].map(LocalDataPreprocess.add_backslash, na_action='ignore')
        
        return df
    
    @staticmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str):

        for column in columns_list:
            try:
                df[column] = df[column].astype(change_type)
            
            # BBBB일 경우
            except ValueError:
                df.loc[df[column] == 'BBBB', column] = 13
                df[column] = df[column].astype(change_type)
        
        return df
    
    @staticmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace missing value to NaN(np.nan)
        """

        # 좌표값이 결측치인 경우, 공백인 경우를 모두 제거한다
        ## 좌표값이 공백인 경우를 모두 결측치로 변경
        df.loc[:, ['x', 'y']] = df.loc[:, ['x', 'y']].replace(r'^\s*$', np.nan, regex=True)

        ## 좌표값이 결측치인 경우 삭제 후 
        df = df[~df['x'].isna() & ~df['y'].isna()]

        # 나머지 값들은 결측치나 공백으로 이루어진 것을 null로 바꿈
        ## MySQL에서 NULL값으로 인식하도록 하기 위함
        df.fillna('NULL', inplace=True)
        df.replace(r'^\s*$', 'NULL', regex=True, inplace=True)
        
        return df

    @staticmethod
    def remove_closed_shop(df: pd.DataFrame) -> pd.DataFrame:
        df.drop(df[( df['trdStateGbn'] == 3 ) | ( df['trdStateGbn'] == 4 ) | ( df['trdStateGbn'] == 5 )].index, inplace=True)
        return df
      
    
    @staticmethod
    def filter_cafe(df: pd.DataFrame) -> pd.DataFrame:
        allowed_shop = ['과자점', '기타 휴게음식점', '다방', '아이스크림', '전통찻집', '커피숍']
        keywords = ['커피', '카페', 'coffee', '까페', '스타벅스', '이디야', '빽다방', '파스쿠찌', '투썸플레이스', '폴바셋', '할리스', '더벤티', '탐앤탐스', '매머드', '공차', '스킨라빈스', '와플대학', '던킨', '크리스피']
        restricted_keywords = ['PC']

        # Delete rows based on 업태구분
        df_new = df[df['uptaeNm'].isin(allowed_shop)]

        # Delete rows based on 사업장명
        df_new = df_new[df_new['bplcNm'].str.contains('|'.join(keywords), case=False)]

        # Delete rows that contain 'PC' in 사업장명
        df_new = df_new[~df_new['bplcNm'].str.contains('|'.join(restricted_keywords), case=False)]

        return df_new
    
    @staticmethod
    def filter_convenience_store(df: pd.DataFrame) -> pd.DataFrame:

        # Filter values in the '사업장명' column for the first file
        keywords = ['씨유', 'CU', '세븐', '지에스', 'GS', '이마트24', '미니스톱', 'MINISTOP']
        
        df_new = df[df['bplcNm'].str.contains('|'.join(keywords))]

        return df_new

    @staticmethod
    def add_backslash(text: str) -> str:
        pattern = r'(["\'])'  # 큰따옴표 또는 작은따옴표 패턴
        replace = r'\\\1'  # 백슬래시와 해당 따옴표

        return re.sub(pattern, replace, f'{text}') # 상호명이 숫자로만 이루어진 경우도 있었음(ex.676)
    
    @staticmethod
    def set_columns_dict(path: str) -> None:

        columns_df = pd.read_csv(path)
        from_list = list(columns_df['From'])
        to_list = list(columns_df['To'])
        columns_dict = {k: v for k, v in zip(from_list, to_list)}

        LocalDataPreprocess.columns_dict = columns_dict
    

class SeoulBusDataPreprocess(DataPreprocess):

    columns_dict = {}
    
    # decorator
    def preprocess_decorator(func):
        def decorated(df: pd.DataFrame) -> pd.DataFrame:
            """
            1. Column realign
            2. Replace String
            3. Replace NaN
            4. remove data which have invalid lat, lon
            """

            # 1. Column realign
            df = func(df)

            # 2.Replace String
            replace_column = ['StationName']
            df = SeoulBusDataPreprocess.replace_string(df, replace_column)

            # 3. Replace NaN
            df = SeoulBusDataPreprocess.replace_nan(df)
            
            # 4.remove data which have invalid lat, lon
            df = ManageLonLat.return_vaild_data(df)
            df.reset_index(drop=True, inplace=True)

            return df
        
        return decorated
    
    @staticmethod
    @preprocess_decorator
    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        
        # Realign columns for Seoul
        df = SeoulBusDataPreprocess.column_realign(df, SeoulBusDataPreprocess.columns_dict)

        return df

    
    @staticmethod
    def column_realign(df:pd.DataFrame, columns_dict: Dict) -> pd.DataFrame:
        """
        Realign columns to insert DB
        """
        
        columns_values = list(columns_dict.values())
        new_df = df.rename(columns = columns_dict)[columns_values]
   
        return new_df

    @staticmethod
    def replace_string(df: pd.DataFrame, replace_column: List[str]) -> pd.DataFrame:
        """
        Replace string by column to prevent error
        """

        before = '\u2024'
        after = '.'
        for column in replace_column:
            df[column]= df[column].str.replace(before, after)

        return df

    @staticmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str) -> pd.DataFrame:
        """
        Type change by column
        """
        pass

    @staticmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace missing value to NaN(np.nan)
        """

        # 좌표값이 결측치인 경우, 공백인 경우를 모두 제거한다
        ## 좌표값이 공백인 경우를 모두 결측치로 변경
        df.loc[:, ['lon', 'lat']] = df.loc[:, ['lon', 'lat']].replace(r'^\s*$', np.nan, regex=True)

        ## 좌표값/ StationID, StationName이 결측치인 경우 삭제 후 
        df = df[~df['lon'].isna() & ~df['lat'].isna() & ~df['StationID'].isna() & ~df['StationName'].isna()]


        # 나머지 값들은 결측치나 공백으로 이루어진 것을 null로 바꿈
        ## MySQL에서 NULL값으로 인식하도록 하기 위함
        df.fillna('NULL', inplace=True)
        df.replace(r'^\s*$', 'NULL', regex=True, inplace=True)

        return df
    
    @staticmethod
    def total_concat(df_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concat two dataframe to make total dataframe
        """

        total_df = pd.concat(df_list).reset_index(drop=True)

        return total_df
    
    @staticmethod
    def set_columns_dict(path: str) -> None:

        columns_df = pd.read_csv(path)
        from_list = list(columns_df['From'])
        to_list = list(columns_df['To'])
        columns_dict = {k: v for k, v in zip(from_list, to_list)}

        SeoulBusDataPreprocess.columns_dict = columns_dict


class OtherBusDataPreprocess(DataPreprocess):

    columns_dict = {}
    
    # decorator
    def preprocess_decorator(func):
        def decorated(df: pd.DataFrame) -> pd.DataFrame:
            """
            1. Column realign
            2. Replace String
            3. replace NaN
            4.remove data which have invalid lat, lon
            """

            # 1. Column realign
            df = func(df)

            # 2.Replace String
            replace_column = ['StationName']
            df = OtherBusDataPreprocess.replace_string(df, replace_column)

            # 3. replace NaN
            df = OtherBusDataPreprocess.replace_nan(df)
                   
            # 4.remove data which have invalid lat, lon
            df = ManageLonLat.return_vaild_data(df)

            df.reset_index(drop=True, inplace=True)

            return df
        
        return decorated
    
    @staticmethod
    @preprocess_decorator
    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        
        # Realign columns for Seoul
        df = OtherBusDataPreprocess.column_realign(df, OtherBusDataPreprocess.columns_dict)

        return df

    @staticmethod
    @preprocess_decorator
    def preprocess_other(df: pd.DataFrame) -> pd.DataFrame:
        
        # Realign columns for others
        df = OtherBusDataPreprocess.column_realign(df, OtherBusDataPreprocess.columns_dict)

        return df
    
    @staticmethod
    def column_realign(df:pd.DataFrame, columns_dict: Dict) -> pd.DataFrame:
        """
        Realign columns to insert DB
        """
        columns_values = list(columns_dict.values())
        new_df = df.rename(columns = columns_dict)[columns_values]
        
        return new_df

    @staticmethod
    def replace_string(df: pd.DataFrame, replace_column: List[str]) -> pd.DataFrame:
        """
        Replace string by column to prevent error
        """

        before = '\u2024'
        after = '.'
        for column in replace_column:
            df[column]= df[column].str.replace(before, after)

        return df

    @staticmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str) -> pd.DataFrame:
        """
        Type change by column
        """
        pass

    @staticmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace missing value to NaN(np.nan)
        """


        # 좌표값이 결측치인 경우, 공백인 경우를 모두 제거한다
        ## 좌표값이 공백인 경우를 모두 결측치로 변경
        df.loc[:, ['lon', 'lat']] = df.loc[:, ['lon', 'lat']].replace(r'^\s*$', np.nan, regex=True)

        ## 좌표값 / StationID, StationName이 결측치인 경우 삭제 후 
        df = df[~df['lon'].isna() & ~df['lat'].isna() & ~df['StationID'].isna() & ~df['StationName'].isna()]

        # 나머지 값들은 결측치나 공백으로 이루어진 것을 null로 바꿈
        ## MySQL에서 NULL값으로 인식하도록 하기 위함
        df.fillna('NULL', inplace=True)
        df.replace(r'^\s*$', 'NULL', regex=True, inplace=True)

        return df
    
    @staticmethod
    def total_concat(df_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Concat two dataframe to make total dataframe
        """

        total_df = pd.concat(df_list).reset_index(drop=True)

        return total_df
    
    @staticmethod
    def set_columns_dict(path: str) -> None:

        columns_df = pd.read_csv(path)
        from_list = list(columns_df['From'])
        to_list = list(columns_df['To'])
        columns_dict = {k: v for k, v in zip(from_list, to_list)}

        OtherBusDataPreprocess.columns_dict = columns_dict


class MetroDataPreprocess(DataPreprocess):
    @staticmethod
    def preprocess(df: pd.DataFrame) -> pd.DataFrame:
        """
        1. Column realign
        2. Replace NaN
        3.remove data which have invalid lat, lon
        """
        
        # 1. Column realign
        df = MetroDataPreprocess.column_realign(df, MetroDataPreprocess.columns_dict)

        # 2. Replace NaN
        df = MetroDataPreprocess.replace_nan(df)

        # 3.remove data which have invalid lat, lon
        df = ManageLonLat.return_vaild_data(df)

        df.reset_index(drop=True, inplace=True)

        return df
    
    @staticmethod
    def column_realign(df:pd.DataFrame, columns_dict: Dict) -> pd.DataFrame:
        """
        Realign columns to insert DB
        """

        columns_values = list(columns_dict.values())
        new_df = df.rename(columns = columns_dict)[columns_values]
   
        return new_df

    @staticmethod
    def replace_string(df: pd.DataFrame, replace_column: List[str]) -> pd.DataFrame:
        """
        Replace string by column to prevent error
        """
        pass

    @staticmethod
    def type_change(df: pd.DataFrame, columns_list: List[str], change_type: str) -> pd.DataFrame:
        """
        Type change by column
        """
        pass

    @staticmethod
    def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace missing value to NaN(np.nan)
        """
        # 좌표값이 결측치인 경우, 공백인 경우를 모두 제거한다
        ## 좌표값이 공백인 경우를 모두 결측치로 변경
        df.loc[:, ['lon', 'lat']] = df.loc[:, ['lon', 'lat']].replace(r'^\s*$', np.nan, regex=True)

        ## 좌표값이 결측치이면 삭제
        df = df[ ~df['lon'].isna() & ~df['lat'].isna() ]

        ## 좌표값이 결측치이고, 두 주소 모두(lonmAdr와 lonmAdr) 결측치인 경우 삭제 후 
        # df[ (~df['stinLocLon'].isna() & ~df['stinLocLat'].isna()) | ~(df['lonmAdr'].isna() & df['roadNmAdr'].isna())]

        # 나머지 값들은 결측치나 공백으로 이루어진 것을 null로 바꿈
        ## MySQL에서 NULL값으로 인식하도록 하기 위함
        df.fillna('NULL', inplace=True)
        df.replace(r'^\s*$', 'NULL', regex=True, inplace=True)

        return df

    @staticmethod
    def set_columns_dict(path: str) -> None:

        columns_df = pd.read_csv(path)
        from_list = list(columns_df['From'])
        to_list = list(columns_df['To'])
        columns_dict = {k: v for k, v in zip(from_list, to_list)}

        MetroDataPreprocess.columns_dict = columns_dict