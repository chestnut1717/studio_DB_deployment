import mysql.connector
import pandas as pd
from typing import List, Dict
from tqdm import tqdm

class DBManagement:
    def __init__(self, host: str, user: str, password: str, database: str) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cnx = mysql.connector.connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    database=self.database)
        self.cursor = self.cnx.cursor()
    
    @staticmethod
    # bring db info in local text file(secret_key/db_info.txt)
    def get_db_info(path: str) -> Dict['str','str']:
        db_info_dict = {}

        with open(path, 'r') as f:
            for line in f.readlines():
                line_info = line.split('=')
                key = line_info[0].strip()
                val = line_info[1].strip()
                
                db_info_dict[key] = val

        return db_info_dict
    # Add quote for insert data to DB like address
    @staticmethod
    def replace_quote(value: str) -> str:
        if value == 'NULL':
            return value
        return '"{}"'.format(str(value).replace("'", "\'"))

    # bring table information(columns, PK, etc)
    def set_table(self, table_path: str) -> pd.DataFrame:
        table_df = pd.read_csv(table_path)

        return table_df

    def get_columns(self, table_df: pd.DataFrame) -> List[str]:
        return table_df['Columns']


    def create_table(self, table_name: str, table: pd.DataFrame) -> None:
        # Check if the table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = self.cursor.fetchone()

        # If the table does not exist, create table
        if not result:
            create_query = f"CREATE TABLE {table_name}\n" +"(\n"
            
            # Build create query
            iter_count = len(table)
            for i in range(iter_count):
                row = table.iloc[i,:-1]
                column_query = " ".join([i for i in row if isinstance(i, str)])
                create_query +=  column_query + ",\n"

            primary_query = ", ".join(list(table['Columns'][~table['Indexes'].isna()]))
            create_query += f"PRIMARY KEY({primary_query})\n);"

            self.cursor.execute(create_query)

        else:
            print(f"Table '{table_name}' already exists.")

    def insert_record(self, table_name:str, df:pd.DataFrame, columns: List[str]) -> None:
        base_value_query_list = []
        columns_string = ', '.join(columns)

        for _, row in df.iterrows():
            value_query = ", ".join([str(r) for r in row])
            # the last two value in row is the coordinates [.., lat, lon] or [.., x, y]
            ## 4326 = SRID 4326 (WGS94)
            point_query = f"ST_GeomFromText('POINT({row[-1]} {row[-2]})', 4326)"
            row_query = f"({value_query}, {point_query})"
            base_value_query_list.append(row_query)

        insert_query = f"""
                        INSERT INTO {table_name} ({columns_string})
                        VALUES {", ".join(base_value_query_list)};
                        """
        self.cursor.execute(insert_query)
        self.commit()
            
    # record단위로 commit하지 않음
    def update_record(self, table_name:str, df:pd.DataFrame, columns: List[str]) -> None:
        base_value_query_list = []
        columns_string = ', '.join(columns)

        for _, row in df.iterrows():
            value_query = ", ".join([str(r) for r in row])
            # the last two value in row is the coordinates [.., lat, lon] or [.., x, y]
            ## 4326 = SRID 4326 (WGS94)
            point_query = f"ST_GeomFromText('POINT({row[-1]} {row[-2]})', 4326)"
            row_query = f"({value_query}, {point_query})"
            base_value_query_list.append(row_query)

        
        # If duplicated : Only update values not including coordinates(POINT)
        # If not duplicated : insert all values including coordinates
        condition_query = ",\n".join([f"{col}=IF({col}=VALUES({col}), {col}, VALUES({col}))" for col in columns[:-1]])
        update_query = f"""
                        INSERT INTO {table_name} ({columns_string})
                        VALUES {", ".join(base_value_query_list)}
                        ON DUPLICATE KEY
                        UPDATE {condition_query};
                        """
        # print(update_query)
        self.cursor.execute(update_query)


    def delete_record(self, table_name: str, opnSfTeamCode: str, mgtNo: str, opnSvcId: str) -> None:
        delete_query = f"""
                        DELETE FROM {table_name}
                        WHERE opnSfTeamCode = {opnSfTeamCode} and mgtNo = {mgtNo} and opnSvcId = {opnSvcId};
                        """
        self.cursor.execute(delete_query)


    def create_spatial_index(self, table_name: str, coordinates_column: str) -> None:

                             
        query = f"CREATE SPATIAL INDEX spatial_index ON {table_name}({coordinates_column});"
        self.cursor.execute(query)

    
    def table_size(self, table_name: str) -> int:
        size_query = f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    """
        self.cursor.execute(size_query)

        # query 결과값 가져오기
        result = self.cursor.fetchall()[0][0]

        self.commit()
        return result


    def commit(self) -> None:
        self.cnx.commit()

    def drop_table(self, table_name: str) -> None:
        
        drop_query = f"drop table {table_name}"
        self.cursor.execute(drop_query)
        self.commit()

        
    def insert_image_path(self, image_path: str, related_table_name: str, related_table_id: int) -> None:
        insert_query = f"""
                    INSERT IGNORE INTO image_table (image_path, related_table_name, related_table_id)
                    VALUES (%s, %s, %s)
                    """
        self.cursor.execute(insert_query, (image_path, related_table_name, related_table_id))
        self.commit()
